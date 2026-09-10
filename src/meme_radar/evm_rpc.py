from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

import websockets

from .launchpads import (
    CLANKER_V4_BASE_FACTORY,
    CLANKER_V4_TOKEN_CREATED_TOPIC,
    FOURMEME_BSC_PROXY,
    PONS_V2_FACTORY,
)
from .models import ParseBatch, ParseIssue
from .normalize import build_event, payload_sha256


FOURMEME_TOKEN_CREATE_TOPIC = (
    "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"
)
PONS_TOKEN_LAUNCHED_TOPIC = (
    "0x8d4aad4953d0ca700d468f3753aa14432d1b35b43ec6409f051fb6aa43a89607"
)

_HEX = re.compile(r"^0x[0-9a-fA-F]*$")
_HASH = re.compile(r"^0x[0-9a-fA-F]{64}$")
_ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")


class RpcError(RuntimeError):
    def __init__(self, code: str, status: Optional[int] = None) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _no_duplicate_keys(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate JSON key")
        output[key] = value
    return output


def _validate_endpoint(url: str, scheme: str) -> str:
    parsed = urlsplit(url)
    if (
        parsed.scheme != scheme
        or not parsed.hostname
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("invalid RPC endpoint")
    return url


def _hex_int(value: Any) -> int:
    if not isinstance(value, str) or not _HEX.fullmatch(value) or len(value) < 3:
        raise RpcError("INVALID_HEX_INTEGER")
    return int(value, 16)


def _address_word(value: str) -> str:
    raw = value[2:] if value.startswith("0x") else value
    if len(raw) != 64 or any(char not in "0123456789abcdefABCDEF" for char in raw):
        raise ValueError("invalid address word")
    address = "0x" + raw[-40:].lower()
    if not _ADDRESS.fullmatch(address) or int(address, 16) == 0:
        raise ValueError("invalid decoded address")
    return address


def _data_word(data: str, index: int) -> str:
    if not isinstance(data, str) or not _HEX.fullmatch(data):
        raise ValueError("invalid log data")
    raw = data[2:]
    start = index * 64
    word = raw[start : start + 64]
    if len(word) != 64:
        raise ValueError("short log data")
    return word


def decode_abi_text(value: Any, limit: int = 120) -> str:
    if not isinstance(value, str) or not _HEX.fullmatch(value):
        raise ValueError("invalid ABI text")
    raw = bytes.fromhex(value[2:])
    if len(raw) == 32:
        payload = raw.rstrip(b"\x00")
    else:
        if len(raw) < 64:
            raise ValueError("short ABI text")
        offset = int.from_bytes(raw[:32], "big")
        if offset % 32 or offset + 32 > len(raw):
            raise ValueError("invalid ABI text offset")
        size = int.from_bytes(raw[offset : offset + 32], "big")
        if size > 512 or offset + 32 + size > len(raw):
            raise ValueError("invalid ABI text length")
        payload = raw[offset + 32 : offset + 32 + size]
    return payload.decode("utf-8", errors="strict").strip()[:limit]


@dataclass(frozen=True)
class NativeLaunchSpec:
    chain: str
    chain_id: int
    launchpad: str
    factory: str
    topic0: str
    token_location: Tuple[str, int]
    creator_location: Tuple[str, int]
    quote_symbol: str
    max_log_blocks: int
    history_limit_blocks: int


NATIVE_SPECS = {
    "bsc": NativeLaunchSpec(
        chain="bsc",
        chain_id=56,
        launchpad="four_meme",
        factory=FOURMEME_BSC_PROXY,
        topic0=FOURMEME_TOKEN_CREATE_TOPIC,
        token_location=("data", 1),
        creator_location=("data", 0),
        quote_symbol="BNB",
        # QuickNode's free BSC endpoint accepts the launch filter for at most
        # five recent blocks (larger ranges return HTTP 413). Keep the total
        # history window unchanged and split it into bounded requests.
        max_log_blocks=5,
        history_limit_blocks=100,
    ),
    "base": NativeLaunchSpec(
        chain="base",
        chain_id=8453,
        launchpad="clanker_v4",
        factory=CLANKER_V4_BASE_FACTORY,
        topic0=CLANKER_V4_TOKEN_CREATED_TOPIC,
        token_location=("topic", 1),
        creator_location=("topic", 2),
        quote_symbol="ETH",
        max_log_blocks=10,
        history_limit_blocks=100,
    ),
    "robinhood": NativeLaunchSpec(
        chain="robinhood",
        chain_id=4663,
        launchpad="pons_v2_event",
        factory=PONS_V2_FACTORY,
        topic0=PONS_TOKEN_LAUNCHED_TOPIC,
        token_location=("topic", 1),
        creator_location=("topic", 3),
        quote_symbol="",
        max_log_blocks=10,
        history_limit_blocks=500,
    ),
}


@dataclass(frozen=True)
class NativeLogIdentity:
    block_number: int
    block_hash: str
    transaction_hash: str
    log_index: int
    token_address: str
    creator: str


def parse_log_identity(log: Dict[str, Any], spec: NativeLaunchSpec) -> NativeLogIdentity:
    if not isinstance(log, dict) or log.get("removed") is True:
        raise ValueError("removed or invalid log")
    address = str(log.get("address", "")).lower()
    topics = log.get("topics")
    if address != spec.factory or not isinstance(topics, list):
        raise ValueError("log source mismatch")
    if not topics or str(topics[0]).lower() != spec.topic0:
        raise ValueError("log topic mismatch")

    def located(location: Tuple[str, int]) -> str:
        source, index = location
        if source == "topic":
            if index >= len(topics):
                raise ValueError("missing address topic")
            return _address_word(str(topics[index]))
        return _address_word(_data_word(str(log.get("data", "")), index))

    block_hash = str(log.get("blockHash", ""))
    tx_hash = str(log.get("transactionHash", ""))
    if not _HASH.fullmatch(block_hash) or not _HASH.fullmatch(tx_hash):
        raise ValueError("invalid log hash")
    return NativeLogIdentity(
        block_number=_hex_int(log.get("blockNumber")),
        block_hash=block_hash.lower(),
        transaction_hash=tx_hash.lower(),
        log_index=_hex_int(log.get("logIndex")),
        token_address=located(spec.token_location),
        creator=located(spec.creator_location),
    )


def parse_native_launch(
    log: Dict[str, Any],
    spec: NativeLaunchSpec,
    *,
    published_at_ms: int,
    observed_at_ms: int,
    received_at_ms: int,
    name: str = "",
    symbol: str = "",
    is_backfill: bool = False,
) -> ParseBatch:
    payload = {"chain": spec.chain, "log": log}
    digest = payload_sha256(payload)
    try:
        identity = parse_log_identity(log, spec)
        event = build_event(
            source="native_rpc",
            source_event_id="%s:%s:%d"
            % (spec.chain, identity.transaction_hash, identity.log_index),
            event_kind="launch",
            chain=spec.chain,
            token_address=identity.token_address,
            source_published_at_ms=published_at_ms,
            observed_at_ms=observed_at_ms,
            received_at_ms=received_at_ms,
            launchpad=spec.launchpad,
            token_created_at_ms=published_at_ms,
            name=name,
            symbol=symbol,
            creator=identity.creator,
            quote_symbol=spec.quote_symbol,
            tx_hash=identity.transaction_hash,
            is_backfill=is_backfill,
            raw_sha256=digest,
        )
        return ParseBatch((event,), (), digest)
    except (TypeError, ValueError, RpcError) as exc:
        return ParseBatch(
            (),
            (ParseIssue(0, "INVALID_NATIVE_RPC_LOG", str(exc)),),
            digest,
        )


class HttpRpcClient:
    ALLOWED_METHODS = frozenset(
        {
            "eth_chainId",
            "eth_blockNumber",
            "eth_getLogs",
            "eth_getBlockByNumber",
            "eth_call",
        }
    )

    def __init__(
        self,
        endpoint: str,
        *,
        timeout_seconds: float = 12,
        max_bytes: int = 2 * 1024 * 1024,
        max_requests_per_second: float = 5,
    ) -> None:
        self._endpoint = _validate_endpoint(endpoint, "https")
        if timeout_seconds <= 0 or max_bytes <= 0 or max_requests_per_second <= 0:
            raise ValueError("invalid RPC boundary")
        self._timeout = timeout_seconds
        self._max_bytes = max_bytes
        self._minimum_interval = 1.0 / max_requests_per_second
        self._last_request = 0.0
        self._lock = threading.Lock()
        self._opener = build_opener(_NoRedirect())
        self._next_id = 1

    def _pace(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._minimum_interval - (now - self._last_request)
            if delay > 0:
                time.sleep(delay)
            self._last_request = time.monotonic()

    def _request(self, payload: Any) -> Any:
        self._pace()
        body = json.dumps(
            payload,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > self._max_bytes:
            raise RpcError("REQUEST_TOO_LARGE")
        request = Request(
            self._endpoint,
            data=body,
            method="POST",
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": "meme-radar/0.2",
            },
        )
        try:
            with self._opener.open(request, timeout=self._timeout) as response:
                content_type = str(response.headers.get("content-type", ""))
                content_type = content_type.split(";", 1)[0].strip().lower()
                if content_type != "application/json" and not content_type.endswith("+json"):
                    raise RpcError("CONTENT_TYPE")
                length = response.headers.get("content-length")
                if length is not None and int(length) > self._max_bytes:
                    raise RpcError("BODY_TOO_LARGE")
                raw = response.read(self._max_bytes + 1)
                if len(raw) > self._max_bytes:
                    raise RpcError("BODY_TOO_LARGE")
                data = json.loads(
                    raw.decode("utf-8", errors="strict"),
                    object_pairs_hook=_no_duplicate_keys,
                )
        except RpcError:
            raise
        except HTTPError as exc:
            raise RpcError("HTTP_STATUS", int(exc.code)) from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise RpcError("NETWORK_ERROR") from exc
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise RpcError("INVALID_JSON") from exc
        return data

    def call(self, method: str, params: List[Any]) -> Any:
        if method not in self.ALLOWED_METHODS:
            raise ValueError("RPC method not allowed")
        request_id = self._next_id
        self._next_id += 1
        data = self._request(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        if not isinstance(data, dict) or data.get("id") != request_id:
            raise RpcError("INVALID_RPC_RESPONSE")
        if data.get("error") is not None:
            raise RpcError("RPC_RESPONSE_ERROR")
        if "result" not in data:
            raise RpcError("RPC_RESULT_MISSING")
        return data["result"]

    def call_batch(self, calls: List[Tuple[str, List[Any]]]) -> List[Any]:
        if not 1 <= len(calls) <= 50:
            raise ValueError("RPC batch size outside 1..50")
        requests = []
        request_ids = []
        for method, params in calls:
            if method not in self.ALLOWED_METHODS:
                raise ValueError("RPC method not allowed")
            request_id = self._next_id
            self._next_id += 1
            request_ids.append(request_id)
            requests.append(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
        data = self._request(requests)
        if not isinstance(data, list) or len(data) != len(requests):
            raise RpcError("INVALID_BATCH_RESPONSE")
        responses = {}
        for item in data:
            if not isinstance(item, dict) or item.get("id") in responses:
                raise RpcError("INVALID_BATCH_RESPONSE")
            if item.get("error") is not None or "result" not in item:
                raise RpcError("RPC_BATCH_ERROR")
            responses[item.get("id")] = item["result"]
        if set(responses) != set(request_ids):
            raise RpcError("INVALID_BATCH_RESPONSE")
        return [responses[request_id] for request_id in request_ids]

    def chain_id(self) -> int:
        return _hex_int(self.call("eth_chainId", []))

    def block_number(self) -> int:
        return _hex_int(self.call("eth_blockNumber", []))

    def block_timestamp_ms(self, block_number: int) -> int:
        block = self.call("eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(block, dict):
            raise RpcError("BLOCK_MISSING")
        return _hex_int(block.get("timestamp")) * 1000

    def block_timestamps_ms(self, block_numbers: Iterable[int]) -> Dict[int, int]:
        numbers = list(dict.fromkeys(int(value) for value in block_numbers))
        if not numbers or len(numbers) > 50 or any(value < 0 for value in numbers):
            raise ValueError("invalid block timestamp batch")
        blocks = self.call_batch(
            [
                ("eth_getBlockByNumber", [hex(number), False])
                for number in numbers
            ]
        )
        output = {}
        for expected, block in zip(numbers, blocks):
            if (
                not isinstance(block, dict)
                or _hex_int(block.get("number")) != expected
            ):
                raise RpcError("BLOCK_MISSING")
            output[expected] = _hex_int(block.get("timestamp")) * 1000
        return output

    def get_logs(
        self,
        spec: NativeLaunchSpec,
        from_block: int,
        to_block: int,
    ) -> List[Dict[str, Any]]:
        block_count = to_block - from_block + 1
        if (
            from_block < 0
            or to_block < from_block
            or block_count > spec.max_log_blocks
        ):
            raise ValueError("invalid log range")
        result = self.call(
            "eth_getLogs",
            [
                {
                    "address": spec.factory,
                    "topics": [spec.topic0],
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                }
            ],
        )
        if not isinstance(result, list) or len(result) > 5000:
            raise RpcError("INVALID_LOG_RESULT")
        return [item for item in result if isinstance(item, dict)]

    def token_metadata(self, token_address: str) -> Tuple[str, str]:
        if not _ADDRESS.fullmatch(token_address):
            raise ValueError("invalid token address")
        values = []
        for selector, limit in (("0x06fdde03", 120), ("0x95d89b41", 32)):
            try:
                result = self.call(
                    "eth_call",
                    [{"to": token_address, "data": selector}, "latest"],
                )
                values.append(decode_abi_text(result, limit))
            except (RpcError, ValueError, UnicodeDecodeError):
                values.append("")
        return values[0], values[1]


def get_logs_with_413_split(
    client: HttpRpcClient,
    spec: NativeLaunchSpec,
    from_block: int,
    to_block: int,
) -> Tuple[List[Dict[str, Any]], int]:
    """Read a bounded log range, splitting only provider payload-limit errors."""
    try:
        return client.get_logs(spec, from_block, to_block), 0
    except RpcError as exc:
        if exc.code != "HTTP_STATUS" or exc.status != 413 or from_block >= to_block:
            raise
    middle = (from_block + to_block) // 2
    left, left_splits = get_logs_with_413_split(
        client,
        spec,
        from_block,
        middle,
    )
    right, right_splits = get_logs_with_413_split(
        client,
        spec,
        middle + 1,
        to_block,
    )
    return left + right, left_splits + right_splits + 1


def validate_wss_endpoint(endpoint: str) -> str:
    return _validate_endpoint(endpoint, "wss")


async def stream_native_logs_once(
    *,
    endpoint: str,
    spec: NativeLaunchSpec,
    stop_event: asyncio.Event,
    on_ready: Optional[Callable[[], None]] = None,
    reconnect_event: Optional[asyncio.Event] = None,
):
    url = validate_wss_endpoint(endpoint)
    async with websockets.connect(
        url,
        open_timeout=10,
        close_timeout=3,
        ping_interval=20,
        max_size=2 * 1024 * 1024,
        max_queue=64,
    ) as websocket:
        await websocket.send(
            json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []})
        )
        chain_response = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
        if not isinstance(chain_response, dict) or _hex_int(chain_response.get("result")) != spec.chain_id:
            raise RpcError("CHAIN_ID_MISMATCH")
        await websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": ["logs", {"address": spec.factory, "topics": [spec.topic0]}],
                }
            )
        )
        subscription_response = json.loads(
            await asyncio.wait_for(websocket.recv(), timeout=10)
        )
        subscription = (
            subscription_response.get("result")
            if isinstance(subscription_response, dict)
            else None
        )
        if not isinstance(subscription, str) or not subscription:
            raise RpcError("SUBSCRIPTION_REJECTED")
        if on_ready is not None:
            on_ready()
        # Let the caller run a second backfill after subscription is active.
        # Messages remain buffered by the websocket, closing the fetch/subscribe gap.
        yield None
        try:
            while not stop_event.is_set() and not (
                reconnect_event is not None and reconnect_event.is_set()
            ):
                try:
                    raw = await asyncio.wait_for(websocket.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                if not isinstance(raw, str):
                    raise RpcError("BINARY_WS_MESSAGE")
                if len(raw.encode("utf-8")) > 2 * 1024 * 1024:
                    raise RpcError("MESSAGE_TOO_LARGE")
                message = json.loads(raw, object_pairs_hook=_no_duplicate_keys)
                if not isinstance(message, dict):
                    raise RpcError("INVALID_WS_MESSAGE")
                if message.get("method") != "eth_subscription":
                    continue
                params = message.get("params")
                if not isinstance(params, dict) or params.get("subscription") != subscription:
                    raise RpcError("SUBSCRIPTION_ID_MISMATCH")
                log = params.get("result")
                if not isinstance(log, dict):
                    raise RpcError("INVALID_LOG_RESULT")
                yield log
        finally:
            try:
                await websocket.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": 3,
                            "method": "eth_unsubscribe",
                            "params": [subscription],
                        }
                    )
                )
            except Exception:
                pass


async def probe_native_wss_once(
    *,
    endpoint: str,
    spec: NativeLaunchSpec,
) -> None:
    url = validate_wss_endpoint(endpoint)
    async with websockets.connect(
        url,
        open_timeout=10,
        close_timeout=3,
        ping_interval=None,
        max_size=256 * 1024,
        max_queue=4,
    ) as websocket:
        await websocket.send(
            json.dumps(
                {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}
            )
        )
        chain = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
        if not isinstance(chain, dict) or _hex_int(chain.get("result")) != spec.chain_id:
            raise RpcError("CHAIN_ID_MISMATCH")
        await websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_subscribe",
                    "params": ["logs", {"address": spec.factory, "topics": [spec.topic0]}],
                }
            )
        )
        subscribed = json.loads(
            await asyncio.wait_for(websocket.recv(), timeout=10)
        )
        subscription = subscribed.get("result") if isinstance(subscribed, dict) else None
        if not isinstance(subscription, str) or not subscription:
            raise RpcError("SUBSCRIPTION_REJECTED")
        await websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "eth_unsubscribe",
                    "params": [subscription],
                }
            )
        )
