from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sqlite3
import sys
import time
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import websockets

from .credentials import load_env_file
from .evm_rpc import HttpRpcClient, RpcError, validate_wss_endpoint
from .launchpads import PONS_V2_FACTORY
from .pons_curve import (
    PONS_CURVE_BUY_TOPIC,
    PONS_CURVE_COMPLETED_TOPIC,
    PONS_CURVE_SELL_TOPIC,
    PONS_CURVE_TOPICS,
    PONS_FACTORY_TOPICS,
    PONS_POOL_GRADUATED_TOPIC,
    PonsLaunch,
    parse_pons_launch,
    parse_pons_lifecycle,
    parse_pons_trade,
)
from .pons_curve_storage import (
    connect_shadow,
    get_state,
    initialize_shadow,
    load_launches,
    set_state,
    shadow_counts,
    store_launch,
    store_lifecycle,
    store_trade,
)
from .runtime import write_health


CHAIN_ID = 4663
CHECKPOINT_KEY = "robinhood_block"
SEED_CHECKPOINT_KEY = "radar_seed_received_at_ms"
CURVE_WATERMARK_KEY = "curve_event_time_watermark_ms"
CURVE_COVERAGE_START_KEY = "curve_coverage_start_ms"
MAX_RPC_LOGS = 5000
WSS_IDLE_SECONDS = 60
BACKFILL_CHUNK_BLOCKS = 5
PONS_HTTP_MAX_RPS = 3
PONS_WSS_MAX_QUEUE = 128
REGISTRY_RETENTION_MS = 2 * 60 * 60_000
POLL_WINDOW_BLOCKS = 100
POLL_OVERLAP_BLOCKS = 2
POLL_INTERVAL_SECONDS = 3


def _log(kind: str, **fields: Any) -> None:
    print(
        json.dumps(
            {"event": kind, **fields},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )


def _pairs(values: Mapping[str, str]) -> List[Tuple[str, str, str]]:
    candidates = (
        (
            "primary",
            values.get("ROBINHOOD_RPC_URL", ""),
            values.get("ROBINHOOD_WSS_URL", ""),
        ),
        (
            "standby",
            values.get("ROBINHOOD_RPC_URL_STANDBY", ""),
            values.get("ROBINHOOD_WSS_URL_STANDBY", ""),
        ),
    )
    endpoints = []
    for slot, http, wss in candidates:
        if bool(http) != bool(wss):
            raise ValueError("incomplete Robinhood endpoint pair")
        if http:
            endpoints.append((slot, http, validate_wss_endpoint(wss)))
    if not endpoints:
        raise ValueError("Robinhood endpoints unavailable")
    return endpoints


def _safe_topic(log: Mapping[str, Any]) -> str:
    topics = log.get("topics")
    if not isinstance(topics, list) or not topics or not isinstance(topics[0], str):
        return ""
    return topics[0].lower()


def _block_number(log: Mapping[str, Any]) -> int:
    value = log.get("blockNumber")
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ValueError("invalid block number")
    return int(value, 16)


def _rpc_logs(
    client: HttpRpcClient,
    *,
    from_block: int,
    to_block: int,
    topics: Iterable[str],
    address: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if from_block < 0 or to_block < from_block or to_block - from_block >= 10:
        raise ValueError("invalid Pons log range")
    query: Dict[str, Any] = {
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "topics": [list(topics)],
    }
    if address is not None:
        query["address"] = address
    result = client.call("eth_getLogs", [query])
    if not isinstance(result, list) or len(result) > MAX_RPC_LOGS:
        raise RpcError("INVALID_LOG_RESULT")
    if any(not isinstance(item, dict) for item in result):
        raise RpcError("INVALID_LOG_RESULT")
    return result


def _public_poll_logs(
    client: HttpRpcClient,
    *,
    from_block: int,
    to_block: int,
) -> List[Dict[str, Any]]:
    if (
        from_block < 0
        or to_block < from_block
        or to_block - from_block >= 100
    ):
        raise ValueError("invalid Pons public poll range")
    topics = list(PONS_FACTORY_TOPICS) + list(PONS_CURVE_TOPICS)
    result = client.call(
        "eth_getLogs",
        [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "topics": [topics],
            }
        ],
    )
    if not isinstance(result, list) or len(result) > MAX_RPC_LOGS:
        raise RpcError("INVALID_LOG_RESULT")
    if any(not isinstance(item, dict) for item in result):
        raise RpcError("INVALID_LOG_RESULT")
    return [
        item
        for item in result
        if _safe_topic(item) != PONS_FACTORY_TOPICS[0]
        or str(item.get("address", "")).lower() == PONS_V2_FACTORY.lower()
    ]


def _history_backfill_can_fallback(exc: RpcError) -> bool:
    return exc.code == "RPC_RESPONSE_ERROR" or (
        exc.code == "HTTP_STATUS" and exc.status == 403
    )


def _no_duplicate_keys(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate JSON key")
        output[key] = value
    return output


async def _recv_json(websocket) -> Dict[str, Any]:
    raw = await asyncio.wait_for(websocket.recv(), timeout=10)
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > 2 * 1024 * 1024:
        raise RpcError("INVALID_WS_MESSAGE")
    message = json.loads(raw, object_pairs_hook=_no_duplicate_keys)
    if not isinstance(message, dict):
        raise RpcError("INVALID_WS_MESSAGE")
    return message


async def _verify_chain(websocket) -> None:
    await websocket.send(
        json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}
        )
    )
    response = await _recv_json(websocket)
    result = response.get("result")
    if not isinstance(result, str) or int(result, 16) != CHAIN_ID:
        raise RpcError("CHAIN_ID_MISMATCH")


async def _subscribe(websocket):
    filters = (
        {"address": PONS_V2_FACTORY, "topics": [list(PONS_FACTORY_TOPICS)]},
        {"topics": [list(PONS_CURVE_TOPICS)]},
    )
    for request_id, query in enumerate(filters, start=2):
        await websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_subscribe",
                    "params": ["logs", query],
                }
            )
        )
    subscriptions = set()
    buffered = []
    while len(subscriptions) < len(filters):
        response = await _recv_json(websocket)
        if response.get("id") in {2, 3}:
            subscription = response.get("result")
            if not isinstance(subscription, str) or not subscription:
                raise RpcError("SUBSCRIPTION_REJECTED")
            subscriptions.add(subscription)
        elif response.get("method") == "eth_subscription":
            if len(buffered) >= 64:
                raise RpcError("SUBSCRIPTION_BUFFER_FULL")
            buffered.append(response)
        else:
            raise RpcError("UNEXPECTED_WS_MESSAGE")
    return subscriptions, buffered


def _subscription_log(message: Dict[str, Any], subscriptions: set):
    if message.get("method") != "eth_subscription":
        return None
    params = message.get("params")
    if not isinstance(params, dict) or params.get("subscription") not in subscriptions:
        raise RpcError("SUBSCRIPTION_ID_MISMATCH")
    item = params.get("result")
    if not isinstance(item, dict):
        raise RpcError("INVALID_LOG_RESULT")
    return item


async def _recv_log_batch(websocket, subscriptions: set):
    batch = []
    deadline = asyncio.get_running_loop().time() + 0.25
    while len(batch) < 100:
        timeout = 5 if not batch else deadline - asyncio.get_running_loop().time()
        if timeout <= 0:
            break
        try:
            message = await asyncio.wait_for(_recv_json(websocket), timeout=timeout)
        except asyncio.TimeoutError:
            break
        item = _subscription_log(message, subscriptions)
        if item is not None:
            batch.append(item)
    return batch


async def stream_pons_logs_once(
    endpoint: str,
    stop_event: asyncio.Event,
):
    url = validate_wss_endpoint(endpoint)
    async with websockets.connect(
        url,
        open_timeout=10,
        close_timeout=3,
        ping_interval=20,
        max_size=2 * 1024 * 1024,
        max_queue=PONS_WSS_MAX_QUEUE,
    ) as websocket:
        await _verify_chain(websocket)
        subscriptions, buffered = await _subscribe(websocket)
        yield None
        initial = []
        for message in buffered:
            item = _subscription_log(message, subscriptions)
            if item is not None:
                initial.append(item)
        if initial:
            yield initial
        last_batch_at = time.monotonic()
        try:
            while not stop_event.is_set():
                batch = await _recv_log_batch(websocket, subscriptions)
                if batch:
                    last_batch_at = time.monotonic()
                    yield batch
                elif time.monotonic() - last_batch_at > WSS_IDLE_SECONDS:
                    raise RpcError("WSS_IDLE")
        finally:
            for request_id, subscription in enumerate(subscriptions, start=10):
                try:
                    await websocket.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "method": "eth_unsubscribe",
                                "params": [subscription],
                            }
                        )
                    )
                except Exception:
                    pass


class PonsCurveShadow:
    def __init__(
        self,
        *,
        env_file: Path,
        source_db: Path,
        db_path: Path,
        health_path: Path,
        backfill_blocks: int,
    ) -> None:
        values = load_env_file(env_file)
        self.endpoints = _pairs(values)
        self.clients = [
            HttpRpcClient(http, max_requests_per_second=PONS_HTTP_MAX_RPS)
            for _, http, _ in self.endpoints
        ]
        poll_url = values.get("PONS_PUBLIC_POLL_RPC_URL", "").strip()
        self.poll_client = (
            HttpRpcClient(poll_url, max_requests_per_second=1)
            if poll_url
            else None
        )
        self.poll_primary = values.get("PONS_PUBLIC_POLL_PRIMARY", "0") == "1"
        if self.poll_primary and self.poll_client is None:
            raise ValueError("Pons public poll primary unavailable")
        self.source_db = source_db
        self.db_path = db_path
        self.connection = connect_shadow(db_path)
        initialize_shadow(self.connection)
        self.health_path = health_path
        self.backfill_blocks = backfill_blocks
        self.endpoint_index = 0
        self.stop_event = asyncio.Event()
        self.started_at_ms = int(time.time() * 1000)
        self.status = "starting"
        self.connected = False
        self.source_mode = "starting"
        self.counters = Counter()
        self.last_success: Dict[str, int] = {}
        self.last_error: Dict[str, Any] = {}
        self.factory_backfill_supported = True
        self.curve_backfill_supported = True
        self.block_timestamp_batch_supported = True
        self.block_times: OrderedDict[int, int] = OrderedDict()
        self.launches_by_curve: Dict[str, PonsLaunch] = {}
        self.launches_by_token: Dict[str, PonsLaunch] = {}
        self.counts_cache: Dict[str, int] = {}
        self._reload_registry()

    @property
    def client(self) -> HttpRpcClient:
        return self.clients[self.endpoint_index]

    def close(self) -> None:
        self.connection.close()

    def _reload_registry(self) -> None:
        launches = list(
            load_launches(
                self.connection,
                min_block_timestamp_ms=self.started_at_ms - REGISTRY_RETENTION_MS,
            )
        )
        self.launches_by_curve = {item.curve_address: item for item in launches}
        self.launches_by_token = {item.token_address: item for item in launches}

    def _prune_registry(self, now_ms: int) -> None:
        cutoff = now_ms - REGISTRY_RETENTION_MS
        expired = [
            token_address
            for token_address, launch in self.launches_by_token.items()
            if launch.block_timestamp_ms < cutoff
        ]
        for token_address in expired:
            launch = self.launches_by_token.pop(token_address)
            self.launches_by_curve.pop(launch.curve_address, None)
        self.counters["registry_pruned"] += len(expired)

    @staticmethod
    def _seed_row(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> Tuple[bool, PonsLaunch]:
        payload = json.loads(row["payload_json"])
        launch = parse_pons_launch(
            payload["log"],
            block_timestamp_ms=int(row["token_created_at_ms"]),
            observed_at_ms=int(row["received_at_ms"]),
        )
        if launch.token_address != row["token_address"]:
            raise ValueError("seed token mismatch")
        return store_launch(
            connection,
            launch,
            allow_timestamp_correction=True,
        ), launch

    def _seed_from_radar_sync(self):
        target = connect_shadow(self.db_path)
        try:
            checkpoint = int(
                get_state(target, SEED_CHECKPOINT_KEY, 0) or 0
            )
            uri = self.source_db.resolve().as_uri() + "?mode=ro"
            source = sqlite3.connect(uri, uri=True, timeout=10)
            source.row_factory = sqlite3.Row
            maximum = checkpoint
            seen = 0
            inserted = 0
            rejected = 0
            launches = []
            try:
                rows = source.execute(
                    """
                    SELECT e.token_address,e.token_created_at_ms,
                           e.received_at_ms,p.payload_json
                    FROM radar_events e JOIN raw_payloads p USING(raw_sha256)
                    WHERE e.source='native_rpc' AND e.chain='robinhood'
                      AND e.event_kind='launch'
                      AND e.received_at_ms>=?
                    ORDER BY e.token_created_at_ms,e.event_id
                    """,
                    (max(0, checkpoint - 5000),),
                )
                for row in rows:
                    maximum = max(maximum, int(row["received_at_ms"]))
                    seen += 1
                    try:
                        was_inserted, launch = self._seed_row(target, row)
                        inserted += int(was_inserted)
                        launches.append(launch)
                    except (KeyError, TypeError, ValueError, RuntimeError):
                        rejected += 1
            finally:
                source.close()
            now_ms = int(time.time() * 1000)
            set_state(target, SEED_CHECKPOINT_KEY, maximum, now_ms)
            return seen, inserted, rejected, launches, now_ms
        finally:
            target.close()

    async def seed_from_radar(self) -> None:
        seen, inserted, rejected, launches, now_ms = await asyncio.to_thread(
            self._seed_from_radar_sync
        )
        self.counters["seed_seen"] += seen
        self.counters["seed_inserted"] += inserted
        self.counters["seed_rejected"] += rejected
        for launch in launches:
            self._remember_launch(launch)
        self._prune_registry(now_ms)
        self.counters["seed_runs"] += 1
        self.last_success["seed"] = now_ms

    async def seed_reconcile_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=30)
            except asyncio.TimeoutError:
                try:
                    await self.seed_from_radar()
                except (OSError, sqlite3.Error, RuntimeError, ValueError) as exc:
                    self.counters["seed_errors"] += 1
                    self.last_error = {
                        "at_ms": int(time.time() * 1000),
                        "type": type(exc).__name__,
                        "code": getattr(exc, "sqlite_errorname", None),
                        "status": None,
                    }

    def _read_counts(self) -> Dict[str, int]:
        return {"registry_launches": len(self.launches_by_token)}

    async def counts_refresh_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.counts_cache = await asyncio.to_thread(self._read_counts)
                self.last_success["counts"] = int(time.time() * 1000)
            except (OSError, sqlite3.Error, RuntimeError, ValueError):
                self.counters["counts_errors"] += 1
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=300)
            except asyncio.TimeoutError:
                pass

    async def block_timestamp_ms(self, block_number: int) -> int:
        cached = self.block_times.get(block_number)
        if cached is not None:
            self.block_times.move_to_end(block_number)
            return cached
        timestamp = (await self.fetch_block_times([block_number]))[block_number]
        self.block_times[block_number] = timestamp
        if len(self.block_times) > 2048:
            self.block_times.popitem(last=False)
        return timestamp

    async def fetch_block_times(self, block_numbers: List[int]) -> Dict[int, int]:
        if getattr(self, "block_timestamp_batch_supported", True):
            try:
                return await asyncio.to_thread(
                    self.client.block_timestamps_ms, block_numbers
                )
            except RpcError as exc:
                unsupported = exc.code == "RPC_BATCH_ERROR" or (
                    exc.code == "HTTP_STATUS" and exc.status == 403
                )
                if not unsupported:
                    raise
                self.block_timestamp_batch_supported = False
                self.counters["block_timestamp_batch_fallbacks"] += 1
        values = {}
        for block_number in block_numbers:
            values[block_number] = await self.fetch_block_time(block_number)
        return values

    async def fetch_block_time(self, block_number: int) -> int:
        for attempt in range(5):
            try:
                return await asyncio.to_thread(
                    self.client.block_timestamp_ms, block_number
                )
            except RpcError as exc:
                if (
                    exc.code not in {"BLOCK_MISSING", "RPC_RESPONSE_ERROR"}
                    or attempt == 4
                ):
                    raise
                self.counters["block_timestamp_retries"] += 1
                await asyncio.sleep(0.5 * (attempt + 1))
        raise AssertionError("unreachable block time retry")

    async def prime_block_times(self, logs: List[Dict[str, Any]]) -> None:
        missing = sorted(
            {
                _block_number(item)
                for item in logs
                if _block_number(item) not in self.block_times
            }
        )
        for index in range(0, len(missing), 50):
            chunk = missing[index : index + 50]
            values = await self.fetch_block_times(chunk)
            self.block_times.update(values)
        while len(self.block_times) > 2048:
            self.block_times.popitem(last=False)

    def _remember_launch(self, launch: PonsLaunch) -> None:
        self.launches_by_curve[launch.curve_address] = launch
        self.launches_by_token[launch.token_address] = launch

    def _advance_curve_watermark(self, timestamp_ms: int) -> None:
        current = int(
            get_state(self.connection, CURVE_WATERMARK_KEY, 0) or 0
        )
        if timestamp_ms > current:
            set_state(
                self.connection,
                CURVE_WATERMARK_KEY,
                timestamp_ms,
                int(time.time() * 1000),
            )

    def disable_historical_backfill(self) -> None:
        self.factory_backfill_supported = False
        self.curve_backfill_supported = False
        self.counters["historical_backfill_disabled"] += 1

    async def skip_unsupported_backfill(self, latest: Optional[int] = None) -> None:
        timestamp_ms = int(time.time() * 1000)
        self._advance_curve_watermark(timestamp_ms)
        now_ms = timestamp_ms
        if latest is not None:
            set_state(self.connection, CHECKPOINT_KEY, latest, now_ms)
        self.counters["backfill_unavailable_skips"] += 1
        self.last_success["backfill"] = now_ms

    async def process_log(self, log: Dict[str, Any], *, backfill: bool) -> None:
        observed_at_ms = int(time.time() * 1000)
        block = _block_number(log)
        published_at_ms = await self.block_timestamp_ms(block)
        topic = _safe_topic(log)
        if topic in PONS_CURVE_TOPICS:
            self._advance_curve_watermark(published_at_ms)
        if topic == PONS_FACTORY_TOPICS[0]:
            launch = parse_pons_launch(
                log,
                block_timestamp_ms=published_at_ms,
                observed_at_ms=observed_at_ms,
            )
            inserted = store_launch(self.connection, launch)
            self._remember_launch(launch)
            self.counters["launch_inserted"] += int(inserted)
        elif topic in {PONS_CURVE_BUY_TOPIC, PONS_CURVE_SELL_TOPIC}:
            curve = str(log.get("address", "")).lower()
            launch = self.launches_by_curve.get(curve)
            if launch is None or launch.removed:
                self.counters["unregistered_curve_logs"] += 1
                return
            trade = parse_pons_trade(
                log,
                launch,
                block_timestamp_ms=published_at_ms,
                observed_at_ms=observed_at_ms,
            )
            inserted = store_trade(self.connection, trade)
            self.counters[trade.event_kind + "_inserted"] += int(inserted)
            if inserted and trade.is_creator_initial:
                self.counters["creator_initial_inserted"] += 1
        elif topic in {PONS_CURVE_COMPLETED_TOPIC, PONS_POOL_GRADUATED_TOPIC}:
            event = parse_pons_lifecycle(
                log,
                self.launches_by_curve,
                self.launches_by_token,
                block_timestamp_ms=published_at_ms,
                observed_at_ms=observed_at_ms,
            )
            self.counters[event.event_kind + "_inserted"] += int(
                store_lifecycle(self.connection, event)
            )
        else:
            self.counters["unknown_topic"] += 1
            return
        self.counters["backfill_logs" if backfill else "wss_logs"] += 1
        self.last_success["log"] = observed_at_ms

    async def process_batch(
        self,
        logs: List[Dict[str, Any]],
        *,
        backfill: bool,
    ) -> None:
        if backfill:
            await self.prime_block_times(logs)
        else:
            observed_at_ms = int(time.time() * 1000)
            missing = {
                _block_number(item)
                for item in logs
                if _block_number(item) not in self.block_times
            }
            self.block_times.update(
                {block_number: observed_at_ms for block_number in missing}
            )
            while len(self.block_times) > 2048:
                self.block_times.popitem(last=False)
            self.counters["wss_observed_timestamp_blocks"] += len(missing)
        launches = [item for item in logs if _safe_topic(item) == PONS_FACTORY_TOPICS[0]]
        others = [item for item in logs if _safe_topic(item) != PONS_FACTORY_TOPICS[0]]
        for index, item in enumerate(launches + others, start=1):
            try:
                await self.process_log(item, backfill=backfill)
            except (KeyError, TypeError, ValueError, RuntimeError, RpcError):
                self.counters[
                    "backfill_rejected" if backfill else "wss_rejected"
                ] += 1
            if index % 20 == 0:
                await asyncio.sleep(0)

    async def backfill(self) -> None:
        if not self.factory_backfill_supported and not self.curve_backfill_supported:
            await self.skip_unsupported_backfill()
            return
        if await asyncio.to_thread(self.client.chain_id) != CHAIN_ID:
            raise RpcError("CHAIN_ID_MISMATCH")
        latest = await asyncio.to_thread(self.client.block_number)
        default = max(0, latest - self.backfill_blocks + 1)
        stored = int(get_state(self.connection, CHECKPOINT_KEY, default))
        floor = max(0, latest - self.backfill_blocks + 1)
        if stored < floor:
            self.counters["backfill_truncated"] += 1
        start = max(floor, stored - 2)
        for lower in range(start, latest + 1, BACKFILL_CHUNK_BLOCKS):
            if self.stop_event.is_set():
                return
            upper = min(latest, lower + BACKFILL_CHUNK_BLOCKS - 1)
            factory = []
            if self.factory_backfill_supported:
                try:
                    factory = await asyncio.to_thread(
                        _rpc_logs,
                        self.client,
                        from_block=lower,
                        to_block=upper,
                        topics=PONS_FACTORY_TOPICS,
                        address=PONS_V2_FACTORY,
                    )
                except RpcError as exc:
                    if not _history_backfill_can_fallback(exc):
                        raise
                    self.counters["factory_backfill_unsupported"] += 1
                    self.disable_historical_backfill()
            curve = []
            if self.curve_backfill_supported:
                try:
                    curve = await asyncio.to_thread(
                        _rpc_logs,
                        self.client,
                        from_block=lower,
                        to_block=upper,
                        topics=PONS_CURVE_TOPICS,
                    )
                except RpcError as exc:
                    if not _history_backfill_can_fallback(exc):
                        raise
                    self.counters["curve_backfill_unsupported"] += 1
                    self.disable_historical_backfill()
            if not self.factory_backfill_supported and not self.curve_backfill_supported:
                await self.skip_unsupported_backfill(latest)
                return
            await self.process_batch(factory + curve, backfill=True)
            self._advance_curve_watermark(
                await self.block_timestamp_ms(upper)
            )
            set_state(
                self.connection,
                CHECKPOINT_KEY,
                upper,
                int(time.time() * 1000),
            )
            self.counters["backfill_batches"] += 1
        self.last_success["backfill"] = int(time.time() * 1000)

    def health_payload(self) -> Dict[str, Any]:
        return {
            "schema_version": 1,
            "status": self.status,
            "updated_at_ms": int(time.time() * 1000),
            "started_at_ms": self.started_at_ms,
            "shadow_only": True,
            "source_db_read_only": True,
            "telegram_enabled": False,
            "no_signing": True,
            "no_broadcast": True,
            "connected": self.connected,
            "source_mode": self.source_mode,
            "endpoint_slot": (
                "public_poll"
                if self.source_mode == "http_poll"
                else self.endpoints[self.endpoint_index][0]
            ),
            "checkpoint_block": get_state(self.connection, CHECKPOINT_KEY),
            "curve_watermark_ms": get_state(
                self.connection, CURVE_WATERMARK_KEY
            ),
            "curve_coverage_start_ms": get_state(
                self.connection, CURVE_COVERAGE_START_KEY
            ),
            "factory_backfill_supported": self.factory_backfill_supported,
            "curve_backfill_supported": self.curve_backfill_supported,
            "block_timestamp_batch_supported": getattr(
                self, "block_timestamp_batch_supported", True
            ),
            "counts": dict(self.counts_cache),
            "counters": dict(sorted(self.counters.items())),
            "last_success": dict(sorted(self.last_success.items())),
            "last_error": self.last_error,
        }

    async def health_loop(self) -> None:
        while not self.stop_event.is_set():
            write_health(self.health_path, self.health_payload())
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                pass

    async def poll_once(self) -> None:
        if self.poll_client is None:
            raise RuntimeError("Pons public poll unavailable")
        latest = await asyncio.to_thread(self.poll_client.block_number)
        stored = int(get_state(self.connection, CHECKPOINT_KEY, latest) or latest)
        floor = max(0, latest - POLL_WINDOW_BLOCKS + 1)
        if stored < floor:
            self.counters["poll_truncated"] += 1
        lower = max(floor, min(latest, stored) - POLL_OVERLAP_BLOCKS)
        logs = await asyncio.to_thread(
            _public_poll_logs,
            self.poll_client,
            from_block=lower,
            to_block=latest,
        )
        await self.process_batch(logs, backfill=False)
        now_ms = int(time.time() * 1000)
        self._advance_curve_watermark(now_ms)
        set_state(self.connection, CHECKPOINT_KEY, latest, now_ms)
        self.connected = True
        self.last_success["poll"] = now_ms
        self.counters["poll_batches"] += 1
        self.counters["poll_raw_logs"] += len(logs)

    async def poll_source(self) -> None:
        if self.poll_client is None:
            raise RuntimeError("Pons public poll unavailable")
        if await asyncio.to_thread(self.poll_client.chain_id) != CHAIN_ID:
            raise RpcError("CHAIN_ID_MISMATCH")
        self.source_mode = "http_poll"
        coverage_start_ms = int(time.time() * 1000)
        set_state(
            self.connection,
            CURVE_COVERAGE_START_KEY,
            coverage_start_ms,
            coverage_start_ms,
        )
        while not self.stop_event.is_set():
            await self.poll_once()
            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=POLL_INTERVAL_SECONDS
                )
            except asyncio.TimeoutError:
                pass

    async def wss_source_once(self) -> None:
        self.source_mode = "wss"
        await self.backfill()
        endpoint = self.endpoints[self.endpoint_index][2]
        async for batch in stream_pons_logs_once(endpoint, self.stop_event):
            if batch is None:
                self.connected = True
                self.last_success["wss"] = int(time.time() * 1000)
                await self.backfill()
                coverage_start_ms = int(time.time() * 1000)
                set_state(
                    self.connection,
                    CURVE_COVERAGE_START_KEY,
                    coverage_start_ms,
                    coverage_start_ms,
                )
                continue
            await self.process_batch(batch, backfill=False)
            current = int(get_state(self.connection, CHECKPOINT_KEY, 0) or 0)
            set_state(
                self.connection,
                CHECKPOINT_KEY,
                max(current, max(_block_number(item) for item in batch)),
                int(time.time() * 1000),
            )
        self.connected = False

    async def source_loop(self) -> None:
        backoff = 1
        use_poll = self.poll_primary
        while not self.stop_event.is_set():
            try:
                if use_poll:
                    await self.poll_source()
                else:
                    await self.wss_source_once()
            except asyncio.CancelledError:
                self.connected = False
                raise
            except Exception as exc:
                self.connected = False
                self.counters["source_errors"] += 1
                self.last_error = {
                    "at_ms": int(time.time() * 1000),
                    "type": type(exc).__name__,
                    "code": exc.code if isinstance(exc, RpcError) else None,
                    "status": exc.status if isinstance(exc, RpcError) else None,
                }
                if use_poll:
                    use_poll = False
                    self.counters["poll_failover_wss"] += 1
                else:
                    self.endpoint_index = (
                        self.endpoint_index + 1
                    ) % len(self.endpoints)
                await asyncio.sleep(backoff)
                backoff = min(60, backoff * 2)
                continue
            backoff = 1

    async def run(self, run_seconds: int = 0) -> None:
        await self.seed_from_radar()
        self.status = "running"
        write_health(self.health_path, self.health_payload())
        _log(
            "PONS_CURVE_SHADOW_START",
            shadow_only=True,
            telegram_enabled=False,
            no_signing=True,
            no_broadcast=True,
        )
        source = asyncio.create_task(self.source_loop())
        health = asyncio.create_task(self.health_loop())
        reconcile = asyncio.create_task(self.seed_reconcile_loop())
        counts = asyncio.create_task(self.counts_refresh_loop())
        timer = None
        if run_seconds:
            async def timed_stop() -> None:
                await asyncio.sleep(run_seconds)
                self.stop_event.set()
            timer = asyncio.create_task(timed_stop())
        await self.stop_event.wait()
        source.cancel()
        health.cancel()
        reconcile.cancel()
        counts.cancel()
        if timer is not None:
            timer.cancel()
        await asyncio.gather(
            source,
            health,
            reconcile,
            counts,
            *([timer] if timer is not None else []),
            return_exceptions=True,
        )
        self.connected = False
        self.status = "stopped"
        write_health(self.health_path, self.health_payload())
        _log("PONS_CURVE_SHADOW_STOP", counters=dict(sorted(self.counters.items())))


def arguments():
    parser = argparse.ArgumentParser(description="Pons curve shadow collector")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--source-db", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--health-path", type=Path, required=True)
    parser.add_argument("--backfill-blocks", type=int, default=100)
    parser.add_argument("--run-seconds", type=int, default=0)
    return parser.parse_args()


async def async_main(args) -> int:
    paths = (args.source_db, args.db_path, args.health_path)
    if any(not str(path) or path == Path("/") for path in paths):
        raise ValueError("unsafe shadow path")
    if args.source_db.resolve() == args.db_path.resolve():
        raise ValueError("shadow database must be separate")
    if not 10 <= args.backfill_blocks <= 10_000:
        raise ValueError("backfill blocks outside 10..10000")
    if not 0 <= args.run_seconds <= 86_400:
        raise ValueError("run-seconds outside 0..86400")
    daemon = PonsCurveShadow(
        env_file=args.env_file,
        source_db=args.source_db,
        db_path=args.db_path,
        health_path=args.health_path,
        backfill_blocks=args.backfill_blocks,
    )
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, daemon.stop_event.set)
        except NotImplementedError:
            pass
    try:
        await daemon.run(args.run_seconds)
    finally:
        daemon.close()
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main(arguments()))
    except Exception as exc:
        _log(
            "PONS_CURVE_SHADOW_FATAL",
            error_type=type(exc).__name__,
            no_signing=True,
            no_broadcast=True,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
