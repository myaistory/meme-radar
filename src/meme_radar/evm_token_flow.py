from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Tuple

from .evm_rpc import HttpRpcClient, RpcError


TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa"
    "952ba7f163c4a11628f55a4df523b3ef"
)
_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")
_WORD = re.compile(r"^0x[0-9a-fA-F]{64}$")


@dataclass(frozen=True)
class CreatorFlowEvidence:
    token_address: str
    creator_address: str
    from_block: int
    to_block: int
    inbound_count: int
    outbound_count: int
    inbound_raw: int
    outbound_raw: int
    balance_raw: int
    total_supply_raw: int
    decimals: int

    @property
    def retention_ratio(self):
        if self.inbound_raw <= 0:
            return None
        return self.balance_raw / self.inbound_raw


def _topic_address(value: Any) -> str:
    if not isinstance(value, str) or not _WORD.fullmatch(value):
        raise ValueError("invalid transfer address topic")
    address = "0x" + value[-40:].lower()
    if not _ADDRESS.fullmatch(address):
        raise ValueError("invalid transfer address")
    return address


def analyze_creator_transfers(
    logs: Iterable[Dict[str, Any]],
    *,
    token_address: str,
    creator_address: str,
) -> Tuple[int, int, int, int]:
    token = token_address.lower()
    creator = creator_address.lower()
    if not _ADDRESS.fullmatch(token) or not _ADDRESS.fullmatch(creator):
        raise ValueError("invalid token or creator address")
    inbound_count = outbound_count = inbound_raw = outbound_raw = 0
    for log in logs:
        if not isinstance(log, dict) or log.get("removed") is True:
            continue
        topics = log.get("topics")
        if (
            str(log.get("address", "")).lower() != token
            or not isinstance(topics, list)
            or len(topics) < 3
            or str(topics[0]).lower() != TRANSFER_TOPIC
        ):
            raise ValueError("transfer log identity mismatch")
        sender = _topic_address(topics[1])
        recipient = _topic_address(topics[2])
        data = str(log.get("data", ""))
        if not _WORD.fullmatch(data):
            raise ValueError("invalid transfer amount")
        amount = int(data, 16)
        if recipient == creator and sender != creator:
            inbound_count += 1
            inbound_raw += amount
        if sender == creator and recipient != creator:
            outbound_count += 1
            outbound_raw += amount
    return inbound_count, outbound_count, inbound_raw, outbound_raw


def _uint_call(client: HttpRpcClient, token: str, data: str) -> int:
    result = client.call("eth_call", [{"to": token, "data": data}, "latest"])
    if not isinstance(result, str) or not result.startswith("0x"):
        raise RpcError("INVALID_TOKEN_CALL")
    return int(result, 16)


def collect_creator_flow(
    client: HttpRpcClient,
    *,
    token_address: str,
    creator_address: str,
    from_block: int,
    to_block: int,
    chunk_blocks: int,
    max_span_blocks: int,
) -> CreatorFlowEvidence:
    token = token_address.lower()
    creator = creator_address.lower()
    span = to_block - from_block + 1
    if (
        not _ADDRESS.fullmatch(token)
        or not _ADDRESS.fullmatch(creator)
        or from_block < 0
        or to_block < from_block
        or not 1 <= chunk_blocks <= max_span_blocks
        or not 1 <= span <= max_span_blocks
    ):
        raise ValueError("creator flow range outside boundary")
    logs: List[Dict[str, Any]] = []
    for lower in range(from_block, to_block + 1, chunk_blocks):
        upper = min(to_block, lower + chunk_blocks - 1)
        result = client.call(
            "eth_getLogs",
            [
                {
                    "address": token,
                    "topics": [TRANSFER_TOPIC],
                    "fromBlock": hex(lower),
                    "toBlock": hex(upper),
                }
            ],
        )
        if not isinstance(result, list) or len(result) > 5000:
            raise RpcError("INVALID_LOG_RESULT")
        logs.extend(item for item in result if isinstance(item, dict))
        if len(logs) > 5000:
            raise RpcError("TOO_MANY_TRANSFER_LOGS")
    inbound_count, outbound_count, inbound_raw, outbound_raw = (
        analyze_creator_transfers(
            logs,
            token_address=token,
            creator_address=creator,
        )
    )
    creator_word = "0" * 24 + creator[2:]
    balance = _uint_call(client, token, "0x70a08231" + creator_word)
    supply = _uint_call(client, token, "0x18160ddd")
    decimals = _uint_call(client, token, "0x313ce567")
    if supply <= 0 or not 0 <= balance <= supply or not 0 <= decimals <= 36:
        raise RpcError("INVALID_TOKEN_SNAPSHOT")
    return CreatorFlowEvidence(
        token_address=token,
        creator_address=creator,
        from_block=from_block,
        to_block=to_block,
        inbound_count=inbound_count,
        outbound_count=outbound_count,
        inbound_raw=inbound_raw,
        outbound_raw=outbound_raw,
        balance_raw=balance,
        total_supply_raw=supply,
        decimals=decimals,
    )
