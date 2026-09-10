from __future__ import annotations

from typing import Any, Dict, List

from ..config import RadarConfig
from ..models import ParseBatch, ParseIssue, RadarEvent
from ..normalize import (
    assert_payload_size,
    build_event,
    parse_timestamp_ms,
    payload_sha256,
)


def _first_output_address(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("missing call output")
    raw = value[2:] if value.startswith("0x") else value
    if len(raw) < 64:
        raise ValueError("short call output")
    word = raw[:64]
    if any(char not in "0123456789abcdefABCDEF" for char in word):
        raise ValueError("non-hex call output")
    return "0x" + word[-40:]


def _argument_map(arguments: Any) -> Dict[str, Any]:
    if not isinstance(arguments, list):
        raise ValueError("arguments must be a list")
    output: Dict[str, Any] = {}
    for item in arguments:
        if not isinstance(item, dict) or not isinstance(item.get("Name"), str):
            raise ValueError("invalid argument")
        value = item.get("Value")
        if not isinstance(value, dict):
            raise ValueError("invalid argument value")
        selected = None
        for key in ("string", "address", "bigInteger"):
            if value.get(key) is not None:
                selected = value[key]
                break
        output[item["Name"].lower()] = selected
    return output


def parse_pons(
    payload: Dict[str, Any],
    *,
    observed_at_ms: int,
    received_at_ms: int,
    is_backfill: bool = False,
    config: RadarConfig = RadarConfig(),
) -> ParseBatch:
    assert_payload_size(payload, config.max_raw_payload_bytes)
    raw_hash = payload_sha256(payload)
    try:
        rows = payload["EVM"]["Calls"]
    except (KeyError, TypeError) as exc:
        raise ValueError("missing EVM.Calls") from exc
    if not isinstance(rows, list):
        raise ValueError("EVM.Calls must be a list")

    events: List[RadarEvent] = []
    issues: List[ParseIssue] = []
    for index, row in enumerate(rows):
        try:
            if not isinstance(row, dict):
                raise ValueError("call must be an object")
            block = row.get("Block")
            tx = row.get("Transaction")
            call = row.get("Call")
            if not all(isinstance(value, dict) for value in (block, tx, call)):
                raise ValueError("missing block, transaction or call")
            published = parse_timestamp_ms(block.get("Time"))
            tx_hash = tx.get("Hash")
            if not isinstance(tx_hash, str) or not tx_hash:
                raise ValueError("missing transaction hash")
            event = build_event(
                source="bitquery",
                source_event_id="pons_call:%s:%d" % (tx_hash, index),
                event_kind="launch",
                chain="robinhood",
                token_address=_first_output_address(call.get("Output")),
                source_published_at_ms=published,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                launchpad="pons_v2",
                token_created_at_ms=published,
                creator=tx.get("From", ""),
                tx_hash=tx_hash,
                is_backfill=is_backfill,
                raw_sha256=raw_hash,
            )
            events.append(event)
        except (KeyError, TypeError, ValueError) as exc:
            issues.append(ParseIssue(index, "INVALID_PONS_CALL", str(exc)))
    return ParseBatch(tuple(events), tuple(issues), raw_hash)


def parse_pons_launched(
    payload: Dict[str, Any],
    *,
    observed_at_ms: int,
    received_at_ms: int,
    is_backfill: bool = False,
    config: RadarConfig = RadarConfig(),
) -> ParseBatch:
    assert_payload_size(payload, config.max_raw_payload_bytes)
    raw_hash = payload_sha256(payload)
    try:
        rows = payload["EVM"]["Events"]
    except (KeyError, TypeError) as exc:
        raise ValueError("missing EVM.Events") from exc
    if not isinstance(rows, list):
        raise ValueError("EVM.Events must be a list")

    events: List[RadarEvent] = []
    issues: List[ParseIssue] = []
    for index, row in enumerate(rows):
        try:
            if not isinstance(row, dict):
                raise ValueError("event must be an object")
            block = row.get("Block")
            tx = row.get("Transaction")
            if not isinstance(block, dict) or not isinstance(tx, dict):
                raise ValueError("missing block or transaction")
            published = parse_timestamp_ms(block.get("Time"))
            tx_hash = tx.get("Hash")
            if not isinstance(tx_hash, str) or not tx_hash:
                raise ValueError("missing transaction hash")
            args = _argument_map(row.get("Arguments"))
            token = (
                args.get("token")
                or args.get("tokenaddress")
                or args.get("launchedtoken")
            )
            event = build_event(
                source="bitquery",
                source_event_id="pons_event:%s:%d" % (tx_hash, index),
                event_kind="launch",
                chain="robinhood",
                token_address=token,
                source_published_at_ms=published,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                launchpad="pons_v2_event",
                token_created_at_ms=published,
                creator=args.get("creator") or tx.get("From", ""),
                tx_hash=tx_hash,
                is_backfill=is_backfill,
                raw_sha256=raw_hash,
            )
            events.append(event)
        except (KeyError, TypeError, ValueError) as exc:
            issues.append(ParseIssue(index, "INVALID_PONS_EVENT", str(exc)))
    return ParseBatch(tuple(events), tuple(issues), raw_hash)
