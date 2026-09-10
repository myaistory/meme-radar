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


def _argument_map(arguments: Any) -> Dict[str, Any]:
    if not isinstance(arguments, list):
        raise ValueError("arguments must be a list")
    result: Dict[str, Any] = {}
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
        result[item["Name"].lower()] = selected
    return result


def _pick(args: Dict[str, Any], *names: str) -> Any:
    for name in names:
        value = args.get(name)
        if value not in (None, ""):
            return value
    return ""


def parse_fourmeme(
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
            event = build_event(
                source="bitquery",
                source_event_id="fourmeme:%s:%d" % (tx_hash, index),
                event_kind="launch",
                chain="bsc",
                token_address=_pick(args, "token", "tokenaddress", "ca"),
                source_published_at_ms=published,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                launchpad="four_meme",
                token_created_at_ms=published,
                name=_pick(args, "name", "tokenname"),
                symbol=_pick(args, "symbol", "tokensymbol"),
                creator=tx.get("From", ""),
                quote_symbol="BNB",
                tx_hash=tx_hash,
                is_backfill=is_backfill,
                raw_sha256=raw_hash,
            )
            events.append(event)
        except (KeyError, TypeError, ValueError) as exc:
            issues.append(ParseIssue(index, "INVALID_FOURMEME_EVENT", str(exc)))
    return ParseBatch(tuple(events), tuple(issues), raw_hash)
