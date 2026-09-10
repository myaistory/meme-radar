from __future__ import annotations

from typing import Any, Dict, List

from ..config import RadarConfig
from ..models import KNOWN_CHAIN_IDS, ParseBatch, ParseIssue, RadarEvent
from ..normalize import (
    assert_payload_size,
    build_event,
    normalize_chain,
    parse_timestamp_ms,
    payload_sha256,
)


_ALLOWED_ALERT_TYPES = {
    "buy",
    "sell",
    "thesis",
    "whale",
    "price",
    "trade",
}


def parse_fomo_alerts(
    payload: Dict[str, Any],
    *,
    observed_at_ms: int,
    received_at_ms: int,
    is_backfill: bool = False,
    config: RadarConfig = RadarConfig(),
) -> ParseBatch:
    assert_payload_size(payload, config.max_raw_payload_bytes)
    raw_hash = payload_sha256(payload)
    rows = payload.get("alerts")
    if not isinstance(rows, list):
        raise ValueError("alerts must be a list")

    events: List[RadarEvent] = []
    issues: List[ParseIssue] = []
    for index, row in enumerate(rows):
        try:
            if not isinstance(row, dict):
                raise ValueError("alert must be an object")
            event_kind = str(row.get("type", "")).lower()
            if event_kind not in _ALLOWED_ALERT_TYPES:
                raise ValueError("unknown alert type")
            published = parse_timestamp_ms(row.get("ts"))
            chain = normalize_chain(row.get("chain", ""))
            claimed_chain_id = row.get("chainId")
            if (
                claimed_chain_id is not None
                and int(claimed_chain_id) != KNOWN_CHAIN_IDS[chain]
            ):
                raise ValueError("chain id mismatch")
            source_id = row.get("eventId") or row.get("id")
            if not isinstance(source_id, str) or not source_id:
                raise ValueError("missing alert identity")
            event = build_event(
                source="fomo",
                source_event_id=source_id,
                event_kind=event_kind,
                chain=chain,
                token_address=row.get("tokenAddress", ""),
                source_published_at_ms=published,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                name=row.get("token", ""),
                symbol=row.get("token", ""),
                creator=row.get("trader", ""),
                is_backfill=is_backfill,
                raw_sha256=raw_hash,
            )
            events.append(event)
        except (TypeError, ValueError) as exc:
            issues.append(ParseIssue(index, "INVALID_FOMO_ALERT", str(exc)))
    return ParseBatch(tuple(events), tuple(issues), raw_hash)
