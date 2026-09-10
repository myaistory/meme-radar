from __future__ import annotations

from typing import Any, Dict, Iterable, List

from ..config import RadarConfig
from ..models import ParseBatch, ParseIssue, RadarEvent
from ..normalize import (
    assert_payload_size,
    build_event,
    normalize_address,
    normalize_chain,
    parse_timestamp_ms,
    payload_sha256,
)


def parse_clanker_tokens(
    payload: Dict[str, Any],
    *,
    expected_chain: str,
    allowed_factories: Iterable[str],
    observed_at_ms: int,
    received_at_ms: int,
    is_backfill: bool = False,
    config: RadarConfig = RadarConfig(),
) -> ParseBatch:
    """Parse Clanker's public token listing with an explicit chain boundary.

    The public token object does not reliably include a chain identifier.
    Callers therefore must bind the endpoint to an expected chain and a
    separately sourced factory allowlist.
    """

    assert_payload_size(payload, config.max_raw_payload_bytes)
    raw_hash = payload_sha256(payload)
    chain = normalize_chain(expected_chain)
    normalized_factories = {
        normalize_address(chain, address) for address in allowed_factories
    }
    if not normalized_factories:
        raise ValueError("empty factory allowlist")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("data must be a list")

    events: List[RadarEvent] = []
    issues: List[ParseIssue] = []
    for index, row in enumerate(rows):
        try:
            if not isinstance(row, dict):
                raise ValueError("token must be an object")
            factory = normalize_address(chain, row.get("factory_address"))
            if factory not in normalized_factories:
                raise ValueError("factory not allowed for expected chain")
            tx_hash = row.get("tx_hash")
            if not isinstance(tx_hash, str) or not tx_hash:
                raise ValueError("missing transaction hash")
            created = parse_timestamp_ms(
                row.get("deployed_at") or row.get("created_at")
            )
            event = build_event(
                source="clanker_public_api",
                source_event_id=tx_hash,
                event_kind="launch",
                chain=chain,
                token_address=row.get("contract_address"),
                source_published_at_ms=created,
                observed_at_ms=observed_at_ms,
                received_at_ms=received_at_ms,
                launchpad=row.get("type") or "clanker",
                token_created_at_ms=created,
                name=row.get("name", ""),
                symbol=row.get("symbol", ""),
                creator=row.get("msg_sender", ""),
                tx_hash=tx_hash,
                is_backfill=is_backfill,
                raw_sha256=raw_hash,
            )
            events.append(event)
        except (TypeError, ValueError) as exc:
            issues.append(ParseIssue(index, "INVALID_CLANKER_TOKEN", str(exc)))
    return ParseBatch(tuple(events), tuple(issues), raw_hash)
