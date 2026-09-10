from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .models import KNOWN_CHAIN_IDS, RadarEvent


_EVM_ADDRESS = re.compile(r"^0x[a-fA-F0-9]{40}$")
_SOLANA_ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def payload_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def clean_text(value: Any, limit: int) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKC", str(value)).strip()
    text = " ".join(text.split())
    return text[:limit]


def normalize_chain(value: Any) -> str:
    raw = clean_text(value, 32).lower()
    aliases = {
        "bnb": "bsc",
        "eth": "ethereum",
        "hood": "robinhood",
        "rh": "robinhood",
        "sol": "solana",
    }
    chain = aliases.get(raw, raw)
    if chain not in KNOWN_CHAIN_IDS:
        raise ValueError("unknown chain")
    return chain


def normalize_address(chain: str, value: Any) -> str:
    address = clean_text(value, 128)
    if chain == "solana":
        if not _SOLANA_ADDRESS.fullmatch(address):
            raise ValueError("invalid Solana token address")
        return address
    if not _EVM_ADDRESS.fullmatch(address):
        raise ValueError("invalid EVM token address")
    return address.lower()


def parse_timestamp_ms(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("invalid timestamp")
    if isinstance(value, (int, float)):
        number = int(value)
        if number < 10_000_000_000:
            number *= 1000
        if number <= 0:
            raise ValueError("invalid timestamp")
        return number
    if not isinstance(value, str) or not value.strip():
        raise ValueError("invalid timestamp")
    text = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


def make_event_id(
    source: str,
    source_event_id: str,
    chain: str,
    token_address: str,
    event_kind: str,
) -> str:
    material = "|".join(
        (source, source_event_id, chain, token_address, event_kind)
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def build_event(
    *,
    source: str,
    source_event_id: str,
    event_kind: str,
    chain: str,
    token_address: Any,
    source_published_at_ms: int,
    observed_at_ms: int,
    received_at_ms: int,
    launchpad: str = "",
    token_created_at_ms: Optional[int] = None,
    name: Any = "",
    symbol: Any = "",
    creator: Any = "",
    quote_symbol: Any = "",
    tx_hash: Any = "",
    is_backfill: bool = False,
    raw_sha256: str = "",
) -> RadarEvent:
    normalized_chain = normalize_chain(chain)
    normalized_address = normalize_address(normalized_chain, token_address)
    source_name = clean_text(source, 64)
    source_id = clean_text(source_event_id, 160)
    kind = clean_text(event_kind, 32).lower()
    if not source_name or not source_id or not kind:
        raise ValueError("missing normalized identity")
    return RadarEvent(
        schema_version=1,
        event_id=make_event_id(
            source_name, source_id, normalized_chain, normalized_address, kind
        ),
        source=source_name,
        source_event_id=source_id,
        event_kind=kind,
        chain=normalized_chain,
        chain_id=KNOWN_CHAIN_IDS[normalized_chain],
        token_address=normalized_address,
        source_published_at_ms=source_published_at_ms,
        observed_at_ms=observed_at_ms,
        received_at_ms=received_at_ms,
        launchpad=clean_text(launchpad, 64),
        token_created_at_ms=token_created_at_ms,
        name=clean_text(name, 120),
        symbol=clean_text(symbol, 32),
        creator=clean_text(creator, 128),
        quote_symbol=clean_text(quote_symbol, 32),
        tx_hash=clean_text(tx_hash, 160),
        is_backfill=bool(is_backfill),
        raw_sha256=clean_text(raw_sha256, 64),
    )


def assert_payload_size(payload: Dict[str, Any], max_bytes: int) -> None:
    size = len(canonical_json(payload).encode("utf-8"))
    if size > max_bytes:
        raise ValueError("payload exceeds byte limit")
