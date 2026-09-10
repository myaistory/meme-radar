from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, Tuple


KNOWN_CHAIN_IDS = {
    "ethereum": 1,
    "bsc": 56,
    "base": 8453,
    "robinhood": 4663,
    "solana": 1399811149,
}
KNOWN_EVENT_KINDS = {"launch", "buy", "sell", "thesis", "whale", "price", "trade"}
_EVM_ADDRESS = re.compile(r"^0x[a-f0-9]{40}$")
_SOLANA_ADDRESS = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")


@dataclass(frozen=True)
class ParseIssue:
    index: int
    code: str
    detail: str


@dataclass(frozen=True)
class RadarEvent:
    schema_version: int
    event_id: str
    source: str
    source_event_id: str
    event_kind: str
    chain: str
    chain_id: int
    token_address: str
    source_published_at_ms: int
    observed_at_ms: int
    received_at_ms: int
    launchpad: str = ""
    token_created_at_ms: Optional[int] = None
    name: str = ""
    symbol: str = ""
    creator: str = ""
    quote_symbol: str = ""
    tx_hash: str = ""
    is_backfill: bool = False
    raw_sha256: str = ""

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported event schema")
        if not self.event_id or not self.source or not self.source_event_id:
            raise ValueError("missing event identity")
        if self.chain not in KNOWN_CHAIN_IDS:
            raise ValueError("unknown chain")
        if self.chain_id != KNOWN_CHAIN_IDS[self.chain]:
            raise ValueError("chain id mismatch")
        if self.event_kind not in KNOWN_EVENT_KINDS:
            raise ValueError("unknown event kind")
        if self.chain == "solana":
            if not _SOLANA_ADDRESS.fullmatch(self.token_address):
                raise ValueError("invalid Solana token address")
        elif not _EVM_ADDRESS.fullmatch(self.token_address):
            raise ValueError("invalid EVM token address")
        if self.raw_sha256 and not _SHA256.fullmatch(self.raw_sha256):
            raise ValueError("invalid raw payload hash")
        for value in (
            self.source_published_at_ms,
            self.observed_at_ms,
            self.received_at_ms,
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError("invalid timestamp")
        if self.received_at_ms < self.observed_at_ms:
            raise ValueError("received before observed")
        if self.token_created_at_ms is not None:
            if (
                not isinstance(self.token_created_at_ms, int)
                or isinstance(self.token_created_at_ms, bool)
                or self.token_created_at_ms <= 0
            ):
                raise ValueError("invalid token creation timestamp")

    @property
    def token_age_ms(self) -> Optional[int]:
        if self.token_created_at_ms is None:
            return None
        return self.observed_at_ms - self.token_created_at_ms

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def stable_dict(self) -> Dict[str, Any]:
        value = self.to_dict()
        for key in (
            "observed_at_ms",
            "received_at_ms",
            "raw_sha256",
            "is_backfill",
        ):
            value.pop(key, None)
        return value


@dataclass(frozen=True)
class ParseBatch:
    events: Tuple[RadarEvent, ...]
    issues: Tuple[ParseIssue, ...]
    raw_sha256: str


@dataclass(frozen=True)
class FeatureSnapshot:
    event_id: str
    evaluated_at_ms: int
    source_count: int = 1
    metadata_complete: bool = False
    security_status: str = "unknown"
    sell_simulation_ok: Optional[bool] = None
    creator_risk: str = "unknown"
    price_age_ms: Optional[int] = None
    liquidity_usd: Optional[float] = None
    volume_24h_usd: Optional[float] = None
    buy_transactions_24h: Optional[int] = None
    sell_transactions_24h: Optional[int] = None
    market_cap_usd: Optional[float] = None
    holder_count: Optional[int] = None
    creator_inbound_count: Optional[int] = None
    creator_outbound_count: Optional[int] = None
    creator_retention_ratio: Optional[float] = None
    identity_count: Optional[int] = None
    identity_market_cap_rank: Optional[int] = None
    external_buy_transactions_5m: Optional[int] = None
    unique_buyers: Optional[int] = None
    unique_sellers: Optional[int] = None
    narrative_burst: int = 1
    cross_chain_count: int = 1
    hot_term_weight: float = 0.0
    quote_narrative: bool = False

    def __post_init__(self) -> None:
        if not self.event_id or self.evaluated_at_ms <= 0:
            raise ValueError("invalid feature identity")
        if self.source_count < 1:
            raise ValueError("source_count must be positive")
        if self.security_status not in {"safe", "unsafe", "unknown"}:
            raise ValueError("invalid security status")
        if self.creator_risk not in {"safe", "malicious", "unknown"}:
            raise ValueError("invalid creator risk")
        if self.price_age_ms is not None and self.price_age_ms < 0:
            raise ValueError("invalid price age")
        if self.liquidity_usd is not None and self.liquidity_usd < 0:
            raise ValueError("invalid liquidity")
        if self.volume_24h_usd is not None and self.volume_24h_usd < 0:
            raise ValueError("invalid volume")
        if self.buy_transactions_24h is not None and self.buy_transactions_24h < 0:
            raise ValueError("invalid buy transaction count")
        if self.sell_transactions_24h is not None and self.sell_transactions_24h < 0:
            raise ValueError("invalid sell transaction count")
        if self.market_cap_usd is not None and self.market_cap_usd < 0:
            raise ValueError("invalid market cap")
        if self.holder_count is not None and self.holder_count < 0:
            raise ValueError("invalid holder count")
        for value in (
            self.creator_inbound_count,
            self.creator_outbound_count,
            self.identity_count,
            self.identity_market_cap_rank,
            self.external_buy_transactions_5m,
        ):
            if value is not None and value < 0:
                raise ValueError("invalid unified evidence count")
        if (
            self.creator_retention_ratio is not None
            and self.creator_retention_ratio < 0
        ):
            raise ValueError("invalid creator retention")
        if self.unique_buyers is not None and self.unique_buyers < 0:
            raise ValueError("invalid buyer count")
        if self.unique_sellers is not None and self.unique_sellers < 0:
            raise ValueError("invalid seller count")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Decision:
    event_id: str
    ruleset_version: str
    feature_version: str
    evaluated_at_ms: int
    risk_verdict: str
    confidence_score: int
    opportunity_score: int
    delivery: str
    reason_codes: Tuple[str, ...]

    def __post_init__(self) -> None:
        if self.risk_verdict not in {"pass", "review", "reject"}:
            raise ValueError("invalid risk verdict")
        if self.delivery not in {"strong", "medium", "weak", "suppress"}:
            raise ValueError("invalid delivery")
        for score in (self.confidence_score, self.opportunity_score):
            if score < 0 or score > 100:
                raise ValueError("score outside 0..100")

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value["reason_codes"] = list(self.reason_codes)
        return value
