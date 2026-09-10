from __future__ import annotations

from typing import List, Tuple

from .config import RadarConfig
from .models import FeatureSnapshot, RadarEvent


def assess_risk(
    event: RadarEvent,
    features: FeatureSnapshot,
    config: RadarConfig,
) -> Tuple[str, Tuple[str, ...]]:
    reject: List[str] = []
    review: List[str] = []

    if event.is_backfill:
        review.append("BACKFILL_NOT_REALTIME")
    source_age = (
        event.received_at_ms - event.observed_at_ms
        if event.source.startswith("gmgn_trending")
        else event.observed_at_ms - event.source_published_at_ms
    )
    if source_age < -config.max_future_skew_ms:
        reject.append("SOURCE_TIME_IN_FUTURE")
    elif source_age > config.max_source_age_ms and not event.is_backfill:
        reject.append("SOURCE_TOO_OLD")
    age = event.token_age_ms
    if age is None:
        review.append("TOKEN_AGE_UNKNOWN")
    elif age < -config.max_future_skew_ms:
        reject.append("TOKEN_CREATED_IN_FUTURE")
    elif age > config.max_token_age_ms:
        reject.append("TOKEN_TOO_OLD")

    if features.security_status == "unsafe":
        reject.append("SECURITY_UNSAFE")
    elif features.security_status == "unknown":
        review.append("SECURITY_UNKNOWN")
    if features.sell_simulation_ok is False:
        reject.append("SELL_SIMULATION_FAILED")
    elif features.sell_simulation_ok is None:
        review.append("SELL_SIMULATION_UNKNOWN")
    if features.creator_risk == "malicious":
        reject.append("CREATOR_MALICIOUS")
    elif features.creator_risk == "unknown":
        review.append("CREATOR_RISK_UNKNOWN")
    if features.price_age_ms is None:
        review.append("PRICE_FRESHNESS_UNKNOWN")
    elif features.price_age_ms > config.max_price_age_ms:
        review.append("PRICE_STALE")

    if reject:
        return "reject", tuple(reject + review)
    if review:
        return "review", tuple(review)
    return "pass", ("RISK_CHECKS_PASSED",)
