from __future__ import annotations

from typing import List

from .config import RadarConfig
from .models import Decision, FeatureSnapshot, RadarEvent
from .risk import assess_risk


def _clamp(value: float) -> int:
    return max(0, min(100, int(round(value))))


def confidence_score(event: RadarEvent, features: FeatureSnapshot) -> int:
    score = 10.0
    score += min(20, (features.source_count - 1) * 10)
    if features.metadata_complete:
        score += 15
    if event.token_created_at_ms is not None:
        score += 15
    if features.security_status == "safe":
        score += 20
    if features.sell_simulation_ok is True:
        score += 15
    if features.price_age_ms is not None:
        score += 10
    if features.unique_buyers is not None and features.unique_sellers is not None:
        score += 10
    if features.creator_risk == "safe":
        score += 5
    return _clamp(score)


def opportunity_score(features: FeatureSnapshot) -> int:
    score = 0.0
    score += min(25.0, max(0.0, features.hot_term_weight) * 20.0)
    if features.narrative_burst >= 5:
        score += min(20.0, features.narrative_burst * 3.0)
    if features.cross_chain_count >= 2:
        score += 20.0
    if features.quote_narrative:
        score += 15.0
    if features.unique_buyers is not None:
        if features.unique_buyers >= 30:
            score += 15.0
        elif features.unique_buyers >= 10:
            score += 8.0
        elif features.unique_buyers < 5:
            score -= 10.0
    if (
        features.unique_buyers is not None
        and features.unique_sellers is not None
        and features.unique_buyers / max(1, features.unique_sellers) > 1.5
    ):
        score += 10.0
    if features.liquidity_usd is not None:
        if features.liquidity_usd >= 50_000:
            score += 15.0
        elif features.liquidity_usd >= 20_000:
            score += 10.0
        elif features.liquidity_usd < 5_000:
            score -= 15.0
    if features.volume_24h_usd is not None:
        if features.volume_24h_usd >= 250_000:
            score += 20.0
        elif features.volume_24h_usd >= 50_000:
            score += 12.0
        elif features.volume_24h_usd >= 10_000:
            score += 5.0
    buys = features.buy_transactions_24h
    sells = features.sell_transactions_24h
    if buys is not None:
        if buys >= 100:
            score += 20.0
        elif buys >= 30:
            score += 12.0
        elif buys >= 10:
            score += 6.0
    if buys is not None and sells is not None and buys / max(1, sells) > 1.5:
        score += 10.0
    return _clamp(score)


def evaluate(
    event: RadarEvent,
    features: FeatureSnapshot,
    config: RadarConfig = RadarConfig(),
) -> Decision:
    if event.event_id != features.event_id:
        raise ValueError("event and features do not match")
    risk_verdict, risk_reasons = assess_risk(event, features, config)
    confidence = confidence_score(event, features)
    opportunity = opportunity_score(features)
    reasons: List[str] = list(risk_reasons)

    if risk_verdict == "reject":
        delivery = "suppress"
    elif risk_verdict == "review":
        delivery = "weak"
    elif (
        confidence >= config.strong_confidence_min
        and opportunity >= config.strong_opportunity_min
    ):
        delivery = "strong"
    elif (
        confidence >= config.medium_confidence_min
        and opportunity >= config.medium_opportunity_min
    ):
        delivery = "medium"
    else:
        delivery = "weak"

    reasons.extend(
        (
            "CONFIDENCE_%d" % confidence,
            "OPPORTUNITY_%d" % opportunity,
            "DELIVERY_%s" % delivery.upper(),
        )
    )
    return Decision(
        event_id=event.event_id,
        ruleset_version=config.ruleset_version,
        feature_version=config.feature_version,
        evaluated_at_ms=features.evaluated_at_ms,
        risk_verdict=risk_verdict,
        confidence_score=confidence,
        opportunity_score=opportunity,
        delivery=delivery,
        reason_codes=tuple(reasons),
    )
