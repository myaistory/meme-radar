from dataclasses import dataclass


@dataclass(frozen=True)
class RadarConfig:
    """Versioned Phase 0 policy defaults.

    The values are deliberately conservative. They are test policy, not proof
    that a token is safe or profitable.
    """

    ruleset_version: str = "radar_v8_ratio_any_one"
    feature_version: str = "features_v6_ratio_any_one"
    priority_version: str = "priority_v1"
    sample_version: str = "sample_v6_ratio_any_one"
    max_raw_payload_bytes: int = 256 * 1024
    max_token_age_ms: int = 2 * 60 * 60 * 1000
    max_source_age_ms: int = 5 * 60 * 1000
    max_future_skew_ms: int = 2 * 60 * 1000
    max_price_age_ms: int = 60 * 1000
    max_enrichment_age_ms: int = 31 * 60 * 1000
    size_recheck_ms: tuple = (
        5 * 60 * 1000,
        10 * 60 * 1000,
        20 * 60 * 1000,
        30 * 60 * 1000,
    )
    enrichment_delay_ms: tuple = (30 * 1000, 2 * 60 * 1000, 5 * 60 * 1000)
    enrichment_min_retry_ms: tuple = (0, 60 * 1000, 2 * 60 * 1000)
    priority_cohort_ms: int = 2 * 60 * 1000
    narrative_window_ms: int = 30 * 60 * 1000
    narrative_burst_threshold: int = 5
    strong_opportunity_min: int = 65
    strong_confidence_min: int = 75
    medium_opportunity_min: int = 55
    medium_confidence_min: int = 60
    sample_opportunity_min: int = 20
    sample_confidence_min: int = 50
    sample_max_age_ms: int = 4 * 60 * 1000
    sample_min_volume_5m_usd: float = 10.0
    sample_min_buys_5m: int = 10
