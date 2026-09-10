import unittest

from meme_radar import FeatureSnapshot, RadarConfig, evaluate
from meme_radar.normalize import build_event


NOW = 1_788_516_000_000


def make_event(created_at=NOW, backfill=False):
    return build_event(
        source="fixture",
        source_event_id="one",
        event_kind="launch",
        chain="bsc",
        token_address="0x1111111111111111111111111111111111111111",
        source_published_at_ms=NOW,
        observed_at_ms=NOW,
        received_at_ms=NOW + 10,
        token_created_at_ms=created_at,
        name="Bull Run",
        symbol="BULL",
        is_backfill=backfill,
    )


class ScoringTests(unittest.TestCase):
    def test_unknown_security_can_never_be_strong(self):
        event = make_event()
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=NOW + 10,
            source_count=3,
            metadata_complete=True,
            narrative_burst=10,
            cross_chain_count=3,
            hot_term_weight=2,
            unique_buyers=100,
            unique_sellers=10,
            liquidity_usd=100_000,
        )
        decision = evaluate(event, features)
        self.assertEqual("review", decision.risk_verdict)
        self.assertEqual("weak", decision.delivery)
        self.assertIn("SECURITY_UNKNOWN", decision.reason_codes)

    def test_complete_evidence_can_be_strong(self):
        event = make_event()
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=NOW + 10,
            source_count=3,
            metadata_complete=True,
            security_status="safe",
            sell_simulation_ok=True,
            creator_risk="safe",
            price_age_ms=5000,
            liquidity_usd=100_000,
            unique_buyers=100,
            unique_sellers=10,
            narrative_burst=10,
            cross_chain_count=3,
            hot_term_weight=2,
            quote_narrative=True,
        )
        decision = evaluate(event, features)
        self.assertEqual("pass", decision.risk_verdict)
        self.assertEqual("strong", decision.delivery)

    def test_market_activity_can_create_strong_signal_without_narrative_burst(self):
        event = make_event()
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=NOW + 10,
            metadata_complete=True,
            security_status="safe",
            sell_simulation_ok=True,
            creator_risk="safe",
            price_age_ms=1000,
            liquidity_usd=60_000,
            volume_24h_usd=300_000,
            buy_transactions_24h=120,
            sell_transactions_24h=50,
        )
        decision = evaluate(event, features)
        self.assertEqual("pass", decision.risk_verdict)
        self.assertEqual(65, decision.opportunity_score)
        self.assertEqual("strong", decision.delivery)

    def test_old_token_is_rejected(self):
        config = RadarConfig(max_token_age_ms=1000)
        event = make_event(created_at=NOW - 1001)
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=NOW + 10,
            security_status="safe",
            sell_simulation_ok=True,
            creator_risk="safe",
            price_age_ms=0,
        )
        decision = evaluate(event, features, config)
        self.assertEqual("reject", decision.risk_verdict)
        self.assertEqual("suppress", decision.delivery)
        self.assertIn("TOKEN_TOO_OLD", decision.reason_codes)

    def test_backfill_is_review_only(self):
        event = make_event(backfill=True)
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=NOW + 10,
            security_status="safe",
            sell_simulation_ok=True,
            creator_risk="safe",
            price_age_ms=0,
        )
        self.assertEqual("review", evaluate(event, features).risk_verdict)

    def test_stale_realtime_source_is_rejected(self):
        event = build_event(
            source="fixture",
            source_event_id="stale",
            event_kind="launch",
            chain="bsc",
            token_address="0x1111111111111111111111111111111111111111",
            source_published_at_ms=NOW - 301_000,
            observed_at_ms=NOW,
            received_at_ms=NOW + 10,
            token_created_at_ms=NOW,
        )
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=NOW + 10,
            security_status="safe",
            sell_simulation_ok=True,
            creator_risk="safe",
            price_age_ms=0,
        )
        decision = evaluate(event, features)
        self.assertEqual("reject", decision.risk_verdict)
        self.assertIn("SOURCE_TOO_OLD", decision.reason_codes)


if __name__ == "__main__":
    unittest.main()
