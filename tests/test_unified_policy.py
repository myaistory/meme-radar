import unittest

from meme_radar.unified_policy import (
    GmgnDiscoveryEvidence,
    GmgnSafetyEvidence,
    UnifiedSafetyEvidence,
    any_buy_sell_ratio_at_least_one,
    gmgn_discovery_reasons,
    gmgn_safety_reasons,
    market_size_reasons,
    unified_safety_reasons,
)


class UnifiedPolicyTests(unittest.TestCase):
    def evidence(self, **changes):
        values = {
            "market_cap_usd": 200_000,
            "creator_inbound_count": 0,
            "creator_outbound_count": 0,
            "creator_retention_ratio": None,
            "creator_balance_present": False,
            "identity_count": 1,
            "identity_market_cap_rank": 1,
        }
        values.update(changes)
        return UnifiedSafetyEvidence(**values)

    def test_clean_creator_absence_passes(self):
        self.assertEqual((), unified_safety_reasons(self.evidence()))

    def test_market_cap_is_strictly_above_100k(self):
        self.assertEqual(
            ("market_cap_not_above_min",),
            unified_safety_reasons(self.evidence(market_cap_usd=100_000)),
        )

    def test_market_size_prefilter_is_fail_closed_and_strict(self):
        self.assertEqual(
            ("market_cap_not_above_min", "holder_count_not_above_min"),
            market_size_reasons(100_000, 100),
        )
        self.assertEqual((), market_size_reasons(100_000.01, 101))
        self.assertEqual(
            ("market_cap_unknown", "holder_count_unknown"),
            market_size_reasons(None, None),
        )

    def test_only_unsold_full_retention_is_blocked(self):
        self.assertEqual(
            ("creator_full_retention_blocked",),
            unified_safety_reasons(
                self.evidence(
                    creator_inbound_count=1,
                    creator_retention_ratio=1.0,
                )
            ),
        )
        self.assertEqual(
            (),
            unified_safety_reasons(
                self.evidence(
                    creator_inbound_count=1,
                    creator_outbound_count=1,
                    creator_retention_ratio=0.0,
                )
            ),
        )
        self.assertEqual(
            (),
            unified_safety_reasons(
                self.evidence(
                    creator_inbound_count=1,
                    creator_outbound_count=0,
                    creator_retention_ratio=0.5,
                )
            ),
        )

    def test_identity_comparison_is_fail_closed(self):
        self.assertEqual(
            ("identity_market_cap_incomplete",),
            unified_safety_reasons(
                self.evidence(identity_count=2, identity_market_cap_rank=None)
            ),
        )

    def test_balance_without_visible_buy_is_not_this_gate(self):
        self.assertEqual(
            (),
            unified_safety_reasons(
                self.evidence(creator_balance_present=True)
            ),
        )

    def test_gmgn_explicit_risks_are_vetoes(self):
        reasons = gmgn_safety_reasons(
            GmgnSafetyEvidence(
                market_cap_usd=19_000,
                creator_token_status="creator_close",
                is_honeypot=True,
                is_open_source=False,
                owner_renounced=False,
                is_wash_trading=True,
                rug_ratio=0.31,
                top_10_holder_rate=0.51,
                bundler_rate=0.31,
                insider_rate=0.31,
                entrapment_ratio=0.31,
            )
        )
        self.assertEqual(10, len(reasons))
        self.assertNotIn("gmgn_creator_sell_blocked", reasons)
        self.assertIn("gmgn_honeypot", reasons)

    def test_gmgn_missing_optional_fields_are_not_false_passes(self):
        self.assertEqual(
            (),
            gmgn_safety_reasons(
                GmgnSafetyEvidence(
                    market_cap_usd=None,
                    creator_token_status="",
                    is_honeypot=None,
                    is_open_source=None,
                    owner_renounced=None,
                    is_wash_trading=None,
                    rug_ratio=None,
                    top_10_holder_rate=None,
                    bundler_rate=None,
                    insider_rate=None,
                    entrapment_ratio=None,
                )
            ),
        )

    def discovery(self, **changes):
        values = {
            "chain": "bsc",
            "market_cap_usd": 200_000,
            "holder_count": 101,
            "liquidity_usd": 50_000,
            "volume_5m_usd": 10_000,
            "swaps_5m": 100,
            "buys_5m": 60,
            "sells_5m": 30,
            "smart_degen_count": 3,
            "creator_close": True,
            "is_honeypot": False,
            "is_open_source": True,
            "owner_renounced": True,
            "renounced_mint": None,
            "renounced_freeze": None,
            "is_wash_trading": False,
            "rug_ratio": 0.1,
            "top_10_holder_rate": 0.2,
            "bundler_rate": 0.1,
            "insider_rate": 0.1,
            "entrapment_ratio": 0.1,
            "dev_team_hold_rate": 0.1,
        }
        values.update(changes)
        return GmgnDiscoveryEvidence(**values)

    def test_gmgn_growth_requires_complete_safety_and_dev_sale(self):
        self.assertEqual((), gmgn_discovery_reasons(self.discovery()))
        reasons = gmgn_discovery_reasons(
            self.discovery(
                creator_close=False,
                is_wash_trading=None,
                rug_ratio=None,
            )
        )
        self.assertIn("gmgn_discovery_dev_not_confirmed_sold", reasons)
        self.assertIn("gmgn_discovery_wash_status_not_safe", reasons)
        self.assertIn("gmgn_discovery_rug_unknown", reasons)

    def test_one_provider_buy_sell_ratio_at_one_is_enough(self):
        self.assertTrue(
            any_buy_sell_ratio_at_least_one((69, 68), (70, 102))
        )
        self.assertTrue(
            any_buy_sell_ratio_at_least_one((8, 10), (10, 10))
        )
        self.assertFalse(
            any_buy_sell_ratio_at_least_one((8, 10), (9, 10))
        )

    def test_solana_growth_requires_renounced_mint_and_freeze(self):
        evidence = self.discovery(
            chain="solana",
            is_honeypot=None,
            is_open_source=None,
            owner_renounced=None,
            renounced_mint=True,
            renounced_freeze=True,
        )
        self.assertEqual((), gmgn_discovery_reasons(evidence))
        reasons = gmgn_discovery_reasons(
            self.discovery(
                chain="solana",
                is_honeypot=None,
                is_open_source=None,
                owner_renounced=None,
                renounced_mint=False,
                renounced_freeze=None,
            )
        )
        self.assertIn("gmgn_discovery_mint_not_renounced", reasons)
        self.assertIn("gmgn_discovery_freeze_not_renounced", reasons)


if __name__ == "__main__":
    unittest.main()
