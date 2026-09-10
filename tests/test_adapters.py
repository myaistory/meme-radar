import unittest
from dataclasses import replace

from meme_radar.adapters import (
    parse_clanker_tokens,
    parse_fomo_alerts,
    parse_fourmeme,
    parse_pons,
    parse_pons_launched,
)
from meme_radar.launchpads import CLANKER_BASE_FACTORIES

from support import load_fixture


OBSERVED = 1_788_516_100_000
RECEIVED = OBSERVED + 50


class AdapterTests(unittest.TestCase):
    def test_clanker_requires_expected_chain_factory(self):
        batch = parse_clanker_tokens(
            load_fixture("clanker_tokens.json"),
            expected_chain="base",
            allowed_factories=CLANKER_BASE_FACTORIES,
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        self.assertEqual(1, len(batch.events))
        self.assertEqual("base", batch.events[0].chain)
        self.assertEqual("clanker_v4", batch.events[0].launchpad)
        self.assertEqual(1, len(batch.issues))
        self.assertIn("factory not allowed", batch.issues[0].detail)

    def test_clanker_rejects_empty_factory_allowlist(self):
        with self.assertRaisesRegex(ValueError, "empty factory"):
            parse_clanker_tokens(
                load_fixture("clanker_tokens.json"),
                expected_chain="base",
                allowed_factories=(),
                observed_at_ms=OBSERVED,
                received_at_ms=RECEIVED,
            )

    def test_fourmeme_keeps_every_event_in_batch(self):
        batch = parse_fourmeme(
            load_fixture("fourmeme_events.json"),
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        self.assertEqual(2, len(batch.events))
        self.assertEqual(0, len(batch.issues))
        self.assertEqual("0x1111111111111111111111111111111111111111", batch.events[0].token_address)
        self.assertEqual("牛来", batch.events[1].name)

    def test_pons_decodes_address_and_reports_bad_row(self):
        batch = parse_pons(
            load_fixture("pons_calls.json"),
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        self.assertEqual(1, len(batch.events))
        self.assertEqual("0x3333333333333333333333333333333333333333", batch.events[0].token_address)
        self.assertEqual(1, len(batch.issues))
        self.assertEqual("INVALID_PONS_CALL", batch.issues[0].code)

    def test_pons_launched_event_is_separate_identity(self):
        event_batch = parse_pons_launched(
            load_fixture("pons_events.json"),
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        call_batch = parse_pons(
            load_fixture("pons_calls.json"),
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        self.assertEqual(1, len(event_batch.events))
        self.assertEqual(0, len(event_batch.issues))
        self.assertNotEqual(
            event_batch.events[0].source_event_id,
            call_batch.events[0].source_event_id,
        )
        self.assertEqual("pons_v2_event", event_batch.events[0].launchpad)

    def test_fomo_unknown_type_is_fail_closed(self):
        batch = parse_fomo_alerts(
            load_fixture("fomo_alerts.json"),
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        self.assertEqual(1, len(batch.events))
        self.assertEqual("buy", batch.events[0].event_kind)
        self.assertIsNone(batch.events[0].token_created_at_ms)
        self.assertEqual(1, len(batch.issues))

    def test_fomo_chain_id_mismatch_is_fail_closed(self):
        payload = load_fixture("fomo_alerts.json")
        payload["alerts"] = [dict(payload["alerts"][0], chainId=1)]
        batch = parse_fomo_alerts(
            payload,
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        self.assertEqual(0, len(batch.events))
        self.assertEqual("INVALID_FOMO_ALERT", batch.issues[0].code)

    def test_payload_limit_applies_to_actual_encoded_bytes(self):
        config = __import__("meme_radar").RadarConfig(max_raw_payload_bytes=8)
        with self.assertRaisesRegex(ValueError, "byte limit"):
            parse_fomo_alerts(
                {"alerts": []},
                observed_at_ms=OBSERVED,
                received_at_ms=RECEIVED,
                config=config,
            )

    def test_event_model_cannot_bypass_address_validation(self):
        batch = parse_fourmeme(
            load_fixture("fourmeme_events.json"),
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        with self.assertRaisesRegex(ValueError, "invalid EVM"):
            replace(batch.events[0], token_address="not-an-address")


if __name__ == "__main__":
    unittest.main()
