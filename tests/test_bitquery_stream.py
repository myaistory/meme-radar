import json
import unittest

from meme_radar.bitquery_stream import (
    DEFAULT_SUBSCRIPTIONS,
    FOURMEME_SUBSCRIPTION,
    PONS_CALLS_SUBSCRIPTION,
    PONS_EVENTS_SUBSCRIPTION,
    decode_protocol_message,
)
from meme_radar.launchpads import (
    FOURMEME_BSC_PROXY,
    PONS_V2_FACTORY,
    PONS_V2_ROUTER,
)


class BitqueryStreamTests(unittest.TestCase):
    def test_queries_bind_documented_contracts(self):
        self.assertIn(FOURMEME_BSC_PROXY, FOURMEME_SUBSCRIPTION)
        self.assertIn(PONS_V2_FACTORY, PONS_CALLS_SUBSCRIPTION)
        self.assertIn(PONS_V2_ROUTER, PONS_CALLS_SUBSCRIPTION)
        self.assertIn(PONS_V2_FACTORY, PONS_EVENTS_SUBSCRIPTION)
        self.assertIn("TokenLaunched", PONS_EVENTS_SUBSCRIPTION)
        self.assertEqual({"fourmeme", "pons_events"}, set(DEFAULT_SUBSCRIPTIONS))

    def test_decodes_known_next_message(self):
        raw = json.dumps(
            {
                "id": "fourmeme",
                "type": "next",
                "payload": {"data": {"EVM": {"Events": []}}},
            }
        )
        kind, record, error = decode_protocol_message(
            raw, DEFAULT_SUBSCRIPTIONS.keys()
        )
        self.assertEqual("next", kind)
        self.assertEqual("fourmeme", record.subscription_id)
        self.assertIsNone(error)

    def test_unknown_subscription_and_bad_json_fail_closed(self):
        raw = json.dumps(
            {
                "id": "unknown",
                "type": "next",
                "payload": {"data": {}},
            }
        )
        kind, record, error = decode_protocol_message(
            raw, DEFAULT_SUBSCRIPTIONS.keys()
        )
        self.assertEqual("error", kind)
        self.assertIsNone(record)
        self.assertEqual("UNKNOWN_SUBSCRIPTION", error)
        self.assertEqual(
            ("error", None, "INVALID_JSON"),
            decode_protocol_message("{", DEFAULT_SUBSCRIPTIONS.keys()),
        )
        self.assertEqual(
            ("error", None, "INVALID_JSON"),
            decode_protocol_message(
                '{"type":"ping","type":"next"}',
                DEFAULT_SUBSCRIPTIONS.keys(),
            ),
        )


if __name__ == "__main__":
    unittest.main()
