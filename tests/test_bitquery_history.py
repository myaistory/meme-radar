import unittest

from meme_radar.bitquery_history import BitqueryHistorySource
from meme_radar.http_json import HttpJsonResult

from support import load_fixture


NOW = 1_788_516_100_000


class FakeTransport:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post_json(self, url, payload, headers=None):
        self.calls.append((url, payload, dict(headers or {})))
        return HttpJsonResult(
            data=self.response,
            status=200,
            bytes_read=100,
            elapsed_ms=1,
            started_at_ms=NOW - 10,
            received_at_ms=NOW,
            rate_limit={},
        )


class BitqueryHistoryTests(unittest.TestCase):
    def test_fourmeme_history_is_marked_backfill(self):
        transport = FakeTransport(
            {"data": load_fixture("fourmeme_events.json")}
        )
        source = BitqueryHistorySource(transport, "token")
        result, batch = source.fetch(
            "fourmeme",
            since_ms=NOW - 60_000,
            limit=100,
        )
        self.assertEqual(2, len(batch.events))
        self.assertTrue(all(event.is_backfill for event in batch.events))
        self.assertEqual("Bearer token", transport.calls[0][2]["Authorization"])
        self.assertEqual(100, transport.calls[0][1]["variables"]["limit"])
        self.assertEqual(200, result.status)

    def test_pons_history_and_graphql_error(self):
        source = BitqueryHistorySource(
            FakeTransport({"data": load_fixture("pons_events.json")}),
            "token",
        )
        _, batch = source.fetch("pons_events", since_ms=NOW - 60_000)
        self.assertEqual(1, len(batch.events))
        self.assertEqual("pons_v2_event", batch.events[0].launchpad)
        failing = BitqueryHistorySource(
            FakeTransport({"errors": [{"message": "bad"}]}),
            "token",
        )
        with self.assertRaisesRegex(RuntimeError, "GraphQL"):
            failing.fetch("pons_events", since_ms=NOW - 60_000)


if __name__ == "__main__":
    unittest.main()
