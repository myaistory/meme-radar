import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from meme_radar.adapters import parse_fourmeme
from meme_radar.http_json import HttpBoundaryError
from meme_radar.outcome_cohort import HORIZON_TOLERANCE_MS, initialize_cohort
from meme_radar.outcome_collector import OutcomeCollector
from meme_radar.replay import replay
from meme_radar.sources.dexscreener import MarketSnapshot
from meme_radar.storage import (
    connect,
    initialize,
    store_decision,
    store_event,
    store_provider_observation,
    store_raw_payload,
)

from support import load_fixture


OBSERVED = 1_788_700_000_000
RECEIVED = OBSERVED + 100


def snapshot(chain, token_address, *, price=0.001, pairs=1, market_cap=50_000.0):
    return MarketSnapshot(
        chain=chain,
        token_address=token_address,
        observed_at_ms=RECEIVED,
        pair_count=pairs,
        best_pair_address="0x" + "9" * 40,
        pair_created_at_ms=RECEIVED,
        price_usd=price,
        liquidity_usd=20_000.0,
        volume_5m_usd=1_000.0,
        volume_1h_usd=5_000.0,
        volume_24h_usd=50_000.0,
        buy_transactions_5m=10,
        sell_transactions_5m=4,
        buy_transactions_1h=40,
        sell_transactions_1h=20,
        buy_transactions_24h=100,
        sell_transactions_24h=60,
        bytes_read=1024,
        elapsed_ms=50,
        market_cap_usd=market_cap,
    )


class FakeMarket:
    def __init__(self, results=None):
        self.results = list(results or [])
        self.calls = []

    def token_market(self, chain, token_address):
        self.calls.append((chain, token_address))
        if not self.results:
            return snapshot(chain, token_address)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        if result is None:
            return snapshot(chain, token_address, price=None, pairs=0)
        return snapshot(chain, token_address, price=result)


class FakeClock:
    def __init__(self, now_ms):
        self.now_ms = now_ms

    def __call__(self):
        return self.now_ms

    def advance(self, ms):
        self.now_ms += ms


class OutcomeCollectorTests(unittest.TestCase):
    def setUp(self):
        self.payload = load_fixture("fourmeme_events.json")
        self.batch = parse_fourmeme(
            self.payload,
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db_path = Path(self.temp.name) / "radar.db"
        self.health_path = Path(self.temp.name) / "health.json"
        connection = connect(self.db_path)
        initialize(connection)
        initialize_cohort(connection)
        store_raw_payload(
            connection,
            source="bitquery",
            batch_id="fixture",
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
            is_backfill=False,
            payload=self.payload,
        )
        self.connection = connection

    def tearDown(self):
        self.connection.close()

    def add_decision(self, index=0, *, delivered=True, evaluated_at_ms=None):
        event = self.batch.events[index]
        store_event(self.connection, event)
        store_provider_observation(
            self.connection,
            event_id=event.event_id,
            provider="unified_gate",
            observed_at_ms=RECEIVED + 5,
            status="ok",
            payload={"reason_codes": []},
            summary={"reason_codes": []},
        )
        decision = replace(
            replay((event,))[0],
            evaluated_at_ms=RECEIVED + 10 if evaluated_at_ms is None else evaluated_at_ms,
            risk_verdict="pass" if delivered else "review",
            delivery="strong" if delivered else "weak",
            confidence_score=90,
            opportunity_score=80,
        )
        store_decision(self.connection, decision)
        return event, decision

    def collector(self, market, clock, **env):
        for key, value in env.items():
            self.addCleanup(self._restore_env, key)
            import os

            os.environ[key] = str(value)
        collector = OutcomeCollector(
            db_path=self.db_path,
            health_path=self.health_path,
            market=market,
            clock=clock,
        )
        collector.connection = self.connection
        return collector

    def _restore_env(self, key):
        import os

        os.environ.pop(key, None)

    def health(self):
        return json.loads(self.health_path.read_text(encoding="utf-8"))

    def test_delivered_candidate_is_enrolled_with_a_baseline(self):
        self.add_decision()
        clock = FakeClock(RECEIVED + 20)
        collector = self.collector(FakeMarket(), clock)
        self.assertEqual(1, collector.enroll_cycle())
        row = self.connection.execute(
            "SELECT stratum, baseline_status, baseline_price_usd,"
            " baseline_market_cap_usd, baseline_at_ms,"
            " decision_evaluated_at_ms FROM outcome_cohort"
        ).fetchone()
        self.assertEqual("delivered", row["stratum"])
        self.assertEqual("ok", row["baseline_status"])
        self.assertEqual(0.001, row["baseline_price_usd"])
        self.assertEqual(50_000.0, row["baseline_market_cap_usd"])
        self.assertEqual(RECEIVED + 20, row["baseline_at_ms"])
        self.assertEqual(RECEIVED + 10, row["decision_evaluated_at_ms"])

    def test_enrollment_does_not_rescan_the_same_window(self):
        self.add_decision()
        clock = FakeClock(RECEIVED + 20)
        market = FakeMarket()
        collector = self.collector(market, clock)
        self.assertEqual(1, collector.enroll_cycle())
        clock.advance(1_000)
        self.assertEqual(0, collector.enroll_cycle())
        self.assertEqual(1, len(market.calls))

    def test_missing_price_is_recorded_as_unavailable_not_zero(self):
        self.add_decision()
        clock = FakeClock(RECEIVED + 20)
        collector = self.collector(FakeMarket([None]), clock)
        collector.enroll_cycle()
        row = self.connection.execute(
            "SELECT baseline_status, baseline_price_usd FROM outcome_cohort"
        ).fetchone()
        self.assertEqual("unavailable", row["baseline_status"])
        self.assertIsNone(row["baseline_price_usd"])

    def enrolled(self, market=None, price=0.001):
        self.add_decision()
        clock = FakeClock(RECEIVED + 20)
        collector = self.collector(market or FakeMarket([price]), clock)
        collector.enroll_cycle()
        return collector, clock

    def test_horizon_is_measured_once_the_window_opens(self):
        market = FakeMarket([0.001, 0.003])
        collector, clock = self.enrolled(market=market)
        self.assertEqual(0, collector.measure_cycle())
        clock.advance(60_000)
        self.assertEqual(1, collector.measure_cycle())
        row = self.connection.execute(
            "SELECT horizon_minutes, outcome_label, price_usd, market_cap_usd,"
            " actual_elapsed_ms, source, cohort_version FROM outcomes"
        ).fetchone()
        self.assertEqual(1, row["horizon_minutes"])
        self.assertEqual("ok", row["outcome_label"])
        self.assertEqual(0.003, row["price_usd"])
        self.assertEqual(50_000.0, row["market_cap_usd"])
        self.assertEqual(0, row["actual_elapsed_ms"])
        self.assertEqual("dexscreener", row["source"])
        self.assertEqual("outcome_cohort_v1", row["cohort_version"])

    def test_missed_window_is_recorded_as_expired_without_a_price(self):
        market = FakeMarket([0.001])
        collector, clock = self.enrolled(market=market)
        clock.advance(60_000 + HORIZON_TOLERANCE_MS[1] + 1)
        self.assertEqual(1, collector.measure_cycle())
        row = self.connection.execute(
            "SELECT outcome_label, price_usd, error_code, actual_elapsed_ms"
            " FROM outcomes"
        ).fetchone()
        self.assertEqual("expired", row["outcome_label"])
        self.assertIsNone(row["price_usd"])
        self.assertEqual("HORIZON_WINDOW_MISSED", row["error_code"])
        self.assertGreater(row["actual_elapsed_ms"], HORIZON_TOLERANCE_MS[1])
        # An expired horizon must not consume a provider call.
        self.assertEqual(1, len(market.calls))

    def test_exhausted_budget_defers_instead_of_writing_an_empty_sample(self):
        market = FakeMarket([0.001])
        collector, clock = self.enrolled(market=market)
        clock.advance(60_000)
        while collector.budget.take():
            pass
        self.assertEqual(0, collector.measure_cycle())
        self.assertEqual(
            0,
            self.connection.execute("SELECT COUNT(*) FROM outcomes").fetchone()[0],
        )
        self.assertEqual(1, collector.counters["budget_deferred"])
        collector.budget = type(collector.budget)(10)
        self.assertEqual(1, collector.measure_cycle())

    def test_provider_error_is_recorded_as_error_not_as_a_price(self):
        market = FakeMarket([0.001, HttpBoundaryError("NETWORK_ERROR")])
        collector, clock = self.enrolled(market=market)
        clock.advance(60_000)
        collector.measure_cycle()
        row = self.connection.execute(
            "SELECT outcome_label, price_usd, error_code FROM outcomes"
        ).fetchone()
        self.assertEqual("error", row["outcome_label"])
        self.assertIsNone(row["price_usd"])
        self.assertEqual("NETWORK_ERROR", row["error_code"])
        self.assertIn("market", collector.last_error)

    def test_rate_limit_puts_the_provider_into_cooldown(self):
        market = FakeMarket(
            [0.001, HttpBoundaryError("HTTP_STATUS", status=429)]
        )
        collector, clock = self.enrolled(market=market)
        clock.advance(60_000)
        collector.measure_cycle()
        self.assertEqual(1, collector.counters["provider_cooldown"])
        self.assertGreater(
            collector.budget.snapshot()["cooldown_seconds"],
            0,
        )

    def test_unsupported_chain_never_calls_the_provider(self):
        market = FakeMarket()
        clock = FakeClock(RECEIVED + 20)
        collector = self.collector(market, clock)
        result = collector.read_market("cardano", "addr")
        self.assertEqual("unavailable", result["status"])
        self.assertEqual("CHAIN_UNSUPPORTED", result["error_code"])
        self.assertEqual([], market.calls)

    def test_health_reports_cohort_strata_and_budget(self):
        market = FakeMarket([0.001, 0.002])
        collector, clock = self.enrolled(market=market)
        clock.advance(60_000)
        collector.cycle()
        payload = self.health()
        self.assertEqual("meme-radar-outcome-collector", payload["service"])
        self.assertTrue(payload["read_only_upstream"])
        self.assertTrue(payload["no_telegram"])
        self.assertEqual([1, 5, 15, 60, 360, 1440], payload["horizons_minutes"])
        self.assertEqual(
            {"delivered": 1, "evaluated_rejected": 0, "not_evaluated": 0},
            payload["cohort"],
        )
        self.assertEqual(1, payload["outcomes"]["ok"])
        self.assertEqual(0, payload["outcomes"]["expired"])
        self.assertEqual(240, payload["provider_budget"]["limit"])
        self.assertEqual(2, payload["counters"]["provider_calls"])

    def test_invalid_environment_setting_is_a_boundary_error(self):
        import os

        os.environ["MEME_RADAR_OUTCOME_POLL_SECONDS"] = "soon"
        self.addCleanup(self._restore_env, "MEME_RADAR_OUTCOME_POLL_SECONDS")
        with self.assertRaisesRegex(ValueError, "invalid integer"):
            OutcomeCollector(
                db_path=self.db_path,
                health_path=self.health_path,
                market=FakeMarket(),
            )

    def test_sample_rate_above_one_is_rejected(self):
        import os

        os.environ["MEME_RADAR_OUTCOME_CONTROL_SAMPLE_RATE"] = "1.5"
        self.addCleanup(
            self._restore_env, "MEME_RADAR_OUTCOME_CONTROL_SAMPLE_RATE"
        )
        with self.assertRaisesRegex(ValueError, "must be <= 1"):
            OutcomeCollector(
                db_path=self.db_path,
                health_path=self.health_path,
                market=FakeMarket(),
            )

    def test_run_stops_after_run_seconds_and_closes_the_connection(self):
        self.add_decision()
        collector = OutcomeCollector(
            db_path=self.db_path,
            health_path=self.health_path,
            market=FakeMarket(),
            clock=FakeClock(RECEIVED + 20),
        )
        self.assertEqual(0, collector.run(run_seconds=1))
        self.assertIsNone(collector.connection)
        self.assertTrue(self.health_path.is_file())


if __name__ == "__main__":
    unittest.main()


class OutcomeCollectorUpstreamTests(unittest.TestCase):
    def test_a_database_without_upstream_tables_is_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            collector = OutcomeCollector(
                db_path=Path(temp) / "empty.db",
                health_path=Path(temp) / "health.json",
                market=FakeMarket([]),
            )
            with self.assertRaises(RuntimeError) as caught:
                collector.run(run_seconds=1)
            self.assertIn("upstream table missing", str(caught.exception))
            self.assertIsNone(collector.connection)


class OutcomeCollectorAddressTests(unittest.TestCase):
    def test_malformed_address_never_consumes_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            collector = OutcomeCollector(
                db_path=Path(temp) / "x.db",
                health_path=Path(temp) / "h.json",
                market=FakeMarket([]),
            )
            result = collector.read_market("bsc", "not-an-address")
            self.assertEqual("unavailable", result["status"])
            self.assertEqual("ADDRESS_INVALID", result["error_code"])
            self.assertEqual(0, collector.budget.snapshot()["used"])
            self.assertEqual(1, collector.counters["skipped_invalid_address"])
