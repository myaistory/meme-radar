import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from meme_radar.adapters import parse_fourmeme
from meme_radar.outcome_cohort import (
    COHORT_VERSION,
    HORIZON_MINUTES,
    HORIZON_TOLERANCE_MS,
    cohort_counts,
    due_horizons,
    enroll_cohort_member,
    initialize_cohort,
    outcome_counts,
    sample_fraction,
    select_enrollment_candidates,
    store_outcome,
)
from meme_radar.replay import replay
from meme_radar.storage import (
    connect,
    initialize,
    store_decision,
    store_event,
    store_provider_observation,
    store_raw_payload,
)

from support import load_fixture


OBSERVED = 1_788_600_000_000
RECEIVED = OBSERVED + 100


class OutcomeCohortTests(unittest.TestCase):
    def setUp(self):
        self.payload = load_fixture("fourmeme_events.json")
        self.batch = parse_fourmeme(
            self.payload,
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )

    def open_database(self, temp):
        connection = connect(Path(temp) / "radar.db")
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
        return connection

    def add_decision(
        self,
        connection,
        index=0,
        *,
        verdict="review",
        delivery="weak",
        gate=False,
        evaluated_at_ms=None,
    ):
        event = self.batch.events[index]
        store_event(connection, event)
        if gate:
            store_provider_observation(
                connection,
                event_id=event.event_id,
                provider="unified_gate",
                observed_at_ms=RECEIVED + 5,
                status="ok",
                payload={"reason_codes": ["sells_below_min"]},
                summary={"reason_codes": ["sells_below_min"]},
            )
        decision = replace(
            replay((event,))[0],
            evaluated_at_ms=RECEIVED + 10 if evaluated_at_ms is None else evaluated_at_ms,
            risk_verdict=verdict,
            delivery=delivery,
            confidence_score=90 if delivery == "strong" else 40,
            opportunity_score=80 if delivery == "strong" else 20,
        )
        store_decision(connection, decision)
        return decision

    def candidates(self, connection, **kwargs):
        options = {
            "since_ms": RECEIVED,
            "until_ms": RECEIVED + 100_000,
        }
        options.update(kwargs)
        return select_enrollment_candidates(connection, **options)

    def test_schema_is_additive_and_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            self.assertFalse(initialize_cohort(connection))
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(outcomes)")
            }
            self.assertIn("market_cap_usd", columns)
            self.assertIn("cohort_version", columns)
            self.assertEqual(
                4,
                connection.execute("PRAGMA user_version").fetchone()[0],
            )
            self.assertEqual(
                "ok",
                connection.execute("PRAGMA quick_check").fetchone()[0],
            )
            connection.close()

    def test_delivered_and_rejected_are_taken_whole(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            self.add_decision(
                connection,
                0,
                verdict="pass",
                delivery="strong",
                gate=True,
            )
            self.add_decision(connection, 1, gate=True)
            strata = sorted(
                item["stratum"] for item in self.candidates(connection)
            )
            connection.close()
        self.assertEqual(["delivered", "evaluated_rejected"], strata)

    def test_control_stratum_is_sampled_not_taken_whole(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            self.add_decision(connection, 0)
            self.add_decision(connection, 1)
            none_admitted = self.candidates(connection, control_sample_rate=0.0)
            all_admitted = self.candidates(
                connection,
                control_sample_rate=1.0,
                control_limit=10,
            )
            capped = self.candidates(
                connection,
                control_sample_rate=1.0,
                control_limit=1,
            )
            connection.close()
        self.assertEqual([], none_admitted)
        self.assertEqual(2, len(all_admitted))
        self.assertEqual(
            ["not_evaluated", "not_evaluated"],
            [item["stratum"] for item in all_admitted],
        )
        self.assertEqual(1, len(capped))

    def test_control_sampling_is_stable_across_calls(self):
        first = sample_fraction("event-a")
        self.assertEqual(first, sample_fraction("event-a"))
        self.assertNotEqual(first, sample_fraction("event-b"))
        self.assertNotEqual(first, sample_fraction("event-a", cohort_version="v2"))
        for value in (first, sample_fraction("event-b")):
            self.assertGreaterEqual(value, 0.0)
            self.assertLess(value, 1.0)

    def test_backfill_events_are_never_enrolled(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            event = replace(self.batch.events[0], is_backfill=True)
            store_event(connection, event)
            store_decision(
                connection,
                replace(
                    replay((event,))[0],
                    evaluated_at_ms=RECEIVED + 10,
                ),
            )
            selected = self.candidates(connection, control_sample_rate=1.0, control_limit=10)
            connection.close()
        self.assertEqual([], selected)

    def test_enrolled_members_are_not_offered_again(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            self.add_decision(
                connection,
                0,
                verdict="pass",
                delivery="strong",
                gate=True,
            )
            candidate = self.candidates(connection)[0]
            self.assertTrue(
                enroll_cohort_member(
                    connection,
                    candidate=candidate,
                    baseline_at_ms=RECEIVED + 20,
                    enrolled_at_ms=RECEIVED + 20,
                    baseline_status="ok",
                    price_usd=0.001,
                    market_cap_usd=50_000.0,
                    liquidity_usd=20_000.0,
                )
            )
            self.assertFalse(
                enroll_cohort_member(
                    connection,
                    candidate=candidate,
                    baseline_at_ms=RECEIVED + 30,
                    enrolled_at_ms=RECEIVED + 30,
                    baseline_status="ok",
                    price_usd=0.002,
                    market_cap_usd=60_000.0,
                    liquidity_usd=20_000.0,
                )
            )
            self.assertEqual([], self.candidates(connection))
            self.assertEqual(
                {"delivered": 1, "evaluated_rejected": 0, "not_evaluated": 0},
                cohort_counts(connection),
            )
            connection.close()

    def test_unavailable_baseline_cannot_carry_a_price(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            self.add_decision(
                connection,
                0,
                verdict="pass",
                delivery="strong",
                gate=True,
            )
            candidate = self.candidates(connection)[0]
            with self.assertRaisesRegex(ValueError, "non-ok baseline"):
                enroll_cohort_member(
                    connection,
                    candidate=candidate,
                    baseline_at_ms=RECEIVED + 20,
                    enrolled_at_ms=RECEIVED + 20,
                    baseline_status="unavailable",
                    price_usd=0.001,
                    market_cap_usd=None,
                    liquidity_usd=None,
                )
            self.assertTrue(
                enroll_cohort_member(
                    connection,
                    candidate=candidate,
                    baseline_at_ms=RECEIVED + 20,
                    enrolled_at_ms=RECEIVED + 20,
                    baseline_status="unavailable",
                    price_usd=None,
                    market_cap_usd=None,
                    liquidity_usd=None,
                )
            )
            row = connection.execute(
                "SELECT baseline_status, baseline_price_usd FROM outcome_cohort"
            ).fetchone()
            connection.close()
        self.assertEqual("unavailable", row["baseline_status"])
        self.assertIsNone(row["baseline_price_usd"])

    def enrolled_connection(self, temp, baseline_at_ms):
        connection = self.open_database(temp)
        self.add_decision(
            connection,
            0,
            verdict="pass",
            delivery="strong",
            gate=True,
        )
        candidate = self.candidates(connection)[0]
        enroll_cohort_member(
            connection,
            candidate=candidate,
            baseline_at_ms=baseline_at_ms,
            enrolled_at_ms=baseline_at_ms,
            baseline_status="ok",
            price_usd=0.001,
            market_cap_usd=50_000.0,
            liquidity_usd=20_000.0,
        )
        return connection, candidate

    def test_horizons_become_due_in_order_and_only_once(self):
        base = RECEIVED + 1_000
        with tempfile.TemporaryDirectory() as temp:
            connection, candidate = self.enrolled_connection(temp, base)
            self.assertEqual([], due_horizons(connection, now_ms=base))
            first = due_horizons(connection, now_ms=base + 60_000)
            self.assertEqual([1], [item["horizon_minutes"] for item in first])
            self.assertFalse(first[0]["expired"])
            store_outcome(
                connection,
                event_id=candidate["event_id"],
                ruleset_version=candidate["ruleset_version"],
                horizon_minutes=1,
                scheduled_at_ms=first[0]["scheduled_at_ms"],
                checked_at_ms=base + 60_500,
                source="dexscreener",
                outcome_label="ok",
                price_usd=0.002,
                market_cap_usd=100_000.0,
                liquidity_usd=25_000.0,
            )
            self.assertEqual([], due_horizons(connection, now_ms=base + 61_000))
            later = due_horizons(connection, now_ms=base + 6 * 60_000)
            connection.close()
        self.assertEqual([5], [item["horizon_minutes"] for item in later])

    def test_overdue_horizon_is_flagged_expired(self):
        base = RECEIVED + 1_000
        with tempfile.TemporaryDirectory() as temp:
            connection, _ = self.enrolled_connection(temp, base)
            late = base + 60_000 + HORIZON_TOLERANCE_MS[1] + 1
            due = due_horizons(connection, now_ms=late)
            connection.close()
        self.assertTrue(due[0]["expired"])
        self.assertEqual(1, due[0]["horizon_minutes"])

    def test_due_list_is_bounded(self):
        base = RECEIVED + 1_000
        with tempfile.TemporaryDirectory() as temp:
            connection, _ = self.enrolled_connection(temp, base)
            due = due_horizons(
                connection,
                now_ms=base + 1441 * 60_000,
                limit=2,
            )
            unbounded = due_horizons(connection, now_ms=base + 1441 * 60_000)
            connection.close()
        self.assertEqual(2, len(due))
        self.assertEqual(len(HORIZON_MINUTES), len(unbounded))

    def test_unavailable_outcome_is_never_stored_as_zero(self):
        base = RECEIVED + 1_000
        with tempfile.TemporaryDirectory() as temp:
            connection, candidate = self.enrolled_connection(temp, base)
            with self.assertRaisesRegex(ValueError, "only an ok outcome"):
                store_outcome(
                    connection,
                    event_id=candidate["event_id"],
                    ruleset_version=candidate["ruleset_version"],
                    horizon_minutes=1,
                    scheduled_at_ms=base + 60_000,
                    checked_at_ms=base + 60_100,
                    source="dexscreener",
                    outcome_label="unavailable",
                    price_usd=0.0,
                )
            self.assertTrue(
                store_outcome(
                    connection,
                    event_id=candidate["event_id"],
                    ruleset_version=candidate["ruleset_version"],
                    horizon_minutes=1,
                    scheduled_at_ms=base + 60_000,
                    checked_at_ms=base + 60_100,
                    source="dexscreener",
                    outcome_label="unavailable",
                    error_code="NO_PAIR",
                )
            )
            row = connection.execute(
                "SELECT price_usd, market_cap_usd, actual_elapsed_ms, error_code,"
                " cohort_version FROM outcomes"
            ).fetchone()
            counts = outcome_counts(connection)
            connection.close()
        self.assertIsNone(row["price_usd"])
        self.assertIsNone(row["market_cap_usd"])
        self.assertEqual(100, row["actual_elapsed_ms"])
        self.assertEqual("NO_PAIR", row["error_code"])
        self.assertEqual(COHORT_VERSION, row["cohort_version"])
        self.assertEqual(1, counts["unavailable"])
        self.assertEqual(0, counts["ok"])

    def test_outcome_rows_require_a_real_event(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            with self.assertRaises(sqlite3.IntegrityError):
                store_outcome(
                    connection,
                    event_id="missing-event",
                    ruleset_version="radar_v8_ratio_any_one",
                    horizon_minutes=1,
                    scheduled_at_ms=RECEIVED,
                    checked_at_ms=RECEIVED + 1,
                    source="dexscreener",
                    outcome_label="error",
                    error_code="NETWORK_ERROR",
                )
            connection.close()

    def test_invalid_label_and_stratum_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            with self.assertRaisesRegex(ValueError, "invalid outcome label"):
                store_outcome(
                    connection,
                    event_id="x",
                    ruleset_version="r",
                    horizon_minutes=1,
                    scheduled_at_ms=0,
                    checked_at_ms=1,
                    source="dexscreener",
                    outcome_label="maybe",
                )
            with self.assertRaisesRegex(ValueError, "invalid cohort stratum"):
                enroll_cohort_member(
                    connection,
                    candidate={"stratum": "guessed"},
                    baseline_at_ms=0,
                    enrolled_at_ms=0,
                    baseline_status="ok",
                    price_usd=None,
                    market_cap_usd=None,
                    liquidity_usd=None,
                )
            with self.assertRaisesRegex(ValueError, "invalid baseline status"):
                enroll_cohort_member(
                    connection,
                    candidate={"stratum": "delivered"},
                    baseline_at_ms=0,
                    enrolled_at_ms=0,
                    baseline_status="guessed",
                    price_usd=None,
                    market_cap_usd=None,
                    liquidity_usd=None,
                )
            with self.assertRaisesRegex(ValueError, "window must be positive"):
                select_enrollment_candidates(
                    connection,
                    since_ms=RECEIVED,
                    until_ms=RECEIVED,
                )
            with self.assertRaisesRegex(ValueError, "invalid control sample rate"):
                select_enrollment_candidates(
                    connection,
                    since_ms=RECEIVED,
                    until_ms=RECEIVED + 1,
                    control_sample_rate=1.5,
                )
            connection.close()


if __name__ == "__main__":
    unittest.main()
