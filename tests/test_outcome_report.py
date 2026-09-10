import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from meme_radar.outcome_cohort import (
    COHORT_VERSION,
    enroll_cohort_member,
    initialize_cohort,
    store_outcome,
)
from meme_radar.outcome_report import (
    OUTCOME_REPORT_VERSION,
    build_report,
    render_text,
)
from meme_radar.storage import connect, initialize


BASE_MS = 1_788_800_000_000
RULESET = "radar_v8_ratio_any_one"


class OutcomeReportTests(unittest.TestCase):
    def open_database(self, temp):
        connection = connect(Path(temp) / "radar.db")
        initialize(connection)
        initialize_cohort(connection)
        with connection:
            connection.execute(
                "INSERT INTO raw_payloads (raw_sha256, payload_json) VALUES (?, ?)",
                ("sha-report", json.dumps({"fixture": True})),
            )
        return connection

    def add_event(self, connection, index, *, chain="solana", source="gmgn_trending_v2"):
        event_id = "event-%04d" % index
        with connection:
            connection.execute(
                """
                INSERT INTO radar_events
                  (event_id, source, source_event_id, event_kind, chain, chain_id,
                   token_address, source_published_at_ms, observed_at_ms,
                   received_at_ms, token_created_at_ms, is_backfill, raw_sha256,
                   event_json)
                VALUES (?, ?, ?, 'token_launch', ?, 101, ?, ?, ?, ?, ?, 0,
                        'sha-report', '{}')
                """,
                (
                    event_id,
                    source,
                    event_id,
                    chain,
                    "0xtoken%04d" % index,
                    BASE_MS,
                    BASE_MS,
                    BASE_MS,
                    BASE_MS,
                ),
            )
        return event_id

    def enroll(
        self,
        connection,
        index,
        *,
        stratum="delivered",
        baseline_status="ok",
        price=1.0,
        baseline_at_ms=None,
        lag_ms=2_000,
        chain="solana",
        source="gmgn_trending_v2",
        ruleset=RULESET,
    ):
        event_id = self.add_event(connection, index, chain=chain, source=source)
        baseline = BASE_MS + 1_000 if baseline_at_ms is None else baseline_at_ms
        candidate = {
            "event_id": event_id,
            "ruleset_version": ruleset,
            "stratum": stratum,
            "chain": chain,
            "source": source,
            "token_address": "0xtoken%04d" % index,
            "decision_evaluated_at_ms": baseline - lag_ms,
            "risk_verdict": "pass" if stratum == "delivered" else "review",
            "delivery": "strong" if stratum == "delivered" else "weak",
            "confidence_score": 90,
            "opportunity_score": 80,
        }
        enroll_cohort_member(
            connection,
            candidate=candidate,
            baseline_at_ms=baseline,
            enrolled_at_ms=baseline,
            baseline_status=baseline_status,
            price_usd=price if baseline_status == "ok" else None,
            market_cap_usd=None,
            liquidity_usd=None,
        )
        return event_id

    def measure(
        self,
        connection,
        event_id,
        *,
        horizon=5,
        label="ok",
        price=2.0,
        ruleset=RULESET,
        error_code=None,
    ):
        scheduled = BASE_MS + 1_000 + horizon * 60_000
        store_outcome(
            connection,
            event_id=event_id,
            ruleset_version=ruleset,
            horizon_minutes=horizon,
            scheduled_at_ms=scheduled,
            checked_at_ms=scheduled + 500,
            source="dexscreener",
            outcome_label=label,
            price_usd=price if label == "ok" else None,
            error_code=error_code,
        )

    def report(self, connection, **kwargs):
        options = {
            "since_ms": BASE_MS,
            "until_ms": BASE_MS + 3_600_000,
        }
        options.update(kwargs)
        return build_report(connection, **options)

    def test_returns_are_ratios_against_the_frozen_baseline(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            first = self.enroll(connection, 1, price=1.0)
            second = self.enroll(connection, 2, price=2.0)
            self.measure(connection, first, price=3.0)
            self.measure(connection, second, price=3.0)
            report = self.report(connection)
            rows = [row for row in report["horizons"] if row["horizon_minutes"] == 5]
            self.assertEqual(1, len(rows))
            row = rows[0]
            self.assertEqual(2, row["computable"])
            # 3.0/1.0 = 3.0 and 3.0/2.0 = 1.5 -> median 2.25
            self.assertAlmostEqual(2.25, row["median_return"], places=4)
            self.assertEqual(1.0, row["share_up"])
            self.assertEqual(1.0, row["coverage"])
            connection.close()

    def test_unavailable_measurement_is_not_a_zero_return(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            priced = self.enroll(connection, 1, price=1.0)
            missing = self.enroll(connection, 2, price=1.0)
            self.measure(connection, priced, price=2.0)
            self.measure(
                connection,
                missing,
                label="unavailable",
                error_code="NO_PRICE",
            )
            row = [
                item
                for item in self.report(connection)["horizons"]
                if item["horizon_minutes"] == 5
            ][0]
            self.assertEqual(2, row["measured"])
            self.assertEqual(1, row["computable"])
            self.assertEqual(2.0, row["median_return"])
            self.assertEqual(0.5, row["coverage"])
            self.assertEqual(0.0, row["share_halved"])
            connection.close()

    def test_missing_baseline_is_counted_separately_not_as_flat(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            blind = self.enroll(
                connection,
                1,
                baseline_status="unavailable",
                price=None,
            )
            self.measure(connection, blind, price=5.0)
            row = [
                item
                for item in self.report(connection)["horizons"]
                if item["horizon_minutes"] == 5
            ][0]
            self.assertEqual(1, row["measured"])
            self.assertEqual(1, row["baseline_missing"])
            self.assertEqual(0, row["computable"])
            self.assertIsNone(row["median_return"])
            connection.close()

    def test_delivered_rows_always_carry_the_survivorship_warning(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            event_id = self.enroll(connection, 1)
            self.measure(connection, event_id)
            report = self.report(connection)
            row = report["horizons"][0]
            self.assertIn("SURVIVORSHIP", row["annotations"])
            self.assertIn("SURVIVORSHIP", report["annotations"])
            self.assertIn("CONTROL_IS_SAMPLED", report["annotations"])
            connection.close()

    def test_control_rows_are_marked_as_sampled(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            event_id = self.enroll(connection, 1, stratum="not_evaluated")
            self.measure(connection, event_id)
            row = self.report(connection)["horizons"][0]
            self.assertEqual("not_evaluated", row["stratum"])
            self.assertIn("CONTROL_IS_SAMPLED", row["annotations"])
            self.assertNotIn("SURVIVORSHIP", row["annotations"])
            connection.close()

    def test_small_samples_are_flagged(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            event_id = self.enroll(connection, 1)
            self.measure(connection, event_id)
            row = self.report(connection, min_samples=5)["horizons"][0]
            self.assertIn("SAMPLE_TOO_SMALL", row["annotations"])
            plenty = self.report(connection, min_samples=1)["horizons"][0]
            self.assertNotIn("SAMPLE_TOO_SMALL", plenty["annotations"])
            connection.close()

    def test_low_coverage_is_flagged(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            good = self.enroll(connection, 1)
            for index in (2, 3):
                missed = self.enroll(connection, index)
                self.measure(
                    connection,
                    missed,
                    label="expired",
                    error_code="HORIZON_WINDOW_MISSED",
                )
            self.measure(connection, good)
            row = self.report(connection)["horizons"][0]
            self.assertEqual(3, row["measured"])
            self.assertEqual(2, row["labels"]["expired"])
            self.assertIn("COVERAGE_LOW", row["annotations"])
            connection.close()

    def test_strata_are_reported_separately(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            delivered = self.enroll(connection, 1, stratum="delivered", price=1.0)
            rejected = self.enroll(
                connection,
                2,
                stratum="evaluated_rejected",
                price=1.0,
            )
            self.measure(connection, delivered, price=4.0)
            self.measure(connection, rejected, price=0.5)
            report = self.report(connection)
            by_stratum = {row["stratum"]: row for row in report["horizons"]}
            self.assertEqual(4.0, by_stratum["delivered"]["median_return"])
            self.assertEqual(0.5, by_stratum["evaluated_rejected"]["median_return"])
            self.assertEqual(1, report["cohort"]["by_stratum"]["delivered"])
            self.assertEqual(
                1,
                report["cohort"]["by_stratum"]["evaluated_rejected"],
            )
            self.assertEqual(0, report["cohort"]["by_stratum"]["not_evaluated"])
            connection.close()

    def test_ruleset_filter_isolates_one_version(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            current = self.enroll(connection, 1, price=1.0)
            legacy = self.enroll(connection, 2, price=1.0, ruleset="radar_v7")
            self.measure(connection, current, price=2.0)
            self.measure(connection, legacy, price=9.0, ruleset="radar_v7")
            report = self.report(connection, ruleset="radar_v7")
            self.assertEqual(1, len(report["horizons"]))
            self.assertEqual("radar_v7", report["horizons"][0]["ruleset_version"])
            self.assertEqual(9.0, report["horizons"][0]["median_return"])
            connection.close()

    def test_completeness_requires_every_horizon(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            full = self.enroll(connection, 1, price=1.0)
            partial = self.enroll(connection, 2, price=1.0)
            for horizon in (1, 5, 15, 60, 360, 1440):
                self.measure(connection, full, horizon=horizon, price=2.0)
            self.measure(connection, partial, horizon=1, price=2.0)
            completeness = self.report(connection)["completeness"]
            self.assertEqual(2, completeness["cohort_members"])
            self.assertEqual(1, completeness["gap_free_members"])
            self.assertEqual(0.5, completeness["gap_free_share"])
            connection.close()

    def test_enrollment_lag_is_visible(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            self.enroll(connection, 1, lag_ms=4_000)
            self.enroll(connection, 2, lag_ms=6_000)
            cohort = self.report(connection)["cohort"]
            self.assertEqual(5_000, cohort["mean_enrollment_lag_ms"])
            connection.close()

    def test_window_and_min_samples_are_validated(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            with self.assertRaises(ValueError):
                build_report(connection, since_ms=BASE_MS, until_ms=BASE_MS)
            with self.assertRaises(ValueError):
                build_report(
                    connection,
                    since_ms=BASE_MS,
                    until_ms=BASE_MS + 1,
                    min_samples=0,
                )
            connection.close()

    def test_text_rendering_lists_strata_and_annotations(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            event_id = self.enroll(connection, 1)
            self.measure(connection, event_id)
            text = render_text(self.report(connection))
            self.assertIn(OUTCOME_REPORT_VERSION, text)
            self.assertIn("cohort by stratum", text)
            self.assertIn("completeness:", text)
            self.assertIn("SURVIVORSHIP", text)
            self.assertIn("CONTROL_IS_SAMPLED", text)
            connection.close()

    def test_out_of_window_members_are_excluded(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = self.open_database(temp)
            inside = self.enroll(connection, 1, price=1.0)
            outside = self.enroll(
                connection,
                2,
                price=1.0,
                baseline_at_ms=BASE_MS + 7_200_000,
            )
            self.measure(connection, inside, price=2.0)
            self.measure(connection, outside, price=8.0)
            report = self.report(connection)
            self.assertEqual(1, report["cohort"]["members"])
            self.assertEqual(1, len(report["horizons"]))
            self.assertEqual(2.0, report["horizons"][0]["median_return"])
            connection.close()


if __name__ == "__main__":
    unittest.main()
