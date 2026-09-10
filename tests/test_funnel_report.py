import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from meme_radar.adapters import parse_fourmeme
from meme_radar.funnel_report import (
    build_report,
    open_readonly,
    render_text,
)
from meme_radar.replay import replay
from meme_radar.storage import (
    connect,
    enqueue_telegram,
    initialize,
    store_decision,
    store_event,
    store_provider_observation,
    store_raw_payload,
)

from support import load_fixture


OBSERVED = 1_788_500_000_000
RECEIVED = OBSERVED + 1_000


class FunnelReportTests(unittest.TestCase):
    def setUp(self):
        self.payload = load_fixture("fourmeme_events.json")
        self.batch = parse_fourmeme(
            self.payload,
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
        )

    def build_database(self, path):
        connection = connect(path)
        initialize(connection)
        store_raw_payload(
            connection,
            source="bitquery",
            batch_id="fixture",
            observed_at_ms=OBSERVED,
            received_at_ms=RECEIVED,
            is_backfill=False,
            payload=self.payload,
        )
        event = self.batch.events[0]
        store_event(connection, event)
        store_provider_observation(
            connection,
            event_id=event.event_id,
            provider="unified_gate",
            observed_at_ms=RECEIVED + 5,
            status="ok",
            payload={"reason_codes": ["market_cap_not_above_min"]},
            summary={"reason_codes": ["market_cap_not_above_min"]},
        )
        decision = replace(
            replay((event,))[0],
            evaluated_at_ms=RECEIVED + 10,
            risk_verdict="review",
            confidence_score=60,
            opportunity_score=30,
            delivery="weak",
            reason_codes=(
                "SECURITY_UNKNOWN",
                "CONFIDENCE_60",
                "OPPORTUNITY_30",
                "DELIVERY_WEAK",
            ),
        )
        store_decision(connection, decision)
        connection.close()
        return event

    def report(self, connection, **kwargs):
        return build_report(
            connection,
            since_ms=OBSERVED - 1,
            until_ms=RECEIVED + 3_600_000,
            **kwargs,
        )

    def test_reports_each_funnel_stage_and_gate_selectivity(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            self.build_database(path)
            connection = open_readonly(path)
            report = self.report(connection)
            connection.close()
        stages = {item["stage"]: item["count"] for item in report["funnel"]}
        self.assertEqual(1, stages["events"])
        self.assertEqual(1, stages["unified_gate_observed"])
        self.assertEqual(0, stages["unified_gate_passed"])
        self.assertEqual(1, stages["decisions"])
        self.assertEqual(0, stages["risk_pass"])
        self.assertEqual(0, stages["delivery_strong"])
        self.assertEqual(0, stages["telegram_claims"])
        gate = report["unified_gate"]["gates"][0]
        self.assertEqual("market_cap_not_above_min", gate["reason"])
        self.assertEqual(1, gate["count"])
        self.assertEqual(1, gate["sole_blocker"])
        self.assertEqual(0.0, report["unified_gate"]["pass_rate"])

    def test_scoring_score_codes_are_not_counted_as_gates(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            self.build_database(path)
            connection = open_readonly(path)
            report = self.report(connection)
            connection.close()
        reasons = [row["reason"] for row in report["decisions"]["risk_reasons"]]
        self.assertEqual(["SECURITY_UNKNOWN"], reasons)
        self.assertEqual(
            1,
            report["decisions"]["risk_reasons"][0]["sole_blocker"],
        )
        self.assertEqual({"weak": 1}, report["decisions"]["by_delivery"])
        self.assertEqual({"review": 1}, report["decisions"]["by_verdict"])

    def test_scope_filters_apply_to_every_section(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            self.build_database(path)
            connection = open_readonly(path)
            matching = self.report(connection, chain="bsc")
            other = self.report(connection, chain="base")
            missing_source = self.report(connection, source="gmgn_trending_v2")
            connection.close()
        self.assertEqual(1, matching["events"]["total"])
        self.assertEqual(0, other["events"]["total"])
        self.assertEqual(0, other["decisions"]["total"])
        self.assertEqual(0, other["unified_gate"]["observations"])
        self.assertEqual(0, missing_source["events"]["total"])

    def test_report_connection_cannot_write(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            self.build_database(path)
            connection = open_readonly(path)
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("DELETE FROM radar_events")
            connection.close()

    def test_missing_database_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(RuntimeError):
                open_readonly(Path(temp) / "absent.db")

    def test_window_must_be_positive(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            self.build_database(path)
            connection = open_readonly(path)
            with self.assertRaises(ValueError):
                build_report(connection, since_ms=RECEIVED, until_ms=RECEIVED)
            connection.close()

    def test_text_rendering_includes_stage_and_gate_lines(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            self.build_database(path)
            connection = open_readonly(path)
            text = render_text(self.report(connection, hourly=True))
            connection.close()
        self.assertIn("unified_gate_passed", text)
        self.assertIn("market_cap_not_above_min", text)
        self.assertIn("hourly events/pass/strong", text)
        self.assertIn("of_events=", text)

    def test_delivered_claims_are_counted(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "radar.db"
            event = self.build_database(path)
            connection = connect(path)
            decision = replace(
                replay((event,))[0],
                evaluated_at_ms=RECEIVED + 20,
                risk_verdict="pass",
                confidence_score=90,
                opportunity_score=80,
                delivery="strong",
            )
            store_decision(connection, decision)
            enqueue_telegram(
                connection,
                decision=decision,
                dedupe_key="radar_v8_ratio_any_one:bsc:token",
                message_text="test",
                now_ms=RECEIVED + 21,
            )
            connection.close()
            connection = open_readonly(path)
            report = self.report(connection)
            connection.close()
        self.assertEqual(1, report["delivery"]["claims"])
        self.assertEqual(
            {"radar_v8_ratio_any_one": 1},
            report["delivery"]["claims_by_ruleset"],
        )
        self.assertEqual({"pending": 1}, report["delivery"]["outbox_by_state"])
        self.assertEqual(1, report["decisions"]["by_delivery"]["strong"])


if __name__ == "__main__":
    unittest.main()
