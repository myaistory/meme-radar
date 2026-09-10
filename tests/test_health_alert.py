import json
import tempfile
import unittest
from pathlib import Path

from meme_radar.health_alert import HealthAlerter, collect_issues


NOW = 1_788_900_000_000


def healthy():
    main = {
        "updated_at_ms": NOW,
        "native_rpc": {
            "mode": "primary",
            "chains": {
                chain: {"connected": True, "slot": "primary"}
                for chain in ("bsc", "base", "robinhood")
            }
        },
        "telegram_outbox": {"pending": 0, "sent": 0, "failed": 0},
        "last_error": {},
    }
    curve = {
        "updated_at_ms": NOW,
        "connected": True,
        "counters": {"wss_logs": 10},
        "last_error": {},
    }
    notifier = {
        "updated_at_ms": NOW,
        "collector_ready": True,
        "outbox": {"pending": 0},
    }
    return main, curve, notifier


def healthy_outcome():
    return {
        "service": "meme-radar-outcome-collector",
        "updated_at_ms": NOW,
        "last_error": {},
        "provider_budget": {
            "used": 10,
            "limit": 240,
            "window_seconds": 3600,
            "cooldown_seconds": 0,
        },
        "outcomes": {"ok": 100, "unavailable": 5, "error": 1, "expired": 2},
        "cohort": {"delivered": 40, "evaluated_rejected": 30, "not_evaluated": 5},
        "last_enrolled_at_ms": NOW - 60_000,
        "last_measured_at_ms": NOW - 20_000,
    }


class HealthAlertTests(unittest.TestCase):
    def test_detects_rpc_failure_and_zombie_wss(self):
        main, curve, notifier = healthy()
        main["native_rpc"]["chains"]["base"]["connected"] = False
        main["last_error"] = {
            "native_rpc_base": {"at_ms": NOW, "status": 429},
            "native_primary_probe_robinhood": {
                "at_ms": NOW,
                "status": 429,
            },
        }
        metrics = {"pons_wss_logs": 10, "pons_wss_logs_changed_at_ms": NOW - 121_000}
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state=metrics,
        )
        self.assertIn("RPC_DISCONNECTED_BASE", issues)
        self.assertNotIn("RPC_FAILOVER_BASE", issues)
        self.assertIn("RPC_ERROR_NATIVE_RPC_BASE_429", issues)
        self.assertNotIn(
            "RPC_ERROR_NATIVE_PRIMARY_PROBE_ROBINHOOD_429",
            issues,
        )
        self.assertIn("PONS_WSS_IDLE", issues)

    def test_disabled_native_rpc_reports_no_chain_issues(self):
        main, curve, notifier = healthy()
        main["native_rpc"] = {"mode": "off", "chains": {}}
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={},
        )
        self.assertEqual(
            [],
            [code for code in issues if code.startswith("RPC_")],
        )

    def test_only_configured_chains_are_checked(self):
        main, curve, notifier = healthy()
        main["native_rpc"]["chains"].pop("bsc")
        main["native_rpc"]["chains"].pop("robinhood")
        main["native_rpc"]["chains"]["base"]["connected"] = False
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={},
        )
        self.assertIn("RPC_DISCONNECTED_BASE", issues)
        self.assertNotIn("RPC_DISCONNECTED_BSC", issues)
        self.assertNotIn("RPC_DISCONNECTED_ROBINHOOD", issues)

    def test_stalled_checkpoint_is_reported_after_the_hold_window(self):
        main, curve, notifier = healthy()
        main["native_rpc"]["chains"]["bsc"]["checkpoint_hold"] = {
            "block": 4242,
            "holds": 3,
            "since_ms": NOW - 6 * 60_000,
        }
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={},
        )
        self.assertIn("RPC_CHECKPOINT_STALLED_BSC", issues)
        self.assertIn("4242", issues["RPC_CHECKPOINT_STALLED_BSC"])

    def test_fresh_checkpoint_hold_is_not_alerted(self):
        main, curve, notifier = healthy()
        main["native_rpc"]["chains"]["bsc"]["checkpoint_hold"] = {
            "block": 4242,
            "holds": 1,
            "since_ms": NOW - 30_000,
        }
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={},
        )
        self.assertNotIn("RPC_CHECKPOINT_STALLED_BSC", issues)

    def test_terminal_outbox_failures_are_reported(self):
        main, curve, notifier = healthy()
        main["telegram_outbox"] = {"pending": 0, "sent": 4, "failed": 2}
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={},
        )
        self.assertIn("MAIN_OUTBOX_FAILED", issues)
        self.assertNotIn("MAIN_OUTBOX_PENDING", issues)

    def test_standby_slot_is_reported_as_degraded(self):
        main, curve, notifier = healthy()
        main["native_rpc"]["chains"]["base"] = {
            "connected": True,
            "slot": "standby",
        }
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={},
        )
        self.assertIn("RPC_FAILOVER_BASE", issues)
        self.assertIn(
            "备用RPC仍在正常采集和推送",
            issues["RPC_FAILOVER_BASE"],
        )

    def test_gmgn_discovery_staleness_is_reported(self):
        main, curve, notifier = healthy()
        main["gmgn_discovery_enabled"] = True
        main["started_at_ms"] = NOW - 600_000
        main["last_success"] = {
            "gmgn_discovery_bsc": NOW,
            "gmgn_discovery_base": NOW - 301_000,
            "gmgn_discovery_solana": NOW,
        }
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={},
        )
        self.assertIn("GMGN_DISCOVERY_STALE_BASE", issues)
        self.assertNotIn("GMGN_DISCOVERY_STALE_BSC", issues)

    def test_wss_counter_progress_clears_idle(self):
        main, curve, notifier = healthy()
        metrics = {"pons_wss_logs": 9, "pons_wss_logs_changed_at_ms": NOW - 500_000}
        issues = collect_issues(
            now_ms=NOW,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state=metrics,
        )
        self.assertNotIn("PONS_WSS_IDLE", issues)
        self.assertEqual(NOW, metrics["pons_wss_logs_changed_at_ms"])

    def test_alert_and_recovery_are_deduplicated(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = root / "telegram.env"
            env.write_text("", encoding="utf-8")
            env.chmod(0o600)
            paths = {
                name: root / (name + ".json")
                for name in ("main", "curve", "notifier")
            }
            main, curve, notifier = healthy()
            for name, payload in zip(paths, (main, curve, notifier)):
                paths[name].write_text(json.dumps(payload), encoding="utf-8")
            alerter = HealthAlerter(
                telegram_env=env,
                main_health=paths["main"],
                curve_health=paths["curve"],
                notifier_health=paths["notifier"],
                state_path=root / "state.json",
                health_path=root / "health.json",
                poll_seconds=15,
                alert_after_seconds=60,
                cooldown_seconds=300,
                dry_run=True,
            )
            main["native_rpc"]["chains"]["bsc"]["connected"] = False
            paths["main"].write_text(json.dumps(main), encoding="utf-8")
            alerter.poll_once(NOW)
            alerter.poll_once(NOW + 61_000)
            alerter.poll_once(NOW + 62_000)
            self.assertEqual(1, alerter.counters["alerts"])
            main["native_rpc"]["chains"]["bsc"]["connected"] = True
            main["updated_at_ms"] = NOW + 63_000
            paths["main"].write_text(json.dumps(main), encoding="utf-8")
            alerter.poll_once(NOW + 63_000)
            self.assertNotIn("recoveries", alerter.counters)
            main["updated_at_ms"] = NOW + 184_000
            curve["updated_at_ms"] = NOW + 184_000
            notifier["updated_at_ms"] = NOW + 184_000
            for name, payload in zip(paths, (main, curve, notifier)):
                paths[name].write_text(json.dumps(payload), encoding="utf-8")
            alerter.poll_once(NOW + 184_000)
            self.assertEqual(1, alerter.counters["recoveries"])

    def test_flapping_issue_does_not_emit_early_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = root / "telegram.env"
            env.write_text("", encoding="utf-8")
            env.chmod(0o600)
            paths = {
                name: root / (name + ".json")
                for name in ("main", "curve", "notifier")
            }
            main, curve, notifier = healthy()
            main["native_rpc"]["chains"]["bsc"]["connected"] = False
            for name, payload in zip(paths, (main, curve, notifier)):
                paths[name].write_text(json.dumps(payload), encoding="utf-8")
            alerter = HealthAlerter(
                telegram_env=env,
                main_health=paths["main"],
                curve_health=paths["curve"],
                notifier_health=paths["notifier"],
                state_path=root / "state.json",
                health_path=root / "health.json",
                poll_seconds=15,
                alert_after_seconds=60,
                cooldown_seconds=300,
                recovery_after_seconds=120,
                dry_run=True,
            )
            alerter.poll_once(NOW)
            alerter.poll_once(NOW + 61_000)
            main["native_rpc"]["chains"]["bsc"]["connected"] = True
            main["updated_at_ms"] = NOW + 62_000
            paths["main"].write_text(json.dumps(main), encoding="utf-8")
            alerter.poll_once(NOW + 62_000)
            main["native_rpc"]["chains"]["bsc"]["connected"] = False
            main["updated_at_ms"] = NOW + 100_000
            paths["main"].write_text(json.dumps(main), encoding="utf-8")
            alerter.poll_once(NOW + 100_000)
            self.assertEqual(1, alerter.counters["alerts"])
            self.assertNotIn("recoveries", alerter.counters)


class OutcomeCollectorAlertTests(unittest.TestCase):
    def issues(self, outcome, *, now_ms=NOW, expected=True):
        main, curve, notifier = healthy()
        return collect_issues(
            now_ms=now_ms,
            main=main,
            curve=curve,
            notifier=notifier,
            metric_state={"pons_wss_logs": 10, "pons_wss_logs_changed_at_ms": NOW},
            outcome=outcome,
            outcome_expected=expected,
        )

    def test_healthy_collector_raises_nothing(self):
        self.assertEqual({}, self.issues(healthy_outcome()))

    def test_unconfigured_collector_is_not_an_issue(self):
        self.assertEqual({}, self.issues(None, expected=False))

    def test_missing_health_file_is_reported_when_configured(self):
        self.assertIn("HEALTH_MISSING_OUTCOME", self.issues(None))

    def test_stale_health_suppresses_the_derived_checks(self):
        outcome = healthy_outcome()
        outcome["updated_at_ms"] = NOW - 11 * 60_000
        outcome["last_error"] = {"measure": {"at_ms": NOW, "code": "HTTP_500"}}
        issues = self.issues(outcome)
        self.assertIn("HEALTH_STALE_OUTCOME", issues)
        self.assertNotIn("OUTCOME_ERROR_MEASURE", issues)

    def test_slow_cycle_below_ten_minutes_is_tolerated(self):
        outcome = healthy_outcome()
        outcome["updated_at_ms"] = NOW - 5 * 60_000
        self.assertNotIn("HEALTH_STALE_OUTCOME", self.issues(outcome))

    def test_exhausted_budget_and_cooldown_are_reported(self):
        outcome = healthy_outcome()
        outcome["provider_budget"]["used"] = 240
        outcome["provider_budget"]["cooldown_seconds"] = 120
        issues = self.issues(outcome)
        self.assertIn("OUTCOME_BUDGET_EXHAUSTED", issues)
        self.assertIn("OUTCOME_PROVIDER_COOLDOWN", issues)

    def test_a_few_missed_windows_do_not_alert(self):
        outcome = healthy_outcome()
        outcome["outcomes"] = {"ok": 60, "unavailable": 0, "error": 0, "expired": 19}
        self.assertNotIn("OUTCOME_WINDOWS_MISSED", self.issues(outcome))

    def test_widespread_missed_windows_alert(self):
        outcome = healthy_outcome()
        outcome["outcomes"] = {"ok": 60, "unavailable": 0, "error": 0, "expired": 40}
        self.assertIn("OUTCOME_WINDOWS_MISSED", self.issues(outcome))

    def test_long_enrollment_idle_is_reported(self):
        outcome = healthy_outcome()
        outcome["last_enrolled_at_ms"] = NOW - 7 * 3_600_000
        self.assertIn("OUTCOME_ENROLLMENT_IDLE", self.issues(outcome))

    def test_never_enrolled_does_not_alert_on_a_fresh_start(self):
        outcome = healthy_outcome()
        outcome["last_enrolled_at_ms"] = 0
        self.assertNotIn("OUTCOME_ENROLLMENT_IDLE", self.issues(outcome))

    def test_recent_stage_error_is_reported(self):
        outcome = healthy_outcome()
        outcome["last_error"] = {"measure": {"at_ms": NOW, "code": "HTTP_500"}}
        self.assertIn("OUTCOME_ERROR_MEASURE", self.issues(outcome))

    def test_stale_stage_error_is_not_reported(self):
        outcome = healthy_outcome()
        outcome["last_error"] = {
            "measure": {"at_ms": NOW - 10 * 60_000, "code": "HTTP_500"}
        }
        self.assertNotIn("OUTCOME_ERROR_MEASURE", self.issues(outcome))


if __name__ == "__main__":
    unittest.main()
