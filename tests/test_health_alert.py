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
            "chains": {
                chain: {"connected": True, "slot": "primary"}
                for chain in ("bsc", "base", "robinhood")
            }
        },
        "telegram_outbox": {"pending": 0},
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


if __name__ == "__main__":
    unittest.main()
