import json
import tempfile
import unittest
from pathlib import Path

from meme_radar.runtime import SlidingBudget, RuntimeSettings, write_health


class Clock:
    def __init__(self):
        self.value = 100.0

    def __call__(self):
        return self.value


class RuntimeTests(unittest.TestCase):
    def test_budget_limit_window_and_cooldown(self):
        clock = Clock()
        budget = SlidingBudget(2, window_seconds=10, now=clock)
        self.assertTrue(budget.take())
        self.assertTrue(budget.take())
        self.assertFalse(budget.take())
        clock.value += 11
        self.assertTrue(budget.take())
        budget.cooldown(5)
        self.assertFalse(budget.take())
        clock.value += 6
        self.assertTrue(budget.take())

    def test_atomic_health_is_mode_0600(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "health.json"
            write_health(path, {"ok": True})
            self.assertEqual({"ok": True}, json.loads(path.read_text()))
            self.assertEqual(0o600, path.stat().st_mode & 0o777)

    def test_runtime_settings_are_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            env = Path(temp) / ".env"
            env.write_text(
                "MEME_RADAR_DB_PATH=/tmp/radar.db\n"
                "MEME_RADAR_HEALTH_PATH=/tmp/health.json\n"
                "MEME_RADAR_FOMO_POLL_SECONDS=300\n"
                "MEME_RADAR_FOMO_ENABLED=0\n"
                "MEME_RADAR_CLANKER_POLL_SECONDS=60\n"
                "MEME_RADAR_ENRICH_MAX_PER_HOUR=30\n"
                "MEME_RADAR_BACKFILL_MAX_MINUTES=10\n"
                "MEME_RADAR_BITQUERY_ENABLED=1\n"
                "MEME_RADAR_SAMPLE_ENABLED=0\n"
                "MEME_RADAR_SAMPLE_MAX_PER_HOUR=3\n"
                "MEME_RADAR_NATIVE_RPC_MODE=shadow\n"
                "MEME_RADAR_NATIVE_RPC_BACKFILL_BLOCKS=500\n"
                "MEME_RADAR_NATIVE_RPC_PRIMARY_PROBE_SECONDS=60\n"
                "MEME_RADAR_NATIVE_RPC_PRIMARY_RECOVERY_SUCCESSES=3\n"
                "MEME_RADAR_GMGN_ENABLED=1\n"
                "MEME_RADAR_GMGN_MAX_PER_HOUR=30\n"
                "MEME_RADAR_GMGN_CACHE_SECONDS=60\n",
                encoding="utf-8",
            )
            env.chmod(0o600)
            settings = RuntimeSettings.from_env_file(env)
            self.assertEqual(300, settings.fomo_poll_seconds)
            self.assertFalse(settings.fomo_enabled)
            self.assertEqual(10, settings.backfill_max_minutes)
            self.assertTrue(settings.bitquery_enabled)
            self.assertFalse(settings.sample_enabled)
            self.assertEqual(3, settings.sample_max_per_hour)
            self.assertEqual("shadow", settings.native_rpc_mode)
            self.assertEqual(500, settings.native_rpc_backfill_blocks)
            self.assertEqual(60, settings.native_rpc_primary_probe_seconds)
            self.assertEqual(3, settings.native_rpc_primary_recovery_successes)
            self.assertTrue(settings.gmgn_enabled)
            self.assertEqual(30, settings.gmgn_max_per_hour)
            self.assertEqual(60, settings.gmgn_cache_seconds)
            self.assertFalse(settings.gmgn_discovery_enabled)

    def test_gmgn_discovery_requires_base_gmgn_and_is_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            env = Path(temp) / ".env"
            env.write_text(
                "MEME_RADAR_GMGN_ENABLED=1\n"
                "MEME_RADAR_GMGN_DISCOVERY_ENABLED=1\n"
                "MEME_RADAR_GMGN_DISCOVERY_POLL_SECONDS=120\n"
                "MEME_RADAR_GMGN_DISCOVERY_MAX_PER_HOUR=90\n",
                encoding="utf-8",
            )
            env.chmod(0o600)
            settings = RuntimeSettings.from_env_file(env)
            self.assertTrue(settings.gmgn_discovery_enabled)
            self.assertEqual(120, settings.gmgn_discovery_poll_seconds)
            self.assertEqual(90, settings.gmgn_discovery_max_per_hour)

    def test_bitquery_disable_requires_native_primary(self):
        with tempfile.TemporaryDirectory() as temp:
            env = Path(temp) / ".env"
            env.write_text(
                "MEME_RADAR_BITQUERY_ENABLED=0\n"
                "MEME_RADAR_NATIVE_RPC_MODE=shadow\n",
                encoding="utf-8",
            )
            env.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "native RPC primary"):
                RuntimeSettings.from_env_file(env)

    def test_sample_alerts_can_stage_without_telegram_sender(self):
        with tempfile.TemporaryDirectory() as temp:
            env = Path(temp) / ".env"
            env.write_text(
                "MEME_RADAR_SAMPLE_ENABLED=1\n"
                "MEME_RADAR_TELEGRAM_ENABLED=0\n",
                encoding="utf-8",
            )
            env.chmod(0o600)
            settings = RuntimeSettings.from_env_file(env)
            self.assertTrue(settings.sample_enabled)
            self.assertFalse(settings.telegram_enabled)


if __name__ == "__main__":
    unittest.main()
