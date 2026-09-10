import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from meme_radar.sol_early_notifier import CURSOR_KEY, SolEarlyNotifier
from meme_radar.sol_early_storage import (
    claim_due,
    claim_candidate,
    claim_notification,
    connect,
    counts,
    get_state,
    initialize,
    mark_unknown,
    provider_budget_state,
    reserve_provider_calls,
    schedule_candidate,
)
from meme_radar.sources.dexscreener import MarketSnapshot
from meme_radar.sources.gmgn_cli import (
    GmgnSafetySnapshot,
    GmgnSolanaSecuritySnapshot,
    GmgnTokenInfoSnapshot,
)


TOKEN = "So11111111111111111111111111111111111111112"
TOKEN_TWO = "11111111111111111111111111111111"


class Info:
    def has_fresh_cache(self, chain, token):
        return False

    def token_info(self, chain, token):
        now = int(time.time() * 1000)
        return GmgnTokenInfoSnapshot(
            chain=chain,
            token_address=token,
            observed_at_ms=now,
            holder_count=80,
            market_cap_usd=45_000,
            price_usd=0.0045,
            liquidity_usd=18_000,
            bytes_read=100,
            elapsed_ms=10,
            total_fee=2.5,
            dev_sold_all=None,
        )


class Trenches:
    def has_fresh_cache(self, chain):
        return False

    def token_safety(self, chain, token):
        now = int(time.time() * 1000)
        return GmgnSafetySnapshot(
            chain=chain,
            token_address=token,
            observed_at_ms=now,
            market_cap_usd=45_000,
            total_fee=2.5,
            creator_token_status="creator_hold",
            creator_balance_rate=0.0,
            is_honeypot=False,
            is_open_source=True,
            owner_renounced=True,
            is_wash_trading=False,
            rug_ratio=0.05,
            top_10_holder_rate=0.22,
            bundler_rate=0.05,
            insider_rate=0.05,
            entrapment_ratio=0.05,
            bytes_read=100,
            elapsed_ms=10,
            liquidity_usd=18_000,
            dev_team_hold_rate=0.05,
            renounced_mint=True,
            renounced_freeze=True,
        )


class Security:
    def token_security(self, token):
        return GmgnSolanaSecuritySnapshot(
            chain="solana",
            token_address=token,
            observed_at_ms=int(time.time() * 1000),
            is_honeypot=False,
            is_blacklisted=False,
            risk_level="normal",
            top_10_holder_rate=0.22,
            renounced_mint=True,
            renounced_freeze=True,
            dev_sold_all=None,
            rug_ratio=0.05,
            bundler_rate=0.05,
            insider_rate=0.05,
            entrapment_ratio=0.05,
            dev_team_hold_rate=0.05,
            bytes_read=100,
            elapsed_ms=10,
        )


class Dex:
    def token_market(self, chain, token):
        return MarketSnapshot(
            chain=chain,
            token_address=token,
            observed_at_ms=int(time.time() * 1000),
            pair_count=1,
            best_pair_address="pair",
            pair_created_at_ms=None,
            price_usd=0.0045,
            liquidity_usd=18_000,
            volume_5m_usd=10_000,
            volume_1h_usd=None,
            volume_24h_usd=None,
            buy_transactions_5m=20,
            sell_transactions_5m=5,
            buy_transactions_1h=None,
            sell_transactions_1h=None,
            buy_transactions_24h=None,
            sell_transactions_24h=None,
            bytes_read=100,
            elapsed_ms=10,
            market_cap_usd=45_000,
        )


class SolEarlyNotifierTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.raw = self.root / "raw.jsonl"
        self.raw.write_text("", encoding="utf-8")
        self.env = self.root / "telegram.env"
        self.env.write_text("", encoding="utf-8")
        self.env.chmod(0o600)
        self.policy = Path(__file__).resolve().parents[1] / "config" / "sol_early_v1.json"
        self.previous = os.environ.get("MEME_RADAR_SOL_EARLY_TELEGRAM_ENABLED")
        os.environ["MEME_RADAR_SOL_EARLY_TELEGRAM_ENABLED"] = "0"

    def tearDown(self):
        if self.previous is None:
            os.environ.pop("MEME_RADAR_SOL_EARLY_TELEGRAM_ENABLED", None)
        else:
            os.environ["MEME_RADAR_SOL_EARLY_TELEGRAM_ENABLED"] = self.previous
        self.temp.cleanup()

    def notifier(self):
        value = SolEarlyNotifier(
            raw_path=self.raw,
            db_path=self.root / "notifier.db",
            health_path=self.root / "health.json",
            policy_path=self.policy,
            telegram_env=self.env,
            dry_run=True,
        )
        value.info = Info()
        value.trenches = Trenches()
        value.security = Security()
        value.dex = Dex()
        return value

    @staticmethod
    def row(card, message):
        now = int(time.time() * 1000)
        return {
            "schema_version": "gmgnsignals_fast_event_v0",
            "mode": "live",
            "is_backfill": False,
            "is_edit": False,
            "message_id": message,
            "posted_at_ms": now - 1000,
            "fast_seen_at_ms": now,
            "structured_fields": {
                "open": "30min ago",
                "top10_percent": "22%",
            },
            "parsed": {
                "token_ca": TOKEN,
                "card_type": card,
                "market_cap": 45_000,
                "liquidity": 18_000,
                "top10_percent": 22.0,
                "total_fee_sol": 2.5,
            },
        }

    def test_two_types_reach_dry_run_outbox_without_volume_cap(self):
        notifier = self.notifier()
        now = int(time.time() * 1000)
        asyncio.run(notifier.process_row(self.row("KOTH", "1"), now))
        self.assertEqual(
            {"pending": 0, "sending": 0, "sent": 0, "unknown": 0},
            counts(notifier.connection),
        )
        asyncio.run(notifier.process_row(self.row("KOL Buy", "2"), now))
        asyncio.run(notifier.process_candidate(int(time.time() * 1000)))
        self.assertEqual(
            {"pending": 1, "sending": 0, "sent": 0, "unknown": 0},
            counts(notifier.connection),
        )
        asyncio.run(notifier.deliver(int(time.time() * 1000)))
        self.assertEqual(
            {"pending": 0, "sending": 0, "sent": 1, "unknown": 0},
            counts(notifier.connection),
        )

    def test_backfill_edit_and_single_card_do_not_push(self):
        notifier = self.notifier()
        now = int(time.time() * 1000)
        for key in ("is_backfill", "is_edit"):
            row = self.row("KOTH", key)
            row[key] = True
            asyncio.run(notifier.process_row(row, now))
        asyncio.run(notifier.process_row(self.row("KOTH", "single"), now))
        self.assertEqual(
            {"pending": 0, "sending": 0, "sent": 0, "unknown": 0},
            counts(notifier.connection),
        )

    def test_cursor_advances_only_after_explicit_commit(self):
        notifier = self.notifier()
        now = int(time.time() * 1000)
        self.assertTrue(notifier.initialize_cursor(now))
        self.raw.write_text(json.dumps(self.row("KOTH", "cursor")) + "\n", encoding="utf-8")
        items = notifier.read_new(now)
        self.assertEqual(1, len(items))
        self.assertEqual(0, get_state(notifier.connection, CURSOR_KEY)["offset"])
        _row, offset, device, inode = items[0]
        notifier.commit_cursor(offset, device, inode, now)
        self.assertEqual(offset, get_state(notifier.connection, CURSOR_KEY)["offset"])

    def test_top10_percentage_points_are_normalized_to_ratio(self):
        notifier = self.notifier()
        row = self.row("KOTH", "top10-unit")
        row["structured_fields"]["top10_percent"] = "27.11%"
        row["parsed"]["top10_percent"] = 27.11
        signal = notifier.compact_signal(row, int(time.time() * 1000))
        self.assertAlmostEqual(0.2711, signal["top10_percent"])

    def test_schema_v1_top10_values_migrate_once(self):
        connection = connect(self.root / "migration.db")
        initialize(connection)
        now = int(time.time() * 1000)
        connection.execute("PRAGMA user_version=1")
        connection.execute(
            """
            INSERT INTO sol_early_signals
              (signal_id,token_address,seen_at_ms,card_type,top10_percent)
            VALUES (?,?,?,?,?)
            """,
            ("legacy", TOKEN, now, "KOTH", 27.11),
        )
        connection.commit()
        initialize(connection)
        value = connection.execute(
            "SELECT top10_percent FROM sol_early_signals WHERE signal_id='legacy'"
        ).fetchone()[0]
        self.assertAlmostEqual(0.2711, value)
        self.assertEqual(2, connection.execute("PRAGMA user_version").fetchone()[0])
        initialize(connection)
        value = connection.execute(
            "SELECT top10_percent FROM sol_early_signals WHERE signal_id='legacy'"
        ).fetchone()[0]
        self.assertAlmostEqual(0.2711, value)

    def test_storage_deduplicates_without_hourly_limit(self):
        connection = connect(self.root / "storage.db")
        initialize(connection)
        now = int(time.time() * 1000)
        self.assertEqual(
            "enqueued",
            claim_notification(connection, token_address=TOKEN, message_text="one", now_ms=now, valid_until_ms=now + 60_000),
        )
        self.assertEqual(
            "enqueued",
            claim_notification(connection, token_address=TOKEN_TWO, message_text="two", now_ms=now, valid_until_ms=now + 60_000),
        )
        self.assertEqual(
            "deduplicated",
            claim_notification(connection, token_address=TOKEN, message_text="again", now_ms=now, valid_until_ms=now + 60_000),
        )

    def test_unknown_delivery_is_not_retried(self):
        connection = connect(self.root / "unknown.db")
        initialize(connection)
        now = int(time.time() * 1000)
        claim_notification(connection, token_address=TOKEN, message_text="one", now_ms=now, valid_until_ms=now + 60_000)
        item = claim_due(connection, now_ms=now)
        self.assertIsNotNone(item)
        mark_unknown(connection, TOKEN, error_code="TIMEOUT")
        self.assertIsNone(claim_due(connection, now_ms=now + 60_000))
        self.assertEqual(1, counts(connection)["unknown"])

    def test_provider_budget_survives_connection_reopen(self):
        path = self.root / "budget.db"
        connection = connect(path)
        initialize(connection)
        now = int(time.time() * 1000)
        self.assertTrue(
            reserve_provider_calls(connection, now_ms=now, cost=3, limit_per_hour=3)
        )
        connection.close()
        connection = connect(path)
        self.assertFalse(
            reserve_provider_calls(connection, now_ms=now + 1, cost=3, limit_per_hour=3)
        )
        self.assertEqual(3, provider_budget_state(connection)["used"])

    def test_provider_budget_deferral_does_not_consume_candidate_attempt(self):
        notifier = self.notifier()
        now = int(time.time() * 1000)
        self.assertTrue(
            reserve_provider_calls(
                notifier.connection,
                now_ms=now,
                cost=24,
                limit_per_hour=24,
            )
        )
        asyncio.run(notifier.process_row(self.row("KOTH", "budget-1"), now))
        asyncio.run(notifier.process_row(self.row("KOL Buy", "budget-2"), now))
        asyncio.run(notifier.process_candidate(int(time.time() * 1000)))
        state = notifier.connection.execute(
            "SELECT state,attempts,last_result FROM sol_early_candidates"
        ).fetchone()
        self.assertEqual(("pending", 0, "PROVIDER_BUDGET"), tuple(state))

    def test_missing_trenches_field_is_reported_by_name(self):
        class MissingRugRatio(Trenches):
            def token_safety(self, chain, token):
                value = super().token_safety(chain, token)
                return value.__class__(
                    **{**value.__dict__, "rug_ratio": None}
                )

        notifier = self.notifier()
        notifier.trenches = MissingRugRatio()
        now = int(time.time() * 1000)
        asyncio.run(notifier.process_row(self.row("KOTH", "missing-1"), now))
        asyncio.run(notifier.process_row(self.row("KOL Buy", "missing-2"), now))
        asyncio.run(notifier.process_candidate(int(time.time() * 1000)))
        state = notifier.connection.execute(
            "SELECT state,last_result FROM sol_early_candidates"
        ).fetchone()
        self.assertEqual("pending", state["state"])
        self.assertIn("rug_ratio", state["last_result"])

    def test_exact_info_quality_failure_short_circuits_bulk_sources(self):
        class LowFee(Info):
            def token_info(self, chain, token):
                value = super().token_info(chain, token)
                return value.__class__(**{**value.__dict__, "total_fee": 0.2})

        class MustNotRun:
            def has_fresh_cache(self, _chain):
                return False

            def token_safety(self, _chain, _token):
                raise AssertionError("trenches must not run")

        notifier = self.notifier()
        notifier.info = LowFee()
        notifier.trenches = MustNotRun()
        now = int(time.time() * 1000)
        asyncio.run(notifier.process_row(self.row("KOTH", "quality-1"), now))
        asyncio.run(notifier.process_row(self.row("KOL Buy", "quality-2"), now))
        asyncio.run(notifier.process_candidate(int(time.time() * 1000)))
        state = notifier.connection.execute(
            "SELECT state,last_result FROM sol_early_candidates"
        ).fetchone()
        self.assertEqual(("blocked", "QUALITY:total_fee"), tuple(state))
        self.assertEqual(1, provider_budget_state(notifier.connection)["used"])

    def test_interrupted_candidate_retries_but_interrupted_send_becomes_unknown(self):
        path = self.root / "recovery.db"
        connection = connect(path)
        initialize(connection)
        now = int(time.time() * 1000)
        schedule_candidate(
            connection,
            token_address=TOKEN,
            detected_at_ms=now,
            expires_at_ms=now + 60_000,
        )
        self.assertIsNotNone(claim_candidate(connection, now_ms=now))
        claim_notification(connection, token_address=TOKEN_TWO, message_text="two", now_ms=now, valid_until_ms=now + 60_000)
        self.assertIsNotNone(claim_due(connection, now_ms=now))
        connection.close()
        connection = connect(path)
        initialize(connection)
        self.assertIsNotNone(claim_candidate(connection, now_ms=now + 1))
        self.assertEqual(1, counts(connection)["unknown"])


if __name__ == "__main__":
    unittest.main()
