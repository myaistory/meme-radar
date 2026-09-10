import asyncio
import sqlite3
import tempfile
import time
import unittest
from collections import Counter
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from meme_radar.adapters import parse_fourmeme
from meme_radar.credentials import CredentialConfig
from meme_radar.config import RadarConfig
from meme_radar.daemon import RadarDaemon
from meme_radar.evm_rpc import NATIVE_SPECS, RpcError, parse_native_launch
from meme_radar.runtime import RuntimeSettings
from meme_radar.models import FeatureSnapshot, ParseBatch
from meme_radar.normalize import build_event, payload_sha256
from meme_radar.sources import (
    GMGN_TRENDING_SOURCE,
    GmgnSafetySnapshot,
    GmgnTokenInfoSnapshot,
    GmgnTrendingSnapshot,
    MarketSnapshot,
    ProviderApiError,
)
from meme_radar.storage import (
    EventIdentityConflict,
    claim_due_enrichment,
    get_runtime_state,
)

from support import load_fixture


NOW = 1_788_516_100_000


class DaemonTests(unittest.TestCase):
    def make_daemon(
        self,
        root,
        *,
        bitquery_enabled=True,
        native_rpc_mode="off",
        cohort_ms=1,
        sample_enabled=False,
        gmgn_enabled=False,
        gmgn_discovery_enabled=False,
        fomo_enabled=True,
    ):
        settings = RuntimeSettings(
            db_path=root / "radar.db",
            health_path=root / "health.json",
            fomo_poll_seconds=300,
            clanker_poll_seconds=60,
            enrich_max_per_hour=30,
            backfill_max_minutes=10,
            fomo_enabled=fomo_enabled,
            bitquery_enabled=bitquery_enabled,
            sample_enabled=sample_enabled,
            telegram_enabled=sample_enabled,
            native_rpc_mode=native_rpc_mode,
            gmgn_enabled=gmgn_enabled,
            gmgn_discovery_enabled=gmgn_discovery_enabled,
        )
        credentials = CredentialConfig(
            bitquery_token="bitquery" if bitquery_enabled else "",
            bitquery_token_standby="",
            fomo_api_key="fomo" if fomo_enabled else "",
            fomo_api_key_standby="",
            goplus_app_key="goplus",
            goplus_app_secret="secret",
            goplus_standby_app_key="",
            goplus_standby_app_secret="",
            goplus_standby_enabled=False,
            telegram_bot_token="123456789:" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcd",
            telegram_chat_id="-1001234567890",
            bsc_rpc_url="https://bsc.example.test/rpc",
            bsc_wss_url="wss://bsc.example.test/ws",
            base_rpc_url="https://base.example.test/rpc",
            base_wss_url="wss://base.example.test/ws",
            robinhood_rpc_url="https://robinhood.example.test/rpc",
            robinhood_wss_url="wss://robinhood.example.test/ws",
        )
        daemon = RadarDaemon(
            settings,
            credentials,
            RadarConfig(priority_cohort_ms=cohort_ms),
        )
        daemon.unified_activation_ms = NOW
        return daemon

    def test_disabled_fomo_needs_no_key_or_task(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp), fomo_enabled=False)
            self.assertIsNone(daemon.fomo)
            self.assertFalse(daemon.settings.fomo_enabled)
            daemon.close()

    def test_native_block_timestamp_retries_head_skew(self):
        class Client:
            calls = 0

            def block_timestamp_ms(inner, block_number):
                inner.calls += 1
                if inner.calls < 3:
                    raise RpcError("BLOCK_MISSING")
                return NOW

        daemon = RadarDaemon.__new__(RadarDaemon)
        client = Client()
        daemon.native_block_times = {}
        daemon.native_http = {"robinhood": (client,)}
        daemon.native_slot_index = {"robinhood": 0}
        daemon.counters = Counter()
        with patch("meme_radar.daemon.asyncio.sleep", new=AsyncMock()):
            result = asyncio.get_event_loop().run_until_complete(
                daemon.native_block_timestamp("robinhood", 16)
            )
        self.assertEqual(NOW, result)
        self.assertEqual(3, client.calls)
        self.assertEqual(
            2,
            daemon.counters["native_rpc_robinhood_block_timestamp_retries"],
        )

    def test_backfill_holds_checkpoint_at_the_first_unprocessed_block(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            spec = NATIVE_SPECS["base"]
            client = SimpleNamespace(
                chain_id=lambda: spec.chain_id,
                block_number=lambda: 0x20,
            )
            daemon.native_http["base"] = (client,)
            logs = [self.native_log(tx_char="c", log_index="0x1")]
            daemon.process_native_log_safe = AsyncMock(return_value=False)
            with patch(
                "meme_radar.daemon.get_logs_with_413_split",
                return_value=(logs, 0),
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.native_backfill("base")
                )
            checkpoint = get_runtime_state(
                daemon.connection,
                daemon.native_checkpoint_key("base"),
            )
            self.assertIsNone(checkpoint)
            hold = daemon.native_checkpoint_hold["base"]
            self.assertEqual(0x10, hold["block"])
            self.assertEqual(1, daemon.counters["native_rpc_base_checkpoint_held"])
            daemon.close()

    def test_backfill_advances_and_releases_when_every_log_is_stored(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            spec = NATIVE_SPECS["base"]
            client = SimpleNamespace(
                chain_id=lambda: spec.chain_id,
                block_number=lambda: 0x20,
            )
            daemon.native_http["base"] = (client,)
            daemon.native_checkpoint_hold["base"] = {
                "block": 0x10,
                "holds": 1,
                "since_ms": NOW,
                "updated_at_ms": NOW,
            }
            daemon.process_native_log_safe = AsyncMock(return_value=True)
            with patch(
                "meme_radar.daemon.get_logs_with_413_split",
                return_value=([self.native_log()], 0),
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.native_backfill("base")
                )
            self.assertEqual(
                0x20,
                get_runtime_state(
                    daemon.connection,
                    daemon.native_checkpoint_key("base"),
                ),
            )
            self.assertNotIn("base", daemon.native_checkpoint_hold)
            self.assertEqual(
                1,
                daemon.counters["native_rpc_base_checkpoint_released"],
            )
            daemon.close()

    def test_live_log_that_fails_processing_does_not_advance_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            daemon.native_backfill = AsyncMock()
            daemon.process_native_log_safe = AsyncMock(return_value=False)

            async def stream(**kwargs):
                yield self.native_log()
                daemon.stop_event.set()

            with patch(
                "meme_radar.daemon.stream_native_logs_once",
                new=stream,
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.native_rpc_loop("base")
                )
            self.assertIsNone(
                get_runtime_state(
                    daemon.connection,
                    daemon.native_checkpoint_key("base"),
                )
            )
            self.assertEqual(
                0x10,
                daemon.native_checkpoint_hold["base"]["block"],
            )
            daemon.close()

    def test_live_log_advances_checkpoint_only_while_not_held(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            daemon.native_backfill = AsyncMock()
            daemon.process_native_log_safe = AsyncMock(return_value=True)
            daemon.native_checkpoint_hold["base"] = {
                "block": 0x08,
                "holds": 1,
                "since_ms": NOW,
                "updated_at_ms": NOW,
            }

            async def stream(**kwargs):
                yield self.native_log()
                daemon.stop_event.set()

            with patch(
                "meme_radar.daemon.stream_native_logs_once",
                new=stream,
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.native_rpc_loop("base")
                )
            self.assertIsNone(
                get_runtime_state(
                    daemon.connection,
                    daemon.native_checkpoint_key("base"),
                )
            )
            daemon.close()

    def test_unparseable_failed_log_holds_the_batch_lower_bound(self):
        daemon = RadarDaemon.__new__(RadarDaemon)
        daemon.counters = Counter()
        daemon.native_checkpoint_hold = {}
        daemon.hold_native_checkpoint("bsc", 41)
        daemon.hold_native_checkpoint("bsc", 41)
        self.assertEqual(2, daemon.native_checkpoint_hold["bsc"]["holds"])
        daemon.hold_native_checkpoint("bsc", 12)
        self.assertEqual(12, daemon.native_checkpoint_hold["bsc"]["block"])
        self.assertEqual(1, daemon.native_checkpoint_hold["bsc"]["holds"])

    def test_token_metadata_failure_is_counted_not_silent(self):
        daemon = RadarDaemon.__new__(RadarDaemon)
        daemon.counters = Counter()
        daemon.last_error = {}
        daemon.record_token_metadata_error("symbol", "RpcError")
        self.assertEqual(
            1,
            daemon.counters["native_token_metadata_error_symbol"],
        )
        self.assertEqual(
            "RpcError",
            daemon.last_error["native_token_metadata_symbol"]["type"],
        )

    def test_native_processing_error_does_not_trigger_rpc_failover(self):
        daemon = RadarDaemon.__new__(RadarDaemon)
        daemon.counters = Counter()
        daemon.last_error = {}
        daemon.process_native_log = AsyncMock(
            side_effect=sqlite3.OperationalError("disk I/O error")
        )
        result = asyncio.get_event_loop().run_until_complete(
            daemon.process_native_log_safe("bsc", {}, is_backfill=True)
        )
        self.assertFalse(result)
        self.assertEqual(1, daemon.counters["native_rpc_bsc_processing_rejected"])
        self.assertEqual(
            "OperationalError",
            daemon.last_error["native_processing_bsc"]["type"],
        )

    def test_a_duplicate_launch_never_holds_the_checkpoint(self):
        """The same log arrives live and via backfill, and only the live path
        fetches name and symbol, so storage refuses the second rendering. The
        event is stored either way, so the checkpoint must keep moving."""
        daemon = RadarDaemon.__new__(RadarDaemon)
        daemon.counters = Counter()
        daemon.last_error = {}
        daemon.process_native_log = AsyncMock(
            side_effect=EventIdentityConflict("conflicting event identity")
        )
        result = asyncio.get_event_loop().run_until_complete(
            daemon.process_native_log_safe("bsc", {}, is_backfill=True)
        )
        self.assertTrue(result)
        self.assertEqual(1, daemon.counters["native_rpc_bsc_identity_conflict"])
        self.assertEqual(0, daemon.counters["native_rpc_bsc_processing_rejected"])
        self.assertNotIn("native_processing_bsc", daemon.last_error)

    def test_native_rpc_error_still_reaches_failover_handler(self):
        daemon = RadarDaemon.__new__(RadarDaemon)
        daemon.counters = Counter()
        daemon.last_error = {}
        daemon.process_native_log = AsyncMock(side_effect=RpcError("NETWORK_ERROR"))
        with self.assertRaisesRegex(RpcError, "NETWORK_ERROR"):
            asyncio.get_event_loop().run_until_complete(
                daemon.process_native_log_safe("base", {}, is_backfill=False)
            )

    def test_primary_probe_recovers_only_after_three_successes(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            primary = SimpleNamespace(chain_id=lambda: 56, block_number=lambda: 100)
            standby = SimpleNamespace(chain_id=lambda: 56, block_number=lambda: 100)
            daemon.native_http["bsc"] = (primary, standby)
            daemon.native_endpoints["bsc"] = (
                ("https://primary.example/rpc", "wss://primary.example/ws"),
                ("https://standby.example/rpc", "wss://standby.example/ws"),
            )
            daemon.native_slot_index["bsc"] = 1
            daemon.native_primary_probe_successes["bsc"] = 0
            daemon.native_reconnect_events["bsc"].clear()
            with patch(
                "meme_radar.daemon.probe_native_wss_once",
                new=AsyncMock(),
            ):
                first = asyncio.get_event_loop().run_until_complete(
                    daemon.probe_primary_once("bsc")
                )
                second = asyncio.get_event_loop().run_until_complete(
                    daemon.probe_primary_once("bsc")
                )
                third = asyncio.get_event_loop().run_until_complete(
                    daemon.probe_primary_once("bsc")
                )
            self.assertEqual((False, False, True), (first, second, third))
            self.assertEqual(0, daemon.native_slot_index["bsc"])
            self.assertTrue(daemon.native_reconnect_events["bsc"].is_set())
            self.assertEqual(1, daemon.counters["native_rpc_bsc_failback_primary"])
            daemon.close()

    def test_failed_primary_probe_resets_recovery_streak(self):
        daemon = RadarDaemon.__new__(RadarDaemon)
        daemon.native_slot_index = {"robinhood": 1}
        daemon.native_primary_probe_successes = {"robinhood": 2}
        daemon.native_http = {
            "robinhood": (SimpleNamespace(chain_id=lambda: (_ for _ in ()).throw(RpcError("NETWORK_ERROR"))),)
        }
        daemon.native_endpoints = {
            "robinhood": (("https://primary.example/rpc", "wss://primary.example/ws"),)
        }
        daemon.counters = Counter()
        daemon.last_error = {}
        result = asyncio.get_event_loop().run_until_complete(
            daemon.probe_primary_once("robinhood")
        )
        self.assertFalse(result)
        self.assertEqual(0, daemon.native_primary_probe_successes["robinhood"])
        self.assertEqual(1, daemon.counters["native_rpc_robinhood_primary_probe_failed"])

    def test_pre_activation_job_finishes_before_provider_calls(self):
        daemon = RadarDaemon.__new__(RadarDaemon)
        daemon.unified_activation_ms = NOW + 1_000
        daemon.counters = Counter()
        finished = []
        daemon.finish_job = lambda job, code, now_ms: finished.append(
            (job, code, now_ms)
        )
        job = {"event": SimpleNamespace(received_at_ms=NOW)}
        with patch(
            "meme_radar.daemon.time.time",
            return_value=(NOW + 2_000) / 1000,
        ):
            asyncio.get_event_loop().run_until_complete(
                daemon.process_market_stage(job)
            )
        self.assertEqual("UNIFIED_PRE_ACTIVATION", finished[0][1])
        self.assertEqual(1, daemon.counters["unified_pre_activation"])

    def test_unified_main_gate_accepts_complete_creator_absence(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            log = self.native_log()
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="unified-pass",
                payload={"chain": "base", "log": log},
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            event = batch.events[0]
            daemon.unified_activation_ms = NOW
            daemon.native_http["base"] = (
                SimpleNamespace(block_number=lambda: 16),
            )
            job = {
                "event": event,
                "features": FeatureSnapshot(
                    event_id=event.event_id,
                    evaluated_at_ms=NOW + 120_000,
                    metadata_complete=True,
                    narrative_burst=3,
                    cross_chain_count=1,
                ),
            }
            flow = SimpleNamespace(
                inbound_count=0,
                outbound_count=0,
                retention_ratio=None,
                balance_raw=0,
            )
            with patch(
                "meme_radar.daemon.collect_creator_flow",
                return_value=flow,
            ):
                result = asyncio.get_event_loop().run_until_complete(
                    daemon.unified_main_market(
                        job,
                        self.market(NOW + 120_000, chain="base"),
                        NOW + 120_000,
                    )
                )
            self.assertIsNotNone(result)
            self.assertEqual(1, daemon.counters["unified_pass"])
            daemon.close()

    def test_gmgn_creator_close_is_observed_but_not_vetoed(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            log = self.native_log()
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="gmgn-veto",
                payload={"chain": "base", "log": log},
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            event = batch.events[0]
            snapshot = GmgnSafetySnapshot(
                chain="base",
                token_address=event.token_address,
                observed_at_ms=NOW + 200,
                market_cap_usd=200_000,
                total_fee=0.01,
                creator_token_status="creator_close",
                creator_balance_rate=0.0,
                is_honeypot=False,
                is_open_source=True,
                owner_renounced=True,
                is_wash_trading=False,
                rug_ratio=0.0,
                top_10_holder_rate=0.1,
                bundler_rate=0.0,
                insider_rate=0.0,
                entrapment_ratio=0.0,
                bytes_read=100,
                elapsed_ms=10,
            )
            daemon.gmgn = SimpleNamespace(
                has_fresh_cache=lambda _chain: True,
                token_safety=lambda _chain, _address: snapshot,
            )
            reasons = asyncio.get_event_loop().run_until_complete(
                daemon.gmgn_gate(event, NOW + 200)
            )
            self.assertEqual([], reasons)
            row = daemon.connection.execute(
                "SELECT status FROM provider_observations "
                "WHERE event_id=? AND provider='gmgn'",
                (event.event_id,),
            ).fetchone()
            self.assertEqual("ok", row["status"])
            daemon.close()

    def test_gmgn_size_prefilter_blocks_strict_threshold(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
                gmgn_enabled=True,
            )
            log = self.native_log()
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="gmgn-size-block",
                payload={"chain": "base", "log": log},
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            job = claim_due_enrichment(
                daemon.connection,
                now_ms=NOW + 5 * 60_000,
                initial_available=True,
                initial_chains=("base",),
                dex_available=True,
                goplus_available=True,
            )
            snapshot = GmgnTokenInfoSnapshot(
                chain="base",
                token_address=batch.events[0].token_address,
                observed_at_ms=NOW + 5 * 60_000,
                holder_count=100,
                market_cap_usd=100_000,
                price_usd=0.001,
                liquidity_usd=50_000,
                bytes_read=100,
                elapsed_ms=10,
            )
            snapshots = [snapshot]
            gmgn_calls = []

            def token_info(chain, address):
                gmgn_calls.append((chain, address))
                return snapshots[-1]

            daemon.gmgn_info = SimpleNamespace(
                has_fresh_cache=lambda _chain, _address: True,
                token_info=token_info,
            )
            result = asyncio.get_event_loop().run_until_complete(
                daemon.gmgn_size_prefilter(job, NOW + 5 * 60_000)
            )
            self.assertIsNone(result)
            row = daemon.connection.execute(
                "SELECT state,result_code FROM enrichment_jobs WHERE event_id=?",
                (batch.events[0].event_id,),
            ).fetchone()
            self.assertEqual(("pending", None), tuple(row))
            due = daemon.connection.execute(
                "SELECT due_at_ms FROM enrichment_jobs WHERE event_id=?",
                (batch.events[0].event_id,),
            ).fetchone()[0]
            self.assertEqual(NOW + 10 * 60_000, due)
            self.assertEqual(1, daemon.counters["size_market_cap_not_above_min"])
            self.assertEqual(1, daemon.counters["size_holder_count_not_above_min"])
            self.assertEqual(1, daemon.counters["market_size_recheck_scheduled"])
            recheck = claim_due_enrichment(
                daemon.connection,
                now_ms=NOW + 10 * 60_000,
                initial_available=False,
                initial_chains=(),
                dex_available=False,
                goplus_available=False,
            )
            self.assertIsNotNone(recheck)
            snapshots.append(
                GmgnTokenInfoSnapshot(
                    chain="base",
                    token_address=batch.events[0].token_address,
                    observed_at_ms=NOW + 10 * 60_000,
                    holder_count=101,
                    market_cap_usd=200_000,
                    price_usd=0.002,
                    liquidity_usd=50_000,
                    bytes_read=100,
                    elapsed_ms=10,
                )
            )
            daemon.dex = SimpleNamespace(
                token_market=lambda chain, _address: self.market(
                    NOW + 10 * 60_000,
                    chain=chain,
                )
            )
            with patch(
                "meme_radar.daemon.time.time",
                return_value=(NOW + 10 * 60_000) / 1000,
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.process_market_stage(recheck)
                )
            self.assertEqual(2, len(gmgn_calls))
            stage = daemon.connection.execute(
                "SELECT stage FROM enrichment_jobs WHERE event_id=?",
                (batch.events[0].event_id,),
            ).fetchone()[0]
            self.assertEqual("security_check", stage)
            daemon.close()

    @staticmethod
    def native_log(tx_char="b", log_index="0x2"):
        spec = NATIVE_SPECS["base"]
        token = "0x" + "1" * 40
        creator = "0x" + "2" * 40
        topic = lambda address: "0x" + "0" * 24 + address[2:]
        return {
            "address": spec.factory,
            "topics": [spec.topic0, topic(token), topic(creator)],
            "data": "0x",
            "blockNumber": "0x10",
            "blockHash": "0x" + "a" * 64,
            "transactionHash": "0x" + tx_char * 64,
            "logIndex": log_index,
            "removed": False,
        }

    @staticmethod
    def market(observed_at_ms, pair_count=1, chain="bsc"):
        return MarketSnapshot(
            chain=chain,
            token_address="0x" + "1" * 40,
            observed_at_ms=observed_at_ms,
            pair_count=pair_count,
            best_pair_address="pair" if pair_count else "",
            pair_created_at_ms=NOW,
            price_usd=0.001 if pair_count else None,
            liquidity_usd=60_000 if pair_count else None,
            volume_5m_usd=20_000 if pair_count else None,
            volume_1h_usd=100_000 if pair_count else None,
            volume_24h_usd=300_000 if pair_count else None,
            buy_transactions_5m=30 if pair_count else None,
            sell_transactions_5m=10 if pair_count else None,
            buy_transactions_1h=80 if pair_count else None,
            sell_transactions_1h=30 if pair_count else None,
            buy_transactions_24h=120 if pair_count else None,
            sell_transactions_24h=50 if pair_count else None,
            elapsed_ms=10,
            bytes_read=100,
            market_cap_usd=200_000 if pair_count else None,
            fdv_usd=200_000 if pair_count else None,
            holder_count=101 if pair_count else None,
            creator_inbound_count=0 if pair_count else None,
            creator_outbound_count=0 if pair_count else None,
            identity_count=1 if pair_count else None,
            identity_market_cap_rank=1 if pair_count else None,
            external_buy_transactions_5m=30 if pair_count else None,
        )

    def test_native_primary_can_run_without_bitquery(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            log = self.native_log()
            payload = {"chain": "base", "log": log}
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            inserted = daemon.process_batch(
                source="native_rpc",
                batch_id="native-live",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            self.assertEqual(1, inserted)
            self.assertEqual(
                1,
                daemon.connection.execute(
                    "SELECT COUNT(*) FROM enrichment_jobs"
                ).fetchone()[0],
            )

            bitquery_payload = load_fixture("fourmeme_events.json")
            daemon.process_batch(
                source="bitquery",
                batch_id="bitquery-live",
                payload=bitquery_payload,
                batch=parse_fourmeme(
                    bitquery_payload,
                    observed_at_ms=NOW,
                    received_at_ms=NOW,
                ),
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            self.assertEqual(
                1,
                daemon.connection.execute(
                    "SELECT COUNT(*) FROM enrichment_jobs"
                ).fetchone()[0],
            )

            backfill_log = self.native_log("c", "0x3")
            backfill_payload = {"chain": "base", "log": backfill_log}
            backfill = parse_native_launch(
                backfill_log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 200,
                received_at_ms=NOW + 200,
                is_backfill=True,
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="native-backfill",
                payload=backfill_payload,
                batch=backfill,
                observed_at_ms=NOW + 200,
                received_at_ms=NOW + 200,
            )
            self.assertEqual(
                1,
                daemon.connection.execute(
                    "SELECT COUNT(*) FROM enrichment_jobs"
                ).fetchone()[0],
            )
            self.assertEqual("disabled", daemon.bitquery_slot())
            self.assertFalse(daemon.health_payload()["bitquery_enabled"])
            daemon.close()

    def test_robinhood_main_lane_cannot_bypass_curve_notifier(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            event = SimpleNamespace(
                is_backfill=False,
                chain="robinhood",
                observed_at_ms=NOW,
                source_published_at_ms=NOW,
                token_age_ms=0,
            )
            self.assertFalse(daemon.candidate_eligible("native_rpc", event))
            daemon.close()

    def test_candidates_are_aligned_to_priority_cohorts(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
                cohort_ms=120_000,
            )
            log = self.native_log()
            payload = {"chain": "base", "log": log}
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="cohort",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            due_at_ms = daemon.connection.execute(
                "SELECT due_at_ms FROM enrichment_jobs"
            ).fetchone()[0]
            earliest = NOW + 5 * 60_000
            expected = ((earliest + 119_999) // 120_000) * 120_000
            self.assertEqual(expected, due_at_ms)
            budget = daemon.initial_probe_budget.snapshot()
            self.assertEqual(
                (20, 3_600),
                (budget["limit"], budget["window_seconds"]),
            )
            daemon.close()

    def test_main_size_recheck_schedule_is_five_ten_twenty_thirty(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp))
            event = SimpleNamespace(
                token_created_at_ms=NOW,
                received_at_ms=NOW,
            )
            self.assertEqual(
                NOW + 10 * 60_000,
                daemon.next_size_check_ms(event, NOW + 5 * 60_000),
            )
            self.assertEqual(
                NOW + 20 * 60_000,
                daemon.next_size_check_ms(event, NOW + 10 * 60_000),
            )
            self.assertEqual(
                NOW + 30 * 60_000,
                daemon.next_size_check_ms(event, NOW + 20 * 60_000),
            )
            self.assertIsNone(
                daemon.next_size_check_ms(event, NOW + 30 * 60_000)
            )
            daemon.close()

    def test_size_rechecks_stop_far_below_floor_and_reduce_mid_band(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp))
            event = SimpleNamespace(
                token_created_at_ms=NOW,
                received_at_ms=NOW,
            )
            base = dict(
                chain="base",
                token_address="0x" + "1" * 40,
                observed_at_ms=NOW + 5 * 60_000,
                price_usd=0.001,
                liquidity_usd=10_000,
                bytes_read=100,
                elapsed_ms=10,
            )
            far = GmgnTokenInfoSnapshot(
                holder_count=10,
                market_cap_usd=10_000,
                **base,
            )
            mid = GmgnTokenInfoSnapshot(
                holder_count=40,
                market_cap_usd=40_000,
                **base,
            )
            self.assertIsNone(
                daemon.next_size_check_ms(event, NOW + 5 * 60_000, far)
            )
            self.assertEqual(
                NOW + 10 * 60_000,
                daemon.next_size_check_ms(event, NOW + 5 * 60_000, mid),
            )
            self.assertEqual(
                NOW + 20 * 60_000,
                daemon.next_size_check_ms(event, NOW + 10 * 60_000, mid),
            )
            self.assertIsNone(
                daemon.next_size_check_ms(event, NOW + 20 * 60_000, mid)
            )
            daemon.close()

    def test_gmgn_budget_is_split_between_initial_and_size_rechecks(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp), gmgn_enabled=True)
            self.assertEqual(120, daemon.initial_probe_budget.snapshot()["limit"])
            self.assertEqual(60, daemon.size_recheck_budget.snapshot()["limit"])
            self.assertEqual(
                {"bsc": 120, "base": 120, "robinhood": 120},
                {
                    chain: budget.snapshot()["limit"]
                    for chain, budget in daemon.initial_chain_budgets.items()
                },
            )
            daemon.close()

    def test_gmgn_calls_are_paced_and_cooldown_is_shared(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp), gmgn_enabled=True)
            daemon.gmgn_next_call_at = time.monotonic() + 1.0
            with patch(
                "meme_radar.daemon.asyncio.sleep",
                new=AsyncMock(),
            ) as sleep:
                result = asyncio.get_event_loop().run_until_complete(
                    daemon.run_gmgn_call(lambda value: value, "ok")
                )
            self.assertEqual("ok", result)
            self.assertEqual(1, sleep.await_count)
            self.assertGreater(sleep.await_args.args[0], 0.5)
            self.assertGreater(
                daemon.gmgn_next_call_at,
                time.monotonic(),
            )
            daemon.cooldown_all_gmgn(60)
            self.assertGreater(
                daemon.gmgn_budget.snapshot()["cooldown_seconds"],
                0,
            )
            self.assertGreater(
                daemon.gmgn_discovery_budget.snapshot()["cooldown_seconds"],
                0,
            )
            daemon.close()

    def test_pressure_admission_keeps_high_and_drops_plain_medium(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp))
            event = SimpleNamespace(
                event_id="event",
                source="native_rpc",
                token_created_at_ms=NOW,
                received_at_ms=NOW,
            )
            features = SimpleNamespace(event_id="event")
            with patch(
                "meme_radar.daemon.enrichment_queue_under_pressure",
                return_value=True,
            ), patch("meme_radar.daemon.schedule_enrichment_job") as schedule:
                daemon.priority_tracker = SimpleNamespace(
                    score=lambda *_args: SimpleNamespace(
                        score=49,
                        reason_codes=("SOURCE_FRESH_30",),
                    )
                )
                daemon.schedule_candidate(event, features)
                schedule.assert_not_called()
                daemon.priority_tracker = SimpleNamespace(
                    score=lambda *_args: SimpleNamespace(
                        score=65,
                        reason_codes=("SOURCE_FRESH_30",),
                    )
                )
                schedule.return_value = True
                daemon.schedule_candidate(event, features)
                schedule.assert_called_once()
            self.assertEqual(1, daemon.counters["priority_admission_pressure"])
            daemon.close()

    def test_gmgn_solana_growth_candidate_reaches_strong_outbox(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
                gmgn_enabled=True,
                gmgn_discovery_enabled=True,
            )
            token = "So11111111111111111111111111111111111111112"
            creator = "11111111111111111111111111111111"
            payload = {"code": 0, "data": {"rank": [{"address": token}]}}
            digest = payload_sha256(payload)
            event = build_event(
                source=GMGN_TRENDING_SOURCE,
                source_event_id="solana:" + token,
                event_kind="launch",
                chain="solana",
                token_address=token,
                source_published_at_ms=NOW - 10 * 60_000,
                observed_at_ms=NOW,
                received_at_ms=NOW,
                launchpad="pump.fun",
                token_created_at_ms=NOW - 10 * 60_000,
                name="Growth Meme",
                symbol="GROW",
                creator=creator,
                raw_sha256=digest,
            )
            snapshot = GmgnTrendingSnapshot(
                event_id=event.event_id,
                chain="solana",
                token_address=token,
                observed_at_ms=NOW,
                holder_count=250,
                market_cap_usd=250_000,
                liquidity_usd=60_000,
                volume_5m_usd=25_000,
                swaps_5m=120,
                buys_5m=40,
                sells_5m=80,
                smart_degen_count=5,
                renowned_count=2,
                creator_close=True,
                is_honeypot=None,
                is_open_source=None,
                owner_renounced=None,
                renounced_mint=True,
                renounced_freeze=True,
                is_wash_trading=False,
                rug_ratio=0.1,
                top_10_holder_rate=0.2,
                bundler_rate=0.1,
                insider_rate=0.1,
                entrapment_ratio=0.1,
                dev_team_hold_rate=0.1,
                bytes_read=100,
                elapsed_ms=10,
            )
            result = SimpleNamespace(
                payload=payload,
                batch=ParseBatch((event,), (), digest),
                snapshots=(snapshot,),
                observed_at_ms=NOW,
            )
            self.assertEqual(1, daemon.ingest_gmgn_discovery("solana", result))
            due = daemon.connection.execute(
                "SELECT due_at_ms FROM enrichment_jobs WHERE event_id=?",
                (event.event_id,),
            ).fetchone()[0]
            self.assertGreaterEqual(due, NOW)
            self.assertLess(due, NOW + 120_000)
            job = claim_due_enrichment(daemon.connection, now_ms=due)
            market = replace(
                self.market(due, chain="solana"),
                token_address=token,
            )
            daemon.dex = SimpleNamespace(
                token_market=lambda _chain, _address: market
            )
            with patch(
                "meme_radar.daemon.time.time",
                return_value=due / 1000,
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.process_market_stage(job)
                )
            security = claim_due_enrichment(
                daemon.connection,
                now_ms=due + 1,
            )
            with patch(
                "meme_radar.daemon.time.time",
                return_value=(due + 1) / 1000,
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.process_security_stage(security)
                )
            row = daemon.connection.execute(
                "SELECT state,result_code FROM enrichment_jobs WHERE event_id=?",
                (event.event_id,),
            ).fetchone()
            self.assertEqual(("done", "FINAL_STRONG"), tuple(row))
            self.assertEqual(
                1,
                daemon.connection.execute(
                    "SELECT count(*) FROM telegram_outbox WHERE state='pending'"
                ).fetchone()[0],
            )
            providers = {
                row[0]
                for row in daemon.connection.execute(
                    "SELECT provider FROM provider_observations WHERE event_id=?",
                    (event.event_id,),
                )
            }
            self.assertIn("gmgn_trending", providers)
            self.assertIn("gmgn_solana_security", providers)
            daemon.close()

    def test_native_shadow_never_queues_for_delivery(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                native_rpc_mode="shadow",
            )
            log = self.native_log()
            payload = {"chain": "base", "log": log}
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="native-shadow",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            self.assertEqual(
                0,
                daemon.connection.execute(
                    "SELECT COUNT(*) FROM enrichment_jobs"
                ).fetchone()[0],
            )
            daemon.close()

    def test_disabled_bitquery_task_is_not_started(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            calls = []

            async def forbidden_bitquery():
                calls.append("bitquery")

            async def stop_source(*args):
                daemon.stop_event.set()

            daemon.bitquery_loop = forbidden_bitquery
            daemon.fomo_loop = stop_source
            daemon.clanker_loop = stop_source
            daemon.native_rpc_loop = stop_source
            asyncio.get_event_loop().run_until_complete(daemon.run())
            self.assertNotIn("bitquery", calls)
            daemon.close()

    def test_background_task_failure_is_fatal(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp))

            async def scenario():
                async def fail():
                    raise ValueError("bounded test failure")

                task = daemon.tracked_task("test", fail())
                await asyncio.gather(task, return_exceptions=True)
                await asyncio.sleep(0)

            asyncio.get_event_loop().run_until_complete(scenario())
            self.assertTrue(daemon.stop_event.is_set())
            self.assertIsInstance(daemon.fatal_task_error, ValueError)
            self.assertEqual(
                "ValueError",
                daemon.last_error["task_test"]["type"],
            )
            daemon.close()

    def test_process_batch_persists_and_queues_live_events(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            daemon = self.make_daemon(root)
            payload = load_fixture("fourmeme_events.json")
            batch = parse_fourmeme(
                payload,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            inserted = daemon.process_batch(
                source="bitquery",
                batch_id="fixture",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW,
                received_at_ms=NOW,
                checkpoint_source="fourmeme",
            )
            self.assertEqual(2, inserted)
            self.assertEqual(
                2,
                daemon.connection.execute(
                    "SELECT COUNT(*) FROM enrichment_jobs"
                ).fetchone()[0],
            )
            self.assertEqual(
                2,
                daemon.connection.execute(
                    "SELECT COUNT(*) FROM radar_events"
                ).fetchone()[0],
            )
            self.assertEqual(
                2,
                daemon.connection.execute(
                    "SELECT COUNT(*) FROM decisions"
                ).fetchone()[0],
            )
            daemon.close()

    def test_goplus_4029_sets_global_cooldown(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp))
            payload = load_fixture("fourmeme_events.json")
            event = parse_fourmeme(
                payload,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            ).events[0]
            daemon.process_batch(
                source="bitquery",
                batch_id="fixture",
                payload=payload,
                batch=parse_fourmeme(
                    payload,
                    observed_at_ms=NOW,
                    received_at_ms=NOW,
                ),
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            daemon.provider_failure(
                event,
                "goplus",
                ProviderApiError(4029),
            )
            self.assertGreater(
                daemon.goplus_budget.snapshot()["cooldown_seconds"],
                0,
            )
            self.assertEqual(1, daemon.counters["goplus_4029_cooldown"])
            daemon.close()

    def test_bitquery_primary_fails_over_after_three_errors(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            daemon = self.make_daemon(root)
            daemon.bitquery_tokens = ("primary", "standby")
            daemon.histories = (object(), object())
            daemon.record_bitquery_failure(RuntimeError("protocol error"))
            daemon.record_bitquery_failure(RuntimeError("protocol error"))
            self.assertEqual("primary", daemon.bitquery_slot())
            daemon.record_bitquery_failure(RuntimeError("protocol error"))
            self.assertEqual("standby", daemon.bitquery_slot())
            self.assertEqual(
                "standby",
                daemon.connection.execute(
                    "SELECT json_extract(value_json, '$') FROM runtime_state "
                    "WHERE key='bitquery_active_slot'"
                ).fetchone()[0],
            )
            self.assertEqual(1, daemon.counters["bitquery_failover_to_standby"])
            daemon.close()

    def test_enrichment_creates_only_final_strong_outbox_item(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp))
            payload = load_fixture("fourmeme_events.json")
            batch = parse_fourmeme(
                payload,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            daemon.process_batch(
                source="bitquery",
                batch_id="fixture",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )

            event = batch.events[0]
            decision = daemon.finalize_enrichment(
                event,
                FeatureSnapshot(
                    event_id=event.event_id,
                    evaluated_at_ms=NOW,
                    metadata_complete=True,
                ),
                self.market(NOW + 200),
                {
                    "is_honeypot": "0",
                    "cannot_sell_all": "0",
                    "is_blacklisted": "0",
                    "is_mintable": "0",
                    "hidden_owner": "0",
                    "selfdestruct": "0",
                    "external_call": "0",
                    "transfer_pausable": "0",
                    "creator_percent": "0.01",
                    "owner_percent": "0",
                },
                NOW + 200,
            )
            self.assertEqual("strong", decision.delivery)
            rows = daemon.connection.execute(
                "SELECT state, message_text FROM telegram_outbox"
            ).fetchall()
            self.assertEqual(1, len(rows))
            self.assertEqual("pending", rows[0]["state"])
            self.assertIn("强信号", rows[0]["message_text"])
            final = daemon.connection.execute(
                """
                SELECT risk_verdict, delivery FROM decisions
                WHERE feature_version=?
                """,
                (daemon.config.feature_version,),
            ).fetchone()
            self.assertEqual(("pass", "strong"), tuple(final))
            daemon.close()

    def test_zero_market_can_never_be_an_observation_sample(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp), sample_enabled=True)
            payload = load_fixture("fourmeme_events.json")
            batch = parse_fourmeme(
                payload,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            daemon.process_batch(
                source="bitquery",
                batch_id="sample",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            event = batch.events[0]
            features = FeatureSnapshot(
                event_id=event.event_id,
                evaluated_at_ms=NOW + 100,
                metadata_complete=True,
                cross_chain_count=2,
            )
            market = self.market(NOW + 200)
            market = MarketSnapshot(
                **{
                    **market.__dict__,
                    "price_usd": None,
                    "liquidity_usd": None,
                    "volume_5m_usd": 0,
                    "volume_1h_usd": 0,
                    "volume_24h_usd": 0,
                    "buy_transactions_5m": 0,
                    "sell_transactions_5m": 0,
                    "buy_transactions_1h": 0,
                    "sell_transactions_1h": 0,
                    "buy_transactions_24h": 0,
                    "sell_transactions_24h": 0,
                }
            )
            decision = daemon.finalize_enrichment(
                event,
                features,
                market,
                {},
                NOW + 200,
            )
            self.assertEqual(("review", "weak", 40, 20), (
                decision.risk_verdict,
                decision.delivery,
                decision.confidence_score,
                decision.opportunity_score,
            ))
            row = daemon.connection.execute(
                "SELECT state,message_text FROM telegram_outbox"
            ).fetchone()
            self.assertIsNone(row)
            self.assertEqual(0, daemon.counters["sample_enqueued"])
            self.assertFalse(
                daemon.sample_eligible(
                    event,
                    decision,
                    features,
                    market,
                    {"is_honeypot": "1"},
                )
            )
            daemon.close()

    def test_real_market_review_can_be_an_observation_sample(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp), sample_enabled=True)
            payload = load_fixture("fourmeme_events.json")
            batch = parse_fourmeme(
                payload,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            daemon.process_batch(
                source="bitquery",
                batch_id="sample-real-market",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW,
                received_at_ms=NOW,
            )
            event = batch.events[0]
            features = FeatureSnapshot(
                event_id=event.event_id,
                evaluated_at_ms=NOW + 100,
                metadata_complete=True,
                cross_chain_count=2,
            )
            market = self.market(NOW + 200)
            decision = daemon.finalize_enrichment(
                event,
                features,
                market,
                {},
                NOW + 200,
            )
            self.assertEqual(("review", "weak", 50, 85), (
                decision.risk_verdict,
                decision.delivery,
                decision.confidence_score,
                decision.opportunity_score,
            ))
            row = daemon.connection.execute(
                "SELECT state,message_text FROM telegram_outbox"
            ).fetchone()
            self.assertEqual("pending", row["state"])
            self.assertIn("$20.0K · 30 买 / 10 卖", row["message_text"])
            self.assertIn("🟢 5分钟成交", row["message_text"])
            daemon.close()

    def test_fourmeme_market_requires_price_volume_and_buy_dominance(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(Path(temp), sample_enabled=True)
            event = parse_fourmeme(
                load_fixture("fourmeme_events.json"),
                observed_at_ms=NOW,
                received_at_ms=NOW,
            ).events[0]
            market = self.market(NOW + 100)
            self.assertTrue(daemon.market_ready_for_enrichment(event, market))
            for changes in (
                {"price_usd": None},
                {"volume_5m_usd": 0},
                {"volume_5m_usd": 9.99},
                {"buy_transactions_5m": 2},
                {"buy_transactions_5m": 10, "sell_transactions_5m": 10},
            ):
                with self.subTest(changes=changes):
                    broken = MarketSnapshot(
                        **{**market.__dict__, **changes}
                    )
                    self.assertFalse(
                        daemon.market_ready_for_enrichment(event, broken)
                    )
            daemon.close()

    def test_pair_moves_to_goplus_after_five_minute_size_wait(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            log = self.native_log()
            payload = {"chain": "base", "log": log}
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="staged",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )

            class Dex:
                calls = 0

                def token_market(inner, chain, token_address):
                    inner.calls += 1
                    observed = NOW + 5 * 60_000
                    return DaemonTests.market(observed, chain=chain)

            class GoPlus:
                calls = 0

                def token_security(inner, chain, token_address):
                    inner.calls += 1
                    return SimpleNamespace(
                        observed_at_ms=NOW + 5 * 60_000 + 1,
                        payload={
                            "is_honeypot": "0",
                            "cannot_sell_all": "0",
                            "is_blacklisted": "0",
                            "creator_percent": "0.01",
                        },
                    )

            daemon.dex = Dex()
            daemon.goplus = GoPlus()
            first = claim_due_enrichment(
                daemon.connection,
                now_ms=NOW + 5 * 60_000,
            )
            with patch(
                "meme_radar.daemon.time.time",
                return_value=(NOW + 5 * 60_000) / 1000,
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.process_market_stage(first)
                )
            self.assertEqual(0, daemon.goplus.calls)

            security = claim_due_enrichment(
                daemon.connection,
                now_ms=NOW + 5 * 60_000 + 1,
            )
            with patch(
                "meme_radar.daemon.time.time",
                return_value=(NOW + 5 * 60_000 + 1) / 1000,
            ):
                with patch.object(
                    daemon,
                    "unified_main_market",
                    new=AsyncMock(side_effect=lambda job, market, now_ms: market),
                ):
                    asyncio.get_event_loop().run_until_complete(
                        daemon.process_security_stage(security)
                    )
            self.assertEqual(1, daemon.dex.calls)
            self.assertEqual(1, daemon.goplus.calls)
            state = daemon.connection.execute(
                "SELECT state, result_code FROM enrichment_jobs"
            ).fetchone()
            self.assertEqual(("done", "FINAL_STRONG"), tuple(state))
            daemon.close()

    def test_missing_pair_is_delayed_without_goplus(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            log = self.native_log()
            payload = {"chain": "base", "log": log}
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="missing-pair",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )

            class Dex:
                def token_market(inner, chain, token_address):
                    return DaemonTests.market(
                        NOW + 5 * 60_000,
                        pair_count=0,
                        chain=chain,
                    )

            class GoPlus:
                calls = 0

                def token_security(inner, chain, token_address):
                    inner.calls += 1
                    raise AssertionError("GoPlus must not run without a pair")

            daemon.dex = Dex()
            daemon.goplus = GoPlus()
            job = claim_due_enrichment(
                daemon.connection,
                now_ms=NOW + 5 * 60_000,
            )
            with patch(
                "meme_radar.daemon.time.time",
                return_value=(NOW + 5 * 60_000) / 1000,
            ):
                asyncio.get_event_loop().run_until_complete(
                    daemon.process_market_stage(job)
                )
            row = daemon.connection.execute(
                "SELECT state, stage, dex_attempts, due_at_ms "
                "FROM enrichment_jobs"
            ).fetchone()
            self.assertEqual(
                ("pending", "dex_recheck", 1, NOW + 6 * 60_000),
                tuple(row),
            )
            self.assertEqual(0, daemon.goplus.calls)
            daemon.close()

    def test_old_candidate_retries_never_collapse(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            log = self.native_log()
            payload = {"chain": "base", "log": log}
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="old-candidate",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )
            job = claim_due_enrichment(
                daemon.connection,
                now_ms=NOW + 7 * 60_000,
            )
            with patch("meme_radar.daemon.time.time", return_value=(NOW + 7 * 60_000) / 1000):
                daemon.retry_market(
                    job,
                    attempts=1,
                    now_ms=NOW + 7 * 60_000,
                    error="DEX_UNAVAILABLE",
                )
            due_at_ms = daemon.connection.execute(
                "SELECT due_at_ms FROM enrichment_jobs"
            ).fetchone()[0]
            self.assertEqual(NOW + 8 * 60_000, due_at_ms)
            daemon.close()

    def test_retry_spacing_uses_provider_completion_time(self):
        with tempfile.TemporaryDirectory() as temp:
            daemon = self.make_daemon(
                Path(temp),
                bitquery_enabled=False,
                native_rpc_mode="primary",
            )
            log = self.native_log()
            payload = {"chain": "base", "log": log}
            batch = parse_native_launch(
                log,
                NATIVE_SPECS["base"],
                published_at_ms=NOW,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
                name="Meme",
                symbol="MEME",
            )
            daemon.process_batch(
                source="native_rpc",
                batch_id="slow-provider",
                payload=payload,
                batch=batch,
                observed_at_ms=NOW + 100,
                received_at_ms=NOW + 100,
            )

            class Dex:
                def token_market(inner, chain, token_address):
                    return DaemonTests.market(
                        NOW + 7 * 60_000 + 15_000,
                        pair_count=0,
                        chain=chain,
                    )

            daemon.dex = Dex()
            job = claim_due_enrichment(
                daemon.connection,
                now_ms=NOW + 7 * 60_000,
            )
            with patch("meme_radar.daemon.time.time", return_value=(NOW + 7 * 60_000) / 1000):
                asyncio.get_event_loop().run_until_complete(
                    daemon.process_market_stage(job)
                )
            due_at_ms = daemon.connection.execute(
                "SELECT due_at_ms FROM enrichment_jobs"
            ).fetchone()[0]
            self.assertEqual(NOW + 8 * 60_000 + 15_000, due_at_ms)
            daemon.close()


if __name__ == "__main__":
    unittest.main()
