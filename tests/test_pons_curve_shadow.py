import asyncio
import tempfile
import unittest
from collections import Counter, OrderedDict
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

from meme_radar.evm_rpc import PONS_TOKEN_LAUNCHED_TOPIC, RpcError
from meme_radar.launchpads import PONS_V2_FACTORY, PONS_V2_ROUTER
from meme_radar.pons_curve import (
    PONS_CURVE_BUY_TOPIC,
    PONS_CURVE_COMPLETED_TOPIC,
    PONS_CURVE_SELL_TOPIC,
    PONS_POOL_GRADUATED_TOPIC,
    parse_pons_launch,
    parse_pons_lifecycle,
    parse_pons_trade,
)
from meme_radar.pons_curve_shadow import (
    BACKFILL_CHUNK_BLOCKS,
    PONS_WSS_MAX_QUEUE,
    REGISTRY_RETENTION_MS,
    PonsCurveShadow,
    _history_backfill_can_fallback,
    _rpc_logs,
)
from meme_radar.pons_curve_storage import (
    connect_shadow,
    get_state,
    initialize_shadow,
    load_launches,
    set_state,
    shadow_counts,
    store_launch,
    store_lifecycle,
    store_trade,
)


NOW = 1_788_600_000_000
TOKEN = "0x1111111111111111111111111111111111111111"
CURVE = "0x2222222222222222222222222222222222222222"
CREATOR = "0x3333333333333333333333333333333333333333"
PAIR = "0x4444444444444444444444444444444444444444"


def topic_address(address):
    return "0x" + "0" * 24 + address[2:]


def word(number):
    return "%064x" % number


def base_log(address, topics, data, *, tx="b", index=2, removed=False):
    return {
        "address": address,
        "topics": topics,
        "data": "0x" + data,
        "blockNumber": "0x10",
        "blockHash": "0x" + "a" * 64,
        "transactionHash": "0x" + tx * 64,
        "logIndex": hex(index),
        "removed": removed,
    }


def launch_log(removed=False):
    return base_log(
        PONS_V2_FACTORY,
        [
            PONS_TOKEN_LAUNCHED_TOPIC,
            topic_address(TOKEN),
            topic_address(CURVE),
            topic_address(CREATOR),
        ],
        topic_address(PAIR)[2:] + word(7) + word(8_090_000_000),
        removed=removed,
    )


def trade_log(topic, *, router=False, removed=False):
    trader = PONS_V2_ROUTER if router else "0x5555555555555555555555555555555555555555"
    return base_log(
        CURVE,
        [topic, topic_address(trader), topic_address(CREATOR)],
        word(100) + word(200) + word(3) + word(4),
        removed=removed,
    )


class PonsCurveParserTests(unittest.TestCase):
    def setUp(self):
        self.launch = parse_pons_launch(
            launch_log(), block_timestamp_ms=NOW, observed_at_ms=NOW + 10
        )

    def test_launch_decodes_curve_pair_and_threshold(self):
        self.assertEqual(TOKEN, self.launch.token_address)
        self.assertEqual(CURVE, self.launch.curve_address)
        self.assertEqual(CREATOR, self.launch.creator_address)
        self.assertEqual(PAIR, self.launch.pair_token_address)
        self.assertEqual(7, self.launch.launch_config_id)
        self.assertEqual(8_090_000_000, self.launch.graduation_threshold_raw)

    def test_buy_and_sell_use_protocol_amount_order(self):
        buy = parse_pons_trade(
            trade_log(PONS_CURVE_BUY_TOPIC, router=True),
            self.launch,
            block_timestamp_ms=NOW,
            observed_at_ms=NOW + 10,
        )
        sell = parse_pons_trade(
            trade_log(PONS_CURVE_SELL_TOPIC),
            self.launch,
            block_timestamp_ms=NOW,
            observed_at_ms=NOW + 10,
        )
        self.assertEqual(("buy", 100, 200), (buy.event_kind, buy.quote_amount_raw, buy.token_amount_raw))
        self.assertEqual(("sell", 200, 100), (sell.event_kind, sell.quote_amount_raw, sell.token_amount_raw))
        self.assertTrue(buy.is_creator_initial)
        self.assertFalse(sell.is_creator_initial)

    def test_unregistered_curve_fails_closed(self):
        bad = dict(trade_log(PONS_CURVE_BUY_TOPIC))
        bad["address"] = "0x6666666666666666666666666666666666666666"
        with self.assertRaisesRegex(ValueError, "unregistered"):
            parse_pons_trade(
                bad,
                self.launch,
                block_timestamp_ms=NOW,
                observed_at_ms=NOW,
            )

    def test_lifecycle_requires_registered_factory_or_curve(self):
        completed_log = base_log(
            CURVE,
            [PONS_CURVE_COMPLETED_TOPIC],
            topic_address(CREATOR)[2:] + word(12) + word(34),
        )
        completed = parse_pons_lifecycle(
            completed_log,
            {CURVE: self.launch},
            {TOKEN: self.launch},
            block_timestamp_ms=NOW,
            observed_at_ms=NOW + 10,
        )
        graduated_log = base_log(
            PONS_V2_FACTORY,
            [PONS_POOL_GRADUATED_TOPIC, topic_address(TOKEN)],
            word(9) + word(1000) + word(500),
            index=3,
        )
        graduated = parse_pons_lifecycle(
            graduated_log,
            {CURVE: self.launch},
            {TOKEN: self.launch},
            block_timestamp_ms=NOW,
            observed_at_ms=NOW + 10,
        )
        self.assertEqual(("curve_completed", 12, 34), (completed.event_kind, completed.quote_amount_raw, completed.token_amount_raw))
        self.assertEqual(("pool_graduated", 500, 1000), (graduated.event_kind, graduated.quote_amount_raw, graduated.token_amount_raw))

    def test_rpc_log_range_is_hard_limited_before_network(self):
        with self.assertRaisesRegex(ValueError, "range"):
            _rpc_logs(
                object(),
                from_block=1,
                to_block=11,
                topics=(PONS_CURVE_BUY_TOPIC,),
            )


class PonsCurveStorageTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "shadow.db"
        self.connection = connect_shadow(self.path)
        initialize_shadow(self.connection)
        self.launch = parse_pons_launch(
            launch_log(), block_timestamp_ms=NOW, observed_at_ms=NOW + 10
        )

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def test_schema_and_file_mode_are_isolated(self):
        initialize_shadow(self.connection)
        self.assertEqual(1, self.connection.execute("PRAGMA user_version").fetchone()[0])
        self.assertEqual(0o600, self.path.stat().st_mode & 0o777)
        tables = {
            row[0]
            for row in self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        self.assertNotIn("telegram_outbox", tables)
        self.assertIn("pons_curve_trades", tables)

    def test_launch_trade_lifecycle_are_idempotent(self):
        self.assertTrue(store_launch(self.connection, self.launch))
        self.assertFalse(store_launch(self.connection, self.launch))
        buy = parse_pons_trade(
            trade_log(PONS_CURVE_BUY_TOPIC, router=True),
            self.launch,
            block_timestamp_ms=NOW,
            observed_at_ms=NOW + 20,
        )
        self.assertTrue(store_trade(self.connection, buy))
        self.assertFalse(store_trade(self.connection, buy))
        completed = parse_pons_lifecycle(
            base_log(
                CURVE,
                [PONS_CURVE_COMPLETED_TOPIC],
                topic_address(CREATOR)[2:] + word(12) + word(34),
                index=4,
            ),
            {CURVE: self.launch},
            {TOKEN: self.launch},
            block_timestamp_ms=NOW,
            observed_at_ms=NOW + 30,
        )
        self.assertTrue(store_lifecycle(self.connection, completed))
        self.assertFalse(store_lifecycle(self.connection, completed))
        self.assertEqual(
            {
                "launches": 1,
                "buys": 1,
                "sells": 0,
                "creator_initial_buys": 1,
                "creator_buys": 1,
                "external_buys": 0,
                "curve_completed": 1,
                "pool_graduated": 0,
            },
            shadow_counts(self.connection),
        )

    def test_reorg_marks_rows_removed_without_deleting_evidence(self):
        store_launch(self.connection, self.launch)
        buy = parse_pons_trade(
            trade_log(PONS_CURVE_BUY_TOPIC, router=True),
            self.launch,
            block_timestamp_ms=NOW,
            observed_at_ms=NOW + 20,
        )
        store_trade(self.connection, buy)
        self.assertFalse(
            store_trade(
                self.connection,
                replace(buy, observed_at_ms=NOW + 40, removed=True),
            )
        )
        self.assertFalse(
            store_trade(
                self.connection,
                replace(buy, observed_at_ms=NOW + 30, removed=False),
            )
        )
        row = self.connection.execute(
            "SELECT removed,observed_at_ms FROM pons_curve_trades"
        ).fetchone()
        self.assertEqual((1, NOW + 40), tuple(row))
        self.assertEqual(0, shadow_counts(self.connection)["buys"])

    def test_conflicting_identity_is_rejected(self):
        store_launch(self.connection, self.launch)
        with self.assertRaisesRegex(RuntimeError, "conflicting"):
            store_launch(
                self.connection,
                replace(self.launch, graduation_threshold_raw=1),
            )

    def test_seed_can_correct_only_a_provisional_launch_timestamp(self):
        provisional = replace(self.launch, block_timestamp_ms=NOW + 10_000)
        store_launch(self.connection, provisional)
        self.assertFalse(
            store_launch(
                self.connection,
                self.launch,
                allow_timestamp_correction=True,
            )
        )
        timestamp = self.connection.execute(
            "SELECT block_timestamp_ms FROM pons_curve_launches"
        ).fetchone()[0]
        self.assertEqual(NOW, timestamp)
        with self.assertRaisesRegex(RuntimeError, "conflicting"):
            store_launch(
                self.connection,
                replace(self.launch, graduation_threshold_raw=1),
                allow_timestamp_correction=True,
            )

    def test_checkpoint_round_trip(self):
        set_state(self.connection, "checkpoint", {"block": 7}, NOW)
        self.assertEqual(
            {"block": 7}, get_state(self.connection, "checkpoint")
        )
        self.assertTrue(store_launch(self.connection, self.launch))
        self.assertEqual(1, len(list(load_launches(self.connection))))

    def test_launch_registry_loader_can_ignore_old_history(self):
        store_launch(self.connection, self.launch)
        launches = list(
            load_launches(
                self.connection,
                min_block_timestamp_ms=NOW + 1,
            )
        )
        self.assertEqual([], launches)


class PonsCurveRpcTests(unittest.TestCase):
    def test_backfill_chunk_matches_chainstack_limit(self):
        self.assertEqual(5, BACKFILL_CHUNK_BLOCKS)

    def test_wss_queue_is_bounded_below_service_memory_limit(self):
        self.assertEqual(128, PONS_WSS_MAX_QUEUE)

    def test_large_batch_yields_every_twenty_logs(self):
        daemon = PonsCurveShadow.__new__(PonsCurveShadow)
        daemon.prime_block_times = AsyncMock()
        daemon.process_log = AsyncMock()
        daemon.block_times = OrderedDict()
        daemon.counters = Counter()
        logs = [launch_log() for _ in range(40)]
        with patch(
            "meme_radar.pons_curve_shadow.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep:
            asyncio.get_event_loop().run_until_complete(
                daemon.process_batch(logs, backfill=False)
            )
        self.assertEqual(2, sleep.await_count)

    def test_health_payload_uses_cached_counts(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "shadow.db"
            connection = connect_shadow(path)
            initialize_shadow(connection)
            daemon = PonsCurveShadow.__new__(PonsCurveShadow)
            daemon.connection = connection
            daemon.started_at_ms = NOW
            daemon.status = "running"
            daemon.connected = True
            daemon.source_mode = "wss"
            daemon.endpoints = (("primary", "https://rpc.test", "wss://rpc.test"),)
            daemon.endpoint_index = 0
            daemon.factory_backfill_supported = True
            daemon.curve_backfill_supported = True
            daemon.counts_cache = {"buys": 7}
            daemon.counters = Counter()
            daemon.last_success = {}
            daemon.last_error = {}
            with patch(
                "meme_radar.pons_curve_shadow.shadow_counts",
                side_effect=AssertionError("health must not scan tables"),
            ):
                payload = daemon.health_payload()
            self.assertEqual({"buys": 7}, payload["counts"])
            connection.close()

    def test_public_poll_advances_checkpoint_with_one_combined_log_call(self):
        class Client:
            def block_number(self):
                return 123

        with tempfile.TemporaryDirectory() as temp:
            connection = connect_shadow(Path(temp) / "shadow.db")
            initialize_shadow(connection)
            set_state(connection, "robinhood_block", 120, NOW)
            shadow = PonsCurveShadow.__new__(PonsCurveShadow)
            shadow.poll_client = Client()
            shadow.connection = connection
            shadow.process_batch = AsyncMock()
            shadow._advance_curve_watermark = unittest.mock.Mock()
            shadow.counters = Counter()
            shadow.last_success = {}
            shadow.connected = False
            with patch(
                "meme_radar.pons_curve_shadow._public_poll_logs",
                return_value=[launch_log()],
            ) as poll_logs, patch(
                "meme_radar.pons_curve_shadow.time.time",
                return_value=NOW / 1000,
            ):
                asyncio.get_event_loop().run_until_complete(shadow.poll_once())
            self.assertEqual(118, poll_logs.call_args.kwargs["from_block"])
            self.assertEqual(123, poll_logs.call_args.kwargs["to_block"])
            shadow.process_batch.assert_awaited_once_with(
                [launch_log()], backfill=False
            )
            self.assertEqual(123, get_state(connection, "robinhood_block"))
            self.assertTrue(shadow.connected)
            self.assertEqual(1, shadow.counters["poll_batches"])
            connection.close()

    def test_only_provider_capability_errors_disable_curve_backfill(self):
        self.assertTrue(
            _history_backfill_can_fallback(RpcError("RPC_RESPONSE_ERROR"))
        )
        self.assertTrue(
            _history_backfill_can_fallback(RpcError("HTTP_STATUS", 403))
        )
        self.assertFalse(
            _history_backfill_can_fallback(RpcError("HTTP_STATUS", 429))
        )

    def test_block_timestamp_retries_head_skew(self):
        class Client:
            batch_calls = 0
            single_calls = 0

            def block_timestamps_ms(inner, block_numbers):
                inner.batch_calls += 1
                raise RpcError("RPC_BATCH_ERROR")

            def block_timestamp_ms(inner, block_number):
                inner.single_calls += 1
                if inner.single_calls == 1:
                    raise RpcError("RPC_RESPONSE_ERROR")
                if inner.single_calls == 2:
                    raise RpcError("BLOCK_MISSING")
                return NOW

        shadow = PonsCurveShadow.__new__(PonsCurveShadow)
        shadow.clients = [Client()]
        shadow.endpoint_index = 0
        shadow.block_times = OrderedDict()
        shadow.counters = Counter()
        with patch(
            "meme_radar.pons_curve_shadow.asyncio.sleep",
            new=AsyncMock(),
        ):
            result = asyncio.get_event_loop().run_until_complete(
                shadow.block_timestamp_ms(16)
            )
        self.assertEqual(NOW, result)
        self.assertEqual(1, shadow.client.batch_calls)
        self.assertEqual(3, shadow.client.single_calls)
        self.assertEqual(1, shadow.counters["block_timestamp_batch_fallbacks"])
        self.assertEqual(2, shadow.counters["block_timestamp_retries"])

    def test_http_403_batch_falls_back_once_to_single_blocks(self):
        class Client:
            batch_calls = 0
            single_calls = 0

            def block_timestamps_ms(inner, _block_numbers):
                inner.batch_calls += 1
                raise RpcError("HTTP_STATUS", 403)

            def block_timestamp_ms(inner, block_number):
                inner.single_calls += 1
                return NOW + block_number

        shadow = PonsCurveShadow.__new__(PonsCurveShadow)
        shadow.clients = [Client()]
        shadow.endpoint_index = 0
        shadow.block_times = OrderedDict()
        shadow.counters = Counter()
        shadow.block_timestamp_batch_supported = True
        first = asyncio.get_event_loop().run_until_complete(
            shadow.fetch_block_times([16, 17])
        )
        second = asyncio.get_event_loop().run_until_complete(
            shadow.fetch_block_times([18])
        )
        self.assertEqual({16: NOW + 16, 17: NOW + 17}, first)
        self.assertEqual({18: NOW + 18}, second)
        self.assertEqual(1, shadow.client.batch_calls)
        self.assertEqual(3, shadow.client.single_calls)
        self.assertFalse(shadow.block_timestamp_batch_supported)

    def test_unsupported_history_jumps_to_latest_checkpoint(self):
        shadow = PonsCurveShadow.__new__(PonsCurveShadow)
        shadow.connection = object()
        shadow.counters = Counter()
        shadow.last_success = {}
        shadow._advance_curve_watermark = unittest.mock.Mock()
        with patch("meme_radar.pons_curve_shadow.time.time", return_value=NOW / 1000), patch(
            "meme_radar.pons_curve_shadow.set_state"
        ) as set_state_mock:
            asyncio.get_event_loop().run_until_complete(
                shadow.skip_unsupported_backfill(123)
            )
        shadow._advance_curve_watermark.assert_called_once_with(NOW)
        self.assertEqual(123, set_state_mock.call_args.args[2])
        self.assertEqual(1, shadow.counters["backfill_unavailable_skips"])

    def test_disabled_history_does_not_gate_wss_on_http(self):
        shadow = PonsCurveShadow.__new__(PonsCurveShadow)
        shadow.factory_backfill_supported = False
        shadow.curve_backfill_supported = False
        shadow.skip_unsupported_backfill = AsyncMock()
        shadow.clients = [unittest.mock.Mock()]
        shadow.endpoint_index = 0
        asyncio.get_event_loop().run_until_complete(shadow.backfill())
        shadow.skip_unsupported_backfill.assert_awaited_once_with()
        shadow.client.chain_id.assert_not_called()
        shadow.client.block_number.assert_not_called()

    def test_one_provider_history_limit_disables_both_log_paths(self):
        class Client:
            def chain_id(self):
                return 4663

            def block_number(self):
                return 123

        with tempfile.TemporaryDirectory() as temp:
            connection = connect_shadow(Path(temp) / "shadow.db")
            initialize_shadow(connection)
            shadow = PonsCurveShadow.__new__(PonsCurveShadow)
            shadow.clients = [Client()]
            shadow.endpoint_index = 0
            shadow.factory_backfill_supported = True
            shadow.curve_backfill_supported = True
            shadow.backfill_blocks = 100
            shadow.stop_event = asyncio.Event()
            shadow.connection = connection
            shadow.counters = Counter()
            shadow.last_success = {}
            shadow._advance_curve_watermark = unittest.mock.Mock()
            with patch(
                "meme_radar.pons_curve_shadow._rpc_logs",
                side_effect=RpcError("HTTP_STATUS", 403),
            ) as rpc_logs:
                asyncio.get_event_loop().run_until_complete(shadow.backfill())
            self.assertEqual(1, rpc_logs.call_count)
            self.assertFalse(shadow.factory_backfill_supported)
            self.assertFalse(shadow.curve_backfill_supported)
            self.assertEqual(1, shadow.counters["historical_backfill_disabled"])
            self.assertEqual(123, get_state(connection, "robinhood_block"))
            connection.close()

    def test_live_batch_uses_observed_time_without_http_timestamp_call(self):
        shadow = PonsCurveShadow.__new__(PonsCurveShadow)
        shadow.prime_block_times = AsyncMock()
        shadow.process_log = AsyncMock()
        shadow.block_times = OrderedDict()
        shadow.counters = Counter()
        with patch(
            "meme_radar.pons_curve_shadow.time.time", return_value=NOW / 1000
        ):
            asyncio.get_event_loop().run_until_complete(
                shadow.process_batch([launch_log()], backfill=False)
            )
        self.assertEqual(NOW, shadow.block_times[16])
        self.assertEqual(1, shadow.counters["wss_observed_timestamp_blocks"])
        shadow.prime_block_times.assert_not_awaited()
        shadow.process_log.assert_awaited_once()

    def test_registry_prunes_launches_older_than_retention(self):
        old = parse_pons_launch(
            launch_log(), block_timestamp_ms=NOW, observed_at_ms=NOW
        )
        fresh = replace(
            old,
            token_address="0x6666666666666666666666666666666666666666",
            curve_address="0x7777777777777777777777777777777777777777",
            block_timestamp_ms=NOW + REGISTRY_RETENTION_MS,
        )
        shadow = PonsCurveShadow.__new__(PonsCurveShadow)
        shadow.launches_by_token = {
            old.token_address: old,
            fresh.token_address: fresh,
        }
        shadow.launches_by_curve = {
            old.curve_address: old,
            fresh.curve_address: fresh,
        }
        shadow.counters = Counter()
        shadow._prune_registry(NOW + REGISTRY_RETENTION_MS + 1)
        self.assertNotIn(old.token_address, shadow.launches_by_token)
        self.assertIn(fresh.token_address, shadow.launches_by_token)
        self.assertEqual(1, shadow.counters["registry_pruned"])


if __name__ == "__main__":
    unittest.main()
