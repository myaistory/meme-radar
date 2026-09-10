import asyncio
import json
import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from meme_radar.normalize import build_event
from meme_radar.pons_curve import PonsCurveTrade, PonsLaunch
from meme_radar.pons_curve_notifier import (
    PonsCurveNotifier,
    _official_address,
    _quote_meta,
    format_curve_sample,
)
from meme_radar.pons_curve_notify_storage import (
    claim_notification,
    connect_notifier,
    initialize_notifier,
    notifier_counts,
)
from meme_radar.pons_curve_storage import (
    connect_shadow,
    initialize_shadow,
    set_state,
    store_launch,
    store_trade,
)
from meme_radar.storage import (
    connect,
    initialize,
    store_event,
    store_raw_payload,
)


NOW = 1_788_600_000_000
TOKEN = "0x1111111111111111111111111111111111111111"
CURVE = "0x2222222222222222222222222222222222222222"
CREATOR = "0x3333333333333333333333333333333333333333"
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"


def transaction_hash(number):
    return "0x%064x" % number


def launch():
    return PonsLaunch(
        token_address=TOKEN,
        curve_address=CURVE,
        creator_address=CREATOR,
        pair_token_address=USDG,
        launch_config_id=0,
        graduation_threshold_raw=8_090_000_000,
        block_number=10,
        block_hash=transaction_hash(100),
        transaction_hash=transaction_hash(101),
        log_index=1,
        block_timestamp_ms=NOW - 310_000,
        observed_at_ms=NOW - 309_000,
    )


def trade(number, *, kind="buy", creator_initial=False, direct_creator=False):
    trader = (
        "0xe33e9e479df8802cb0866d5d05258bec4cf62948"
        if creator_initial
        else CREATOR if direct_creator else "0x%040x" % (500 + number % 5)
    )
    return PonsCurveTrade(
        token_address=TOKEN,
        curve_address=CURVE,
        event_kind=kind,
        trader_address=trader,
        recipient_address=CREATOR if creator_initial or direct_creator else trader,
        quote_amount_raw=100_000_000 if kind == "buy" else 50_000_000,
        token_amount_raw=10**18,
        fee_raw=1_000_000,
        tax_raw=0,
        block_number=11 + number,
        block_hash=transaction_hash(200 + number),
        transaction_hash=(
            transaction_hash(101)
            if creator_initial
            else transaction_hash(300 + number)
        ),
        log_index=2 + number,
        block_timestamp_ms=NOW - 250_000 + number,
        observed_at_ms=NOW - 249_000 + number,
        is_creator_initial=creator_initial,
    )


class FakeRpc:
    def __init__(self, *, balance=0, supply=10**27, decimals=18):
        self.balance = balance
        self.supply = supply
        self.decimals = decimals

    def call(self, method, params):
        data = params[0]["data"]
        if data == "0x18160ddd":
            value = self.supply
        elif data == "0x313ce567":
            value = self.decimals
        elif data.startswith("0x70a08231"):
            value = self.balance
        else:
            raise AssertionError("unexpected token call")
        return hex(value)


class FakeDex:
    def __init__(self, price=1.0):
        self.price = price

    def token_market(self, chain, token):
        return SimpleNamespace(pair_count=1, price_usd=self.price)


class FakeGmgn:
    def __init__(self, *, market_cap=200_000.0, holders=101):
        self.market_cap = market_cap
        self.holders = holders
        self.calls = 0

    def has_fresh_cache(self, chain, token):
        return True

    def token_info(self, chain, token):
        self.calls += 1
        return SimpleNamespace(
            chain=chain,
            token_address=token,
            observed_at_ms=NOW,
            market_cap_usd=self.market_cap,
            holder_count=self.holders,
        )


class PonsCurveNotifierTests(unittest.TestCase):
    def setUp(self):
        try:
            self.previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            self.previous_loop = None
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.shadow_path = root / "shadow.db"
        self.main_path = root / "main.db"
        self.notifier_path = root / "notifier.db"
        self.health_path = root / "health.json"
        self.shadow_health_path = root / "shadow-health.json"
        self.env_path = root / "radar.env"
        self.env_path.write_text("", encoding="utf-8")
        self.env_path.chmod(0o600)
        shadow = connect_shadow(self.shadow_path)
        initialize_shadow(shadow)
        store_launch(shadow, launch())
        for number in range(10):
            store_trade(shadow, trade(number))
        store_trade(shadow, trade(10, kind="sell"))
        set_state(shadow, "curve_event_time_watermark_ms", NOW, NOW)
        set_state(shadow, "curve_coverage_start_ms", NOW - 600_000, NOW)
        shadow.close()
        self.shadow_health_path.write_text(
            json.dumps(
                {
                    "status": "running",
                    "connected": True,
                    "updated_at_ms": NOW,
                }
            ),
            encoding="utf-8",
        )
        main = connect(self.main_path)
        initialize(main)
        digest = store_raw_payload(
            main,
            source="fixture",
            batch_id="launch",
            observed_at_ms=NOW - 119_000,
            received_at_ms=NOW - 119_000,
            is_backfill=False,
            payload={"fixture": True},
        )
        event = build_event(
            source="native_rpc",
            source_event_id="fixture-launch",
            event_kind="launch",
            chain="robinhood",
            token_address=TOKEN,
            source_published_at_ms=NOW - 310_000,
            observed_at_ms=NOW - 309_000,
            received_at_ms=NOW - 309_000,
            launchpad="pons_v2_event",
            token_created_at_ms=NOW - 310_000,
            name="Curve Cat",
            symbol="CCAT",
            creator=CREATOR,
            raw_sha256=digest,
        )
        store_event(main, event)
        cross_chain = build_event(
            source="native_rpc",
            source_event_id="fixture-cross-chain",
            event_kind="launch",
            chain="bsc",
            token_address="0x7777777777777777777777777777777777777777",
            source_published_at_ms=NOW - 100_000,
            observed_at_ms=NOW - 99_000,
            received_at_ms=NOW - 99_000,
            launchpad="four_meme",
            token_created_at_ms=NOW - 100_000,
            name="Curve Cat",
            symbol="CCAT",
            creator="0x8888888888888888888888888888888888888888",
            raw_sha256=digest,
        )
        store_event(main, cross_chain)
        main.close()

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(self.previous_loop)
        self.temp.cleanup()

    def notifier(self, *, rpc=None, dex=None, gmgn=None):
        with patch.dict(
            "os.environ",
            {
                "MEME_RADAR_PONS_CURVE_TELEGRAM_ENABLED": "0",
                "MEME_RADAR_PONS_CURVE_MAX_PER_HOUR": "2",
                "MEME_RADAR_PONS_CURVE_POLL_SECONDS": "10",
            },
            clear=False,
        ):
            with patch(
                "meme_radar.pons_curve_notifier.time.time",
                return_value=NOW / 1000,
            ):
                return PonsCurveNotifier(
                env_file=self.env_path,
                shadow_db=self.shadow_path,
                shadow_health=self.shadow_health_path,
                main_db=self.main_path,
                    db_path=self.notifier_path,
                    health_path=self.health_path,
                    dry_run=True,
                    activation_lookback_seconds=600,
                    rpc_client=rpc or FakeRpc(),
                    dex_source=dex or FakeDex(),
                    gmgn_source=gmgn or FakeGmgn(),
                )

    def test_gate_uses_external_buyers_and_formats_real_activity(self):
        notifier = self.notifier()
        try:
            candidates = notifier.candidates(NOW)
            self.assertEqual(1, len(candidates))
            candidate = candidates[0]
            self.assertEqual(10, candidate.external_buys_5m)
            self.assertEqual(5, candidate.unique_buyers_5m)
            self.assertFalse(candidate.creator_initial)
            message = format_curve_sample(candidate, NOW)
            self.assertIn("10 买 / 1 卖", message)
            self.assertIn("净流入", message)
            self.assertIn("Dev 买入 0 笔（已排除）", message)
            self.assertIn("规模门", message)
            self.assertIn("持有人 101（>100）", message)
            self.assertIn("<b>身份</b>  未验证", message)
            self.assertIn("<code>" + TOKEN + "</code>", message)
            self.assertNotIn("0/0", message)
        finally:
            notifier.close()

    def test_html_message_escapes_untrusted_metadata(self):
        notifier = self.notifier()
        try:
            candidate = notifier.candidates(NOW)[0]
            message = format_curve_sample(
                replace(candidate, name="Cat <script>&", symbol="C&T"),
                NOW,
            )
            self.assertIn("Cat &lt;script&gt;&amp;", message)
            self.assertIn("C&amp;T", message)
            self.assertNotIn("<script>", message)
        finally:
            notifier.close()

    def test_dry_run_persists_without_external_send(self):
        notifier = self.notifier()
        try:
            notifier.enqueue_best(NOW)
            self.loop.run_until_complete(notifier.deliver_one(NOW))
            self.assertEqual(
                {"pending": 0, "sent": 1},
                notifier_counts(notifier.connection),
            )
            self.assertEqual(1, notifier.counters["dry_run_sent"])
        finally:
            notifier.close()

    def test_size_checks_run_at_five_ten_twenty_thirty_only(self):
        gmgn = FakeGmgn()
        notifier = self.notifier(gmgn=gmgn)
        try:
            notifier._collector_ready = lambda _now_ms: True
            notifier._narrative = lambda _name, _now_ms: (3, 2)
            self.assertEqual(1, len(notifier.candidates(NOW)))
            self.assertEqual([], notifier.candidates(NOW + 60_000))
            self.assertEqual(1, len(notifier.candidates(NOW + 5 * 60_000)))
            self.assertEqual(1, len(notifier.candidates(NOW + 15 * 60_000)))
            self.assertEqual(1, len(notifier.candidates(NOW + 25 * 60_000)))
            self.assertEqual([], notifier.candidates(NOW + 26 * 60_000))
            self.assertEqual(4, gmgn.calls)
            self.assertEqual(1, notifier.counters["size_check_5m"])
            self.assertEqual(1, notifier.counters["size_check_10m"])
            self.assertEqual(1, notifier.counters["size_check_20m"])
            self.assertEqual(1, notifier.counters["size_check_30m"])
        finally:
            notifier.close()

    def test_nine_external_buys_do_not_pass(self):
        shadow = sqlite3.connect(str(self.shadow_path))
        shadow.execute(
            "DELETE FROM pons_curve_trades WHERE transaction_hash=?",
            (transaction_hash(309),),
        )
        shadow.commit()
        shadow.close()
        notifier = self.notifier()
        try:
            self.assertEqual([], notifier.candidates(NOW))
        finally:
            notifier.close()

    def test_direct_creator_buys_are_not_external_demand(self):
        shadow = sqlite3.connect(str(self.shadow_path))
        rows = shadow.execute(
            "SELECT transaction_hash,log_index FROM pons_curve_trades "
            "WHERE event_kind='buy' AND is_creator_initial=0 "
            "ORDER BY block_number LIMIT 7"
        ).fetchall()
        for transaction, index in rows:
            shadow.execute(
                "UPDATE pons_curve_trades SET trader_address=?,recipient_address=? "
                "WHERE transaction_hash=? AND log_index=?",
                (CREATOR, CREATOR, transaction, index),
            )
        shadow.commit()
        shadow.close()
        notifier = self.notifier(rpc=FakeRpc(balance=8 * 10**18))
        try:
            self.assertEqual([], notifier.candidates(NOW))
        finally:
            notifier.close()

    def test_creator_balance_reduction_is_not_blocked(self):
        shadow = connect_shadow(self.shadow_path)
        store_trade(shadow, trade(20, creator_initial=True))
        shadow.close()
        notifier = self.notifier(rpc=FakeRpc(balance=0))
        try:
            self.assertEqual(1, len(notifier.candidates(NOW)))
            self.assertEqual(0, notifier.counters["creator_balance_reduced"])
        finally:
            notifier.close()

    def test_creator_full_retention_is_blocked(self):
        shadow = connect_shadow(self.shadow_path)
        store_trade(shadow, trade(20, creator_initial=True))
        shadow.close()
        notifier = self.notifier(rpc=FakeRpc(balance=10**18))
        try:
            self.assertEqual([], notifier.candidates(NOW))
            self.assertGreater(
                notifier.counters["creator_full_retention_blocked"], 0
            )
        finally:
            notifier.close()

    def test_direct_creator_sell_is_not_blocked(self):
        shadow = connect_shadow(self.shadow_path)
        store_trade(shadow, trade(20, creator_initial=True))
        item = trade(30, kind="sell", direct_creator=True)
        store_trade(shadow, item)
        shadow.close()
        notifier = self.notifier(rpc=FakeRpc(balance=0))
        try:
            self.assertEqual(1, len(notifier.candidates(NOW)))
            self.assertEqual(0, notifier.counters["creator_sell_blocked"])
        finally:
            notifier.close()

    def test_market_cap_must_be_strictly_above_100k(self):
        notifier = self.notifier(gmgn=FakeGmgn(market_cap=100_000.0))
        try:
            self.assertEqual([], notifier.candidates(NOW))
            self.assertGreater(
                notifier.counters["size_market_cap_not_above_min"], 0
            )
        finally:
            notifier.close()

    def test_holder_count_must_be_strictly_above_100(self):
        notifier = self.notifier(gmgn=FakeGmgn(holders=100))
        try:
            self.assertEqual([], notifier.candidates(NOW))
            self.assertGreater(
                notifier.counters["size_holder_count_not_above_min"], 0
            )
        finally:
            notifier.close()

    def test_same_name_candidates_keep_highest_market_cap(self):
        notifier = self.notifier()
        try:
            base = notifier.candidates(NOW)[0]
            lower = replace(
                base,
                token_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                same_name_contracts=2,
                market_cap_usd=30_000,
                holder_count=101,
            )
            higher = replace(
                base,
                token_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                same_name_contracts=2,
                market_cap_usd=50_000,
                holder_count=101,
            )
            notifier._market_rows = lambda now_ms: [lower, higher]
            notifier._candidate = lambda row, now_ms: row
            candidates = notifier.candidates(NOW)
            self.assertEqual([higher.token_address], [item.token_address for item in candidates])
            self.assertEqual("同名最高市值", candidates[0].identity_status)
        finally:
            notifier.close()

    def test_official_tokenized_stock_quote_is_named(self):
        self.assertEqual(
            ("TSLA", 18),
            _quote_meta("0x322f0929c4625ed5bad873c95208d54e1c003b2d"),
        )

    def test_owner_confirmed_symbol_blocks_same_name_copy(self):
        main = sqlite3.connect(str(self.main_path))
        row = main.execute(
            "SELECT event_id,event_json FROM radar_events LIMIT 1"
        ).fetchone()
        payload = json.loads(row[1])
        payload["name"] = "pipt"
        payload["symbol"] = "PIPT"
        main.execute(
            "UPDATE radar_events SET event_json=? WHERE event_id=?",
            (json.dumps(payload, sort_keys=True, separators=(",", ":")), row[0]),
        )
        main.commit()
        main.close()
        notifier = self.notifier()
        try:
            self.assertEqual([], notifier.candidates(NOW))
            self.assertGreater(notifier.counters["owner_ca_mismatch"], 0)
            self.assertEqual(
                "0x5f7fd169b227873c051da44886fc51f82932ce04",
                _official_address("pipt", "PIPT"),
            )
        finally:
            notifier.close()

    def test_incomplete_curve_watermark_blocks_candidate(self):
        shadow = sqlite3.connect(str(self.shadow_path))
        shadow.execute(
            """
            UPDATE pons_curve_shadow_state SET value_json=?
            WHERE key='curve_event_time_watermark_ms'
            """,
            (str(NOW - 200_000),),
        )
        shadow.commit()
        shadow.close()
        notifier = self.notifier()
        try:
            self.assertEqual([], notifier.candidates(NOW))
        finally:
            notifier.close()

    def test_narrative_is_a_hard_gate(self):
        main = sqlite3.connect(str(self.main_path))
        main.execute("DELETE FROM radar_events WHERE chain='bsc'")
        main.commit()
        main.close()
        notifier = self.notifier()
        try:
            self.assertEqual([], notifier.candidates(NOW))
            self.assertGreater(notifier.counters["narrative_below_gate"], 0)
        finally:
            notifier.close()

    def test_low_net_inflow_ratio_is_rejected(self):
        shadow = sqlite3.connect(str(self.shadow_path))
        shadow.execute(
            """
            UPDATE pons_curve_trades SET quote_amount_raw=?
            WHERE event_kind='sell'
            """,
            ("800000000",),
        )
        shadow.commit()
        shadow.close()
        notifier = self.notifier()
        try:
            self.assertEqual([], notifier.candidates(NOW))
        finally:
            notifier.close()


class PonsCurveNotifyStorageTests(unittest.TestCase):
    def test_claim_is_deduplicated_and_hourly_limited(self):
        with tempfile.TemporaryDirectory() as temp:
            connection = connect_notifier(Path(temp) / "notifier.db")
            initialize_notifier(connection)
            self.assertEqual(
                "enqueued",
                claim_notification(
                    connection,
                    token_address=TOKEN,
                    message_text="sample",
                    now_ms=NOW,
                    max_per_hour=1,
                ),
            )
            self.assertEqual(
                "deduplicated",
                claim_notification(
                    connection,
                    token_address=TOKEN,
                    message_text="sample",
                    now_ms=NOW,
                    max_per_hour=1,
                ),
            )
            self.assertEqual(
                "rate_limited",
                claim_notification(
                    connection,
                    token_address="0x9999999999999999999999999999999999999999",
                    message_text="sample",
                    now_ms=NOW,
                    max_per_hour=1,
                ),
            )
            connection.close()
if __name__ == "__main__":
    unittest.main()
