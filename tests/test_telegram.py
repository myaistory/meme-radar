import unittest

from meme_radar import FeatureSnapshot, evaluate
from meme_radar.http_json import HttpJsonResult
from meme_radar.normalize import build_event
from meme_radar.sources import MarketSnapshot
from meme_radar.telegram import (
    TelegramClient,
    format_alert,
    format_observation_sample,
)


NOW = 1_788_516_000_000


class FakeTransport:
    def __init__(self):
        self.calls = []

    def post_json(self, url, payload, headers=None):
        self.calls.append((url, payload, headers))
        return HttpJsonResult(
            data={"ok": True, "result": {"message_id": 42}},
            status=200,
            bytes_read=50,
            elapsed_ms=5,
            started_at_ms=NOW,
            received_at_ms=NOW + 5,
            rate_limit={},
        )


def event_and_features():
    event = build_event(
        source="fixture",
        source_event_id="one",
        event_kind="launch",
        chain="bsc",
        token_address="0x1111111111111111111111111111111111111111",
        source_published_at_ms=NOW,
        observed_at_ms=NOW,
        received_at_ms=NOW,
        token_created_at_ms=NOW,
        launchpad="Four.Meme",
        name="Example",
        symbol="EX",
    )
    features = FeatureSnapshot(
        event_id=event.event_id,
        evaluated_at_ms=NOW + 1000,
        metadata_complete=True,
        security_status="safe",
        sell_simulation_ok=True,
        creator_risk="safe",
        price_age_ms=100,
        liquidity_usd=60_000,
        volume_24h_usd=300_000,
        buy_transactions_24h=120,
        sell_transactions_24h=50,
    )
    return event, features


class TelegramTests(unittest.TestCase):
    def test_validates_credentials_before_io(self):
        transport = FakeTransport()
        with self.assertRaisesRegex(ValueError, "bot token"):
            TelegramClient(
                transport,
                bot_token="bad/token",
                chat_id="-1001234567890",
            )
        self.assertEqual([], transport.calls)

    def test_sends_plain_text_without_exposing_token_in_payload(self):
        transport = FakeTransport()
        client = TelegramClient(
            transport,
            bot_token="123456789:" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcd",
            chat_id="-1001234567890",
        )
        result = client.send_message("hello")
        self.assertEqual(42, result.message_id)
        self.assertEqual("hello", transport.calls[0][1]["text"])
        self.assertNotIn("bot_token", transport.calls[0][1])
        self.assertNotIn("parse_mode", transport.calls[0][1])

    def test_html_mode_is_explicit_and_bounded(self):
        transport = FakeTransport()
        client = TelegramClient(
            transport,
            bot_token="123456789:" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcd",
            chat_id="-1001234567890",
            parse_mode="HTML",
        )
        client.send_message("<b>signal</b>")
        self.assertEqual("HTML", transport.calls[0][1]["parse_mode"])
        with self.assertRaisesRegex(ValueError, "parse mode"):
            TelegramClient(
                transport,
                bot_token="123456789:" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcd",
                chat_id="-1001234567890",
                parse_mode="MarkdownV2",
            )

    def test_alert_contains_quality_evidence_and_disclaimer(self):
        event, features = event_and_features()
        message = format_alert(event, evaluate(event, features), features)
        self.assertIn("Meme Radar 强信号", message)
        self.assertIn("流动性: $60.0K", message)
        self.assertIn("机会分: 65", message)
        self.assertIn("不构成投资建议", message)

    def test_sample_alert_is_unambiguously_non_actionable(self):
        event, _ = event_and_features()
        features = FeatureSnapshot(
            event_id=event.event_id,
            evaluated_at_ms=NOW + 1000,
            metadata_complete=True,
            price_age_ms=100,
            narrative_burst=5,
            cross_chain_count=2,
        )
        decision = evaluate(event, features)
        market = MarketSnapshot(
            chain="bsc",
            token_address=event.token_address,
            observed_at_ms=NOW + 1000,
            pair_count=1,
            best_pair_address="pair",
            pair_created_at_ms=NOW,
            price_usd=None,
            liquidity_usd=None,
            volume_5m_usd=0,
            volume_1h_usd=0,
            volume_24h_usd=0,
            buy_transactions_5m=0,
            sell_transactions_5m=0,
            buy_transactions_1h=0,
            sell_transactions_1h=0,
            buy_transactions_24h=0,
            sell_transactions_24h=0,
            bytes_read=100,
            elapsed_ms=10,
        )
        message = format_observation_sample(
            event,
            decision,
            features,
            market,
        )
        self.assertIn("观察样本", message)
        self.assertIn("🔴 风险缺口", message)
        self.assertIn("禁止据此交易", message)
        self.assertIn(event.token_address, message)


if __name__ == "__main__":
    unittest.main()
