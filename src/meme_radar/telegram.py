from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

from .http_json import JsonTransport
from .models import Decision, FeatureSnapshot, RadarEvent
from .sources.dexscreener import MarketSnapshot


_BOT_TOKEN = re.compile(r"^[1-9][0-9]{5,15}:[A-Za-z0-9_-]{30,80}$")
_CHAT_ID = re.compile(r"^-?[1-9][0-9]{4,20}$")


@dataclass(frozen=True)
class TelegramSendResult:
    message_id: int
    received_at_ms: int


def _money(value: Optional[float]) -> str:
    if value is None:
        return "未知"
    if value >= 1_000_000:
        return "$%.2fM" % (value / 1_000_000)
    if value >= 1_000:
        return "$%.1fK" % (value / 1_000)
    return "$%.2f" % value


def _age(event: RadarEvent, evaluated_at_ms: int) -> str:
    if event.token_created_at_ms is None:
        return "未知"
    seconds = max(0, (evaluated_at_ms - event.token_created_at_ms) // 1000)
    if seconds < 60:
        return "%ds" % seconds
    if seconds < 3600:
        return "%dm" % (seconds // 60)
    return "%.1fh" % (seconds / 3600)


def format_alert(
    event: RadarEvent,
    decision: Decision,
    features: FeatureSnapshot,
) -> str:
    if event.event_id != decision.event_id or event.event_id != features.event_id:
        raise ValueError("alert identities do not match")
    title = "%s (%s)" % (
        (event.name or "Unnamed").replace("\n", " ").replace("\r", " "),
        (event.symbol or "?").replace("\n", " ").replace("\r", " "),
    )
    reasons = ", ".join(decision.reason_codes[-5:])
    lines = [
        "🚨 Meme Radar 强信号",
        "%s · %s" % (event.chain.upper(), event.launchpad or event.source),
        title[:160],
        "合约: %s" % event.token_address,
        "年龄: %s | 市值: %s | 持有人: %s | 流动性: %s | 24h量: %s"
        % (
            _age(event, decision.evaluated_at_ms),
            _money(features.market_cap_usd),
            features.holder_count if features.holder_count is not None else "?",
            _money(features.liquidity_usd),
            _money(features.volume_24h_usd),
        ),
        "买/卖笔数: %s/%s | 叙事聚集: %d | 跨链: %d"
        % (
            features.buy_transactions_24h
            if features.buy_transactions_24h is not None
            else "?",
            features.sell_transactions_24h
            if features.sell_transactions_24h is not None
            else "?",
            features.narrative_burst,
            features.cross_chain_count,
        ),
        "风险: %s | 置信度: %d | 机会分: %d"
        % (
            decision.risk_verdict.upper(),
            decision.confidence_score,
            decision.opportunity_score,
        ),
        "依据: %s" % reasons,
        "Dev流入/流出: %s/%s | 持仓保留: %s | 同名排名: %s/%s"
        % (
            features.creator_inbound_count
            if features.creator_inbound_count is not None
            else "?",
            features.creator_outbound_count
            if features.creator_outbound_count is not None
            else "?",
            "?"
            if features.creator_retention_ratio is None
            else "%.1f%%" % (features.creator_retention_ratio * 100),
            features.identity_market_cap_rank
            if features.identity_market_cap_rank is not None
            else "?",
            features.identity_count if features.identity_count is not None else "?",
        ),
        "DexScreener: https://dexscreener.com/%s/%s"
        % (event.chain, event.token_address),
        "⚠️ 仅为监测信号，不构成投资建议。",
    ]
    return "\n".join(lines)[:4096]


def format_observation_sample(
    event: RadarEvent,
    decision: Decision,
    features: FeatureSnapshot,
    market: MarketSnapshot,
) -> str:
    if event.event_id != decision.event_id or event.event_id != features.event_id:
        raise ValueError("sample identities do not match")
    title = "%s (%s)" % (
        (event.name or "Unnamed").replace("\n", " ").replace("\r", " "),
        (event.symbol or "?").replace("\n", " ").replace("\r", " "),
    )
    reasons = ", ".join(
        code
        for code in decision.reason_codes
        if not code.startswith(("CONFIDENCE_", "OPPORTUNITY_", "DELIVERY_"))
    )
    lines = [
        "🧪 %s · %s 观察样本"
        % (event.chain.upper(), event.launchpad or event.source),
        "%s · ⏱ %s" % (title[:160], _age(event, decision.evaluated_at_ms)),
        "CA  %s" % event.token_address,
        "",
        "🟢 5分钟成交",
        "%s · %s 买 / %s 卖"
        % (
            _money(market.volume_5m_usd),
            market.buy_transactions_5m
            if market.buy_transactions_5m is not None
            else "?",
            market.sell_transactions_5m
            if market.sell_transactions_5m is not None
            else "?",
        ),
        "💵 %s · 💧 %s"
        % (
            "未知" if market.price_usd is None else "$%.10g" % market.price_usd,
            _money(market.liquidity_usd),
        ),
        "💰 市值 %s · 持有人 %s · Dev流入/流出 %s/%s · 同名排名 %s/%s"
        % (
            _money(features.market_cap_usd),
            features.holder_count if features.holder_count is not None else "?",
            features.creator_inbound_count
            if features.creator_inbound_count is not None
            else "?",
            features.creator_outbound_count
            if features.creator_outbound_count is not None
            else "?",
            features.identity_market_cap_rank
            if features.identity_market_cap_rank is not None
            else "?",
            features.identity_count if features.identity_count is not None else "?",
        ),
        "",
        "🟣 叙事  聚集 %d · 跨链 %d · 机会 %d"
        % (
            features.narrative_burst,
            features.cross_chain_count,
            decision.opportunity_score,
        ),
        "🟡 证据  置信度 %d" % decision.confidence_score,
        "🔴 风险缺口  %s" % (reasons or "未知"),
        "🔗 https://dexscreener.com/%s/%s"
        % (event.chain, event.token_address),
        "",
        "⚠️ 仅供策略采样，禁止据此交易。",
    ]
    return "\n".join(lines)[:4096]


class TelegramClient:
    BASE_URL = "https://api.telegram.org"

    def __init__(
        self,
        transport: JsonTransport,
        *,
        bot_token: str,
        chat_id: str,
        topic_id: str = "",
        parse_mode: str = "",
    ) -> None:
        if not _BOT_TOKEN.fullmatch(bot_token):
            raise ValueError("invalid Telegram bot token")
        if not _CHAT_ID.fullmatch(chat_id):
            raise ValueError("invalid Telegram chat id")
        if topic_id:
            try:
                parsed_topic = int(topic_id)
            except ValueError as exc:
                raise ValueError("invalid Telegram topic id") from exc
            if parsed_topic <= 0:
                raise ValueError("invalid Telegram topic id")
        if parse_mode not in {"", "HTML"}:
            raise ValueError("invalid Telegram parse mode")
        self._transport = transport
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._topic_id = topic_id
        self._parse_mode = parse_mode

    def send_message(self, text: str) -> TelegramSendResult:
        if not text or len(text) > 4096:
            raise ValueError("invalid Telegram message length")
        payload = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if self._topic_id:
            payload["message_thread_id"] = int(self._topic_id)
        if self._parse_mode:
            payload["parse_mode"] = self._parse_mode
        result = self._transport.post_json(
            self.BASE_URL + "/bot" + self._bot_token + "/sendMessage",
            payload,
        )
        data = result.data
        if not isinstance(data, dict) or data.get("ok") is not True:
            raise RuntimeError("Telegram API rejected message")
        message = data.get("result")
        if not isinstance(message, dict) or not isinstance(message.get("message_id"), int):
            raise RuntimeError("Telegram response missing message id")
        return TelegramSendResult(
            message_id=message["message_id"],
            received_at_ms=result.received_at_ms,
        )
