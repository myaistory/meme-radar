from __future__ import annotations

import argparse
import asyncio
import html
import json
import os
import signal
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .credentials import load_credentials, load_env_file
from .evm_rpc import HttpRpcClient, RpcError
from .http_json import BoundedJsonClient, HttpBoundaryError
from .pons_curve_notify_storage import (
    claim_due_notification,
    claim_notification,
    connect_notifier,
    get_notifier_state,
    initialize_notifier,
    mark_notification_retry,
    mark_notification_sent,
    notifier_counts,
    set_notifier_state,
)
from .runtime import SlidingBudget, write_health
from .sources import GmgnCliError, GmgnTokenInfoSource
from .sources.dexscreener import DexScreenerSource
from .telegram import TelegramClient
from .unified_policy import (
    FULL_CREATOR_RETENTION_RATIO,
    MIN_HOLDER_COUNT,
    MIN_MARKET_CAP_USD,
    UnifiedSafetyEvidence,
    market_size_reasons,
    unified_safety_reasons,
)


ACTIVATION_KEY = "activation_ms:pons_curve_push_v7"
MIN_AGE_MS = 5 * 60_000
MAX_AGE_MS = 31 * 60_000
SIZE_RECHECK_MS = (5 * 60_000, 10 * 60_000, 20 * 60_000, 30 * 60_000)
WINDOW_MS = 120_000
FINALITY_BUFFER_MS = 30_000
MIN_EXTERNAL_BUYS = 10
MIN_UNIQUE_BUYERS = 5
MIN_BUY_SELL_RATIO = 1.5
MIN_NET_BUY_RATIO = 0.30
MIN_GRADUATION_PROGRESS = 0.03
MIN_NARRATIVE_BURST = 3
GLOBAL_TELEGRAM_MAX_PER_HOUR = 20
WETH_ETHEREUM = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
OFFICIAL_IDENTITIES = {
    "pipt": "0x5f7fd169b227873c051da44886fc51f82932ce04",
}

QUOTE_SYMBOLS = {
    "0x0000000000000000000000000000000000000000": "ETH",
    "0x5fc5360d0400a0fd4f2af552add042d716f1d168": "USDG",
    "0xcec185eb182c47d1ba1efc84e6959e18cd620be4": "cbBTC",
    "0xaf3d76f1834a1d425780943c99ea8a608f8a93f9": "AAPL",
    "0x86923f96303d656e4aa86d9d42d1e57ad2023fdc": "AMD",
    "0x12f190a9f9d7d37a250758b26824b97ce941bf54": "AMZN",
    "0x48e39e56acdba37b09020c0b734a613c9a2f100a": "BB",
    "0x6330d8c3178a418788df01a47479c0ce7ccf450b": "COIN",
    "0x4ea005168d7f09a7a0ba9d1def21a479950e44c2": "COST",
    "0xdf0992e440dd0be65bd8439b609d6d4366bf1cb5": "CRCL",
    "0x941ae714ec6d8130c7b75d67160ca08f1e7d11dd": "DELL",
    "0x1d11f0496982706c5e14a514d4e79f2e6bde4516": "DJT",
    "0xc9a981fee1f9dec688bb123ccdecc63d0debfc4e": "GLD",
    "0x1b0e319c6a659f002271b69db8a7df2f911c153e": "GME",
    "0x2e0847e8910a9732eb3fb1bb4b70a580adad4fe3": "GOOGL",
    "0xccee82fe024c36fa15e1005ede3e9e4787e23d09": "HIMS",
    "0x8005d266423c7ea827372c9c864491e5786600ea": "LLY",
    "0xc0d6457c16cc70d6790dd43521c899c87ce02f35": "META",
    "0xe93237c50d904957cf27e7b1133b510c669c2e74": "MSFT",
    "0xec262a75e413fafd0df80480274532c79d42da09": "MSTR",
    "0xff080c8ce2e5feadaca0da81314ae59d232d4afd": "MU",
    "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec": "NVDA",
    "0x894e1ec2d74ffe5aef8dc8a9e84686accb964f2a": "PLTR",
    "0xd5f3879160bc7c32ebb4dc785f8a4f505888de68": "QQQ",
    "0xf0c4bf4c582cb3836e98394b1d4e7b7281101be8": "RBLX",
    "0x05b37fb53a299a1b874a619e1c4c404d52c36f4c": "RDDT",
    "0x84cab63bc87912e71ad199ff14a0ba45de68fef8": "SKHY",
    "0xb90a19ff0af67f7779aff50a882a9cff42446400": "SNDK",
    "0x4a0e65a3eccec6dbe60ae065f2e7bb85fae35eea": "SPCX",
    "0x117cc2133c37b721f49de2a7a74833232b3b4c0c": "SPY",
    "0x322f0929c4625ed5bad873c95208d54e1c003b2d": "TSLA",
    "0x58ffe4a942d3885baa22d7520691f611ef09e7aa": "TSM",
    "0x5e81213613b6b86eab4c6c50d718d34359459786": "TTWO",
    "0xa30fa36db767ad9ed3f7a60fc79526fb4d56d344": "USO",
    "0x9e7abd3c9139d14e4c86dce0e455aab7a0c2fb3e": "WYFI",
}


@dataclass(frozen=True)
class CurveCandidate:
    token_address: str
    creator_address: str
    pair_token_address: str
    launched_at_ms: int
    external_buys_5m: int
    unique_buyers_5m: int
    sells_5m: int
    buy_quote_raw: float
    sell_quote_raw: float
    creator_initial: bool
    creator_buys: int
    creator_sells: int
    creator_retention_ratio: Optional[float]
    creator_launches_1h: int
    name: str
    symbol: str
    event_id: str
    narrative_burst: int
    cross_chain_count: int
    last_price_quote: Optional[float]
    market_cap_usd: float
    holder_count: int
    identity_status: str
    same_name_contracts: int
    net_buy_ratio: float
    graduation_progress: float
    score: int


def _log(kind: str, **fields: Any) -> None:
    print(
        json.dumps(
            {"event": kind, **fields},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
        flush=True,
    )


def _readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=10000")
    return connection


def _clean(value: Any, limit: int) -> str:
    return str(value or "").replace("\r", " ").replace("\n", " ")[:limit]


def _identity_key(value: str) -> str:
    return " ".join(value.casefold().strip().split())


def _official_address(name: str, symbol: str) -> Optional[str]:
    for key in (_identity_key(name), _identity_key(symbol)):
        if key in OFFICIAL_IDENTITIES:
            return OFFICIAL_IDENTITIES[key]
    return None


def _quote_meta(address: str) -> Tuple[str, int]:
    symbol = QUOTE_SYMBOLS.get(address)
    if symbol is None:
        symbol = address[:6] + "…" + address[-4:]
    if symbol == "USDG":
        decimals = 6
    elif symbol == "cbBTC":
        decimals = 8
    else:
        decimals = 18
    return symbol, decimals


def _amount(raw: float, decimals: int) -> str:
    value = raw / (10 ** decimals)
    if value >= 1_000_000:
        return "%.2fM" % (value / 1_000_000)
    if value >= 1_000:
        return "%.2fK" % (value / 1_000)
    return "%.6g" % value


def _usd(value: float) -> str:
    if value >= 1_000_000:
        return "$%.2fM" % (value / 1_000_000)
    if value >= 1_000:
        return "$%.2fK" % (value / 1_000)
    return "$%.0f" % value


def _runtime_settings(dry_run: bool) -> Tuple[bool, int, int]:
    enabled = os.environ.get(
        "MEME_RADAR_PONS_CURVE_TELEGRAM_ENABLED", "0"
    ).strip()
    if enabled not in {"0", "1"}:
        raise ValueError("invalid Pons Telegram enabled flag")
    max_per_hour = int(
        os.environ.get("MEME_RADAR_PONS_CURVE_MAX_PER_HOUR", "2")
    )
    poll_seconds = int(
        os.environ.get("MEME_RADAR_PONS_CURVE_POLL_SECONDS", "10")
    )
    if not 1 <= max_per_hour <= 5 or not 5 <= poll_seconds <= 60:
        raise ValueError("invalid Pons notifier runtime setting")
    return enabled == "1" and not dry_run, max_per_hour, poll_seconds


def _telegram_client(env_file: Path, enabled: bool):
    credentials = load_credentials(env_file)
    if not enabled:
        return None
    if not credentials.telegram_bot_token or not credentials.telegram_chat_id:
        raise RuntimeError("Telegram credentials missing")
    return TelegramClient(
        BoundedJsonClient(
            allowed_hosts={"api.telegram.org"},
            timeout_seconds=15,
            max_bytes=128 * 1024,
        ),
        bot_token=credentials.telegram_bot_token,
        chat_id=credentials.telegram_chat_id,
        topic_id=credentials.telegram_topic_id,
        parse_mode="HTML",
    )


def format_curve_sample(candidate: CurveCandidate, now_ms: int) -> str:
    quote, decimals = _quote_meta(candidate.pair_token_address)
    quote = html.escape(quote)
    age_seconds = max(0, (now_ms - candidate.launched_at_ms) // 1000)
    age = "%dm%02ds" % (age_seconds // 60, age_seconds % 60)
    price = (
        "未知"
        if candidate.last_price_quote is None
        else "%.8g %s" % (candidate.last_price_quote, quote)
    )
    name = html.escape(candidate.name or "Unnamed")
    symbol = html.escape(candidate.symbol or "?")
    token = html.escape(candidate.token_address)
    identity = html.escape(candidate.identity_status)
    lines = [
        "🔭 <b>Robinhood · Pons 曲线观察</b>",
        "<b>%s</b> · <code>%s</code> · ⏱ %s" % (name, symbol, age),
        "<code>%s</code>" % token,
        "",
        "🟢 <b>前2分钟成交</b>",
        "%d 买 / %d 卖 · %d 个独立买家"
        % (
            candidate.external_buys_5m,
            candidate.sells_5m,
            candidate.unique_buyers_5m,
        ),
        "%s → %s %s · 净流入 %.1f%%"
        % (
            _amount(candidate.buy_quote_raw, decimals),
            _amount(candidate.sell_quote_raw, decimals),
            quote,
            candidate.net_buy_ratio * 100,
        ),
        "曲线 +%.1f%% · 最新价 %s"
        % (candidate.graduation_progress * 100, price),
        "💰 <b>规模门</b>  市值 %s（>$100K）· 持有人 %d（>100）"
        % (_usd(candidate.market_cap_usd), candidate.holder_count),
        "",
        "🟣 <b>叙事</b>  聚集 %d · 跨链 %d · 热度 %d"
        % (
            candidate.narrative_burst,
            candidate.cross_chain_count,
            candidate.score,
        ),
        "🟡 <b>身份</b>  %s · 同名CA %d · 创建者1h发币 %d"
        % (identity, candidate.same_name_contracts, candidate.creator_launches_1h),
        "⚪ Dev 买入 %d 笔（已排除）· 卖出 %d 笔 · 持仓保留 %s"
        % (
            candidate.creator_buys,
            candidate.creator_sells,
            (
                "未知"
                if candidate.creator_retention_ratio is None
                else "%.1f%%" % (candidate.creator_retention_ratio * 100)
            ),
        ),
        "",
        "🔴 <b>风险缺口</b>  安全 · 卖出模拟 · 美元价格",
        "<i>仅供策略采样，禁止据此交易。</i>",
    ]
    return "\n".join(lines)[:4096]


class PonsCurveNotifier:
    def __init__(
        self,
        *,
        env_file: Path,
        shadow_db: Path,
        shadow_health: Path,
        main_db: Path,
        db_path: Path,
        health_path: Path,
        dry_run: bool,
        activation_lookback_seconds: int,
        rpc_env: Optional[Path] = None,
        rpc_client: Optional[Any] = None,
        dex_source: Optional[Any] = None,
        gmgn_source: Optional[Any] = None,
    ) -> None:
        settings = _runtime_settings(dry_run)
        self.telegram_enabled, self.max_per_hour, self.poll_seconds = settings
        self.telegram = _telegram_client(env_file, self.telegram_enabled)
        self.dry_run = dry_run
        self.shadow_health = shadow_health
        self.shadow = _readonly(shadow_db)
        self.main = _readonly(main_db)
        self.connection = connect_notifier(db_path)
        initialize_notifier(self.connection)
        if rpc_client is None:
            if rpc_env is None:
                raise ValueError("Pons notifier RPC env missing")
            rpc_values = load_env_file(rpc_env)
            rpc_url = rpc_values.get("ROBINHOOD_RPC_URL", "")
            if not rpc_url:
                raise ValueError("Pons notifier HTTP RPC missing")
            rpc_client = HttpRpcClient(
                rpc_url,
                max_requests_per_second=1,
            )
        self.rpc = rpc_client
        self.dex = dex_source or DexScreenerSource(
            BoundedJsonClient(
                allowed_hosts={"api.dexscreener.com"},
                timeout_seconds=12,
                max_bytes=2 * 1024 * 1024,
            )
        )
        self.gmgn_info = gmgn_source or GmgnTokenInfoSource(cache_seconds=60)
        self.gmgn_budget = SlidingBudget(120)
        self.quote_price_cache: Dict[str, Tuple[int, float]] = {}
        self.token_snapshot_cache: Dict[
            Tuple[str, str], Tuple[int, Tuple[int, int, int]]
        ] = {}
        self.health_path = health_path
        self.stop_event = asyncio.Event()
        self.started_at_ms = int(time.time() * 1000)
        activation = get_notifier_state(self.connection, ACTIVATION_KEY)
        if activation is None:
            activation = self.started_at_ms - activation_lookback_seconds * 1000
            set_notifier_state(
                self.connection, ACTIVATION_KEY, activation, self.started_at_ms
            )
        self.activation_ms = int(activation)
        self.status = "starting"
        self.counters = Counter()
        self.size_blocked_tokens = set()
        self.last_success: Dict[str, int] = {}
        self.last_error: Dict[str, Any] = {}
        self.last_candidate = ""

    def close(self) -> None:
        self.shadow.close()
        self.main.close()
        self.connection.close()

    def _collector_ready(self, now_ms: int) -> bool:
        try:
            info = self.shadow_health.stat()
            if not self.shadow_health.is_file() or info.st_size > 64 * 1024:
                return False
            payload = json.loads(self.shadow_health.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return False
        return (
            isinstance(payload, dict)
            and payload.get("status") == "running"
            and payload.get("connected") is True
            and isinstance(payload.get("updated_at_ms"), int)
            and now_ms - int(payload["updated_at_ms"]) <= 30_000
        )

    def _curve_watermark(self) -> int:
        row = self.shadow.execute(
            """
            SELECT value_json FROM pons_curve_shadow_state
            WHERE key='curve_event_time_watermark_ms'
            """
        ).fetchone()
        if row is None:
            return 0
        try:
            return int(json.loads(row["value_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return 0

    def _curve_coverage_start(self, now_ms: int) -> int:
        row = self.shadow.execute(
            """
            SELECT value_json FROM pons_curve_shadow_state
            WHERE key='curve_coverage_start_ms'
            """
        ).fetchone()
        if row is None:
            return now_ms
        try:
            return int(json.loads(row["value_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return now_ms

    def _token_uint(self, token: str, data: str) -> int:
        value = self.rpc.call(
            "eth_call",
            [{"to": token, "data": data}, "latest"],
        )
        if not isinstance(value, str) or not value.startswith("0x"):
            raise RpcError("INVALID_TOKEN_CALL")
        return int(value, 16)

    def _token_snapshot(
        self,
        token: str,
        creator: str,
        now_ms: int,
    ) -> Optional[Tuple[int, int, int]]:
        key = (token, creator)
        cached = self.token_snapshot_cache.get(key)
        if cached is not None and cached[0] >= now_ms:
            return cached[1]
        try:
            supply = self._token_uint(token, "0x18160ddd")
            decimals = self._token_uint(token, "0x313ce567")
            creator_word = "0" * 24 + creator[2:]
            balance = self._token_uint(token, "0x70a08231" + creator_word)
        except (RpcError, TypeError, ValueError):
            self.counters["token_snapshot_unavailable"] += 1
            return None
        if supply <= 0 or not 0 <= decimals <= 36 or not 0 <= balance <= supply:
            self.counters["token_snapshot_invalid"] += 1
            return None
        snapshot = (supply, decimals, balance)
        self.token_snapshot_cache[key] = (now_ms + 30_000, snapshot)
        return snapshot

    def _quote_price_usd(self, pair: str, now_ms: int) -> Optional[float]:
        cached = self.quote_price_cache.get(pair)
        if cached is not None and cached[0] >= now_ms:
            return cached[1]
        chain, token = (
            ("ethereum", WETH_ETHEREUM)
            if pair == "0x0000000000000000000000000000000000000000"
            else ("robinhood", pair)
        )
        try:
            market = self.dex.token_market(chain, token)
            price = float(market.price_usd)
        except (AttributeError, RuntimeError, TypeError, ValueError, HttpBoundaryError):
            self.counters["quote_price_unavailable"] += 1
            return None
        if market.pair_count < 1 or price <= 0:
            self.counters["quote_price_unavailable"] += 1
            return None
        self.quote_price_cache[pair] = (now_ms + 60_000, price)
        return price

    def _due_size_checkpoint(
        self,
        token: str,
        launched_at_ms: int,
        now_ms: int,
    ) -> Optional[int]:
        age_ms = now_ms - launched_at_ms
        due = [offset for offset in SIZE_RECHECK_MS if offset <= age_ms]
        if not due:
            return None
        checkpoint = max(due)
        completed = int(
            get_notifier_state(
                self.connection,
                "size_checkpoint:" + token,
                0,
            )
        )
        return checkpoint if checkpoint > completed else None

    def _gmgn_size(self, token: str, launched_at_ms: int, now_ms: int):
        checkpoint = self._due_size_checkpoint(token, launched_at_ms, now_ms)
        if checkpoint is None:
            return None
        if not self.gmgn_info.has_fresh_cache("robinhood", token):
            if not self.gmgn_budget.take():
                self.counters["gmgn_size_budget_blocked"] += 1
                set_notifier_state(
                    self.connection,
                    "size_checkpoint:" + token,
                    checkpoint,
                    now_ms,
                )
                return None
        try:
            snapshot = self.gmgn_info.token_info("robinhood", token)
        except (GmgnCliError, OSError, ValueError) as exc:
            self.counters["gmgn_size_error"] += 1
            if isinstance(exc, GmgnCliError) and exc.status == 429:
                self.gmgn_budget.cooldown(300)
            set_notifier_state(
                self.connection,
                "size_checkpoint:" + token,
                checkpoint,
                now_ms,
            )
            return None
        if snapshot is None:
            self.counters["gmgn_size_unavailable"] += 1
            set_notifier_state(
                self.connection,
                "size_checkpoint:" + token,
                checkpoint,
                now_ms,
            )
            return None
        set_notifier_state(
            self.connection,
            "size_checkpoint:" + token,
            checkpoint,
            now_ms,
        )
        self.counters["size_check_%dm" % (checkpoint // 60_000)] += 1
        reasons = market_size_reasons(
            snapshot.market_cap_usd,
            snapshot.holder_count,
        )
        if reasons:
            if token not in self.size_blocked_tokens:
                for reason in reasons:
                    self.counters["size_" + reason] += 1
                self.size_blocked_tokens.add(token)
            return None
        self.counters["market_size_pass"] += 1
        self.last_success["gmgn_size"] = snapshot.observed_at_ms
        return snapshot

    def _market_rows(self, now_ms: int):
        if not self._collector_ready(now_ms):
            self.counters["collector_not_ready"] += 1
            return []
        watermark = self._curve_watermark()
        coverage_start = self._curve_coverage_start(now_ms)
        return self.shadow.execute(
            """
            SELECT l.token_address,l.creator_address,l.pair_token_address,
                   l.block_timestamp_ms,l.graduation_threshold_raw,
                   sum(CASE WHEN t.event_kind='buy' AND t.is_creator_initial=0
                             AND t.trader_address<>l.creator_address
                             AND t.recipient_address<>l.creator_address
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=l.block_timestamp_ms+?
                             THEN 1 ELSE 0 END) buys,
                   count(DISTINCT CASE WHEN t.event_kind='buy'
                             AND t.is_creator_initial=0
                             AND t.trader_address<>l.creator_address
                             AND t.recipient_address<>l.creator_address
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=l.block_timestamp_ms+?
                             THEN t.trader_address END) unique_buyers,
                   sum(CASE WHEN t.event_kind='sell'
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=l.block_timestamp_ms+?
                             THEN 1 ELSE 0 END) sells,
                   sum(CASE WHEN t.event_kind='buy' AND t.is_creator_initial=0
                             AND t.trader_address<>l.creator_address
                             AND t.recipient_address<>l.creator_address
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=l.block_timestamp_ms+?
                             THEN cast(t.quote_amount_raw AS REAL) ELSE 0 END) buy_quote,
                   sum(CASE WHEN t.event_kind='sell'
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=l.block_timestamp_ms+?
                             THEN cast(t.quote_amount_raw AS REAL) ELSE 0 END) sell_quote,
                   sum(CASE WHEN t.event_kind='buy'
                             AND (t.is_creator_initial=1
                               OR t.trader_address=l.creator_address
                               OR t.recipient_address=l.creator_address)
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=?
                             THEN 1 ELSE 0 END) creator_buys,
                   sum(CASE WHEN t.event_kind='buy'
                             AND (t.is_creator_initial=1
                               OR t.trader_address=l.creator_address
                               OR t.recipient_address=l.creator_address)
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=?
                             THEN cast(t.token_amount_raw AS REAL) ELSE 0 END)
                             creator_acquired_tokens,
                   sum(CASE WHEN t.event_kind='sell'
                             AND (t.trader_address=l.creator_address
                               OR t.recipient_address=l.creator_address)
                             AND t.block_timestamp_ms>=l.block_timestamp_ms
                             AND t.block_timestamp_ms<=?
                             THEN 1 ELSE 0 END) creator_sells,
                   max(CASE WHEN t.is_creator_initial=1 THEN 1 ELSE 0 END) creator_initial
            FROM pons_curve_launches l
            LEFT JOIN pons_curve_trades t
              ON t.token_address=l.token_address AND t.removed=0
            WHERE l.removed=0 AND l.block_timestamp_ms>=?
              AND l.block_timestamp_ms<=?
              AND ?>=l.block_timestamp_ms+?+?
              AND NOT EXISTS (
                SELECT 1 FROM pons_curve_lifecycle x
                WHERE x.token_address=l.token_address AND x.removed=0
                  AND x.event_kind='pool_graduated'
              )
            GROUP BY l.token_address
            HAVING buys>=? AND unique_buyers>=?
              AND cast(buys AS REAL)/max(1,sells)>=?
              AND buy_quote>sell_quote
              AND (buy_quote-sell_quote)/buy_quote>=?
              AND cast(graduation_threshold_raw AS REAL)>0
              AND (buy_quote-sell_quote)
                  /cast(graduation_threshold_raw AS REAL)>=?
            """,
            (
                WINDOW_MS,
                WINDOW_MS,
                WINDOW_MS,
                WINDOW_MS,
                WINDOW_MS,
                min(now_ms, watermark),
                min(now_ms, watermark),
                min(now_ms, watermark),
                max(
                    self.activation_ms,
                    coverage_start,
                    now_ms - MAX_AGE_MS,
                ),
                now_ms - MIN_AGE_MS,
                watermark,
                WINDOW_MS,
                FINALITY_BUFFER_MS,
                MIN_EXTERNAL_BUYS,
                MIN_UNIQUE_BUYERS,
                MIN_BUY_SELL_RATIO,
                MIN_NET_BUY_RATIO,
                MIN_GRADUATION_PROGRESS,
            ),
        ).fetchall()

    def _metadata(self, token_address: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        row = self.main.execute(
            """
            SELECT event_id,event_json FROM radar_events
            WHERE chain='robinhood' AND token_address=? AND event_kind='launch'
              AND is_backfill=0
            ORDER BY received_at_ms DESC LIMIT 1
            """,
            (token_address,),
        ).fetchone()
        if row is None:
            return None
        try:
            payload = json.loads(row["event_json"])
        except (TypeError, json.JSONDecodeError):
            return None
        return str(row["event_id"]), payload

    def _explicitly_rejected(self, event_id: str) -> bool:
        row = self.main.execute(
            "SELECT 1 FROM decisions WHERE event_id=? AND risk_verdict='reject' LIMIT 1",
            (event_id,),
        ).fetchone()
        return row is not None

    def _narrative(self, name: str, now_ms: int) -> Tuple[int, int]:
        if not name:
            return 1, 1
        row = self.main.execute(
            """
            SELECT count(*),count(DISTINCT chain) FROM radar_events
            WHERE received_at_ms>=?
              AND lower(trim(json_extract(event_json,'$.name')))=lower(trim(?))
            """,
            (now_ms - 900_000, name),
        ).fetchone()
        return max(1, int(row[0])), max(1, int(row[1]))

    def _same_identity_contracts(
        self,
        name: str,
        symbol: str,
        now_ms: int,
    ) -> int:
        name_key, symbol_key = _identity_key(name), _identity_key(symbol)
        row = self.main.execute(
            """
            SELECT count(DISTINCT token_address) FROM radar_events
            WHERE chain='robinhood' AND event_kind='launch'
              AND token_created_at_ms>=?
              AND ((?<>'' AND lower(trim(json_extract(event_json,'$.name')))=?)
                OR (?<>'' AND lower(trim(json_extract(event_json,'$.symbol')))=?))
            """,
            (
                now_ms - 900_000,
                name_key,
                name_key,
                symbol_key,
                symbol_key,
            ),
        ).fetchone()
        return int(row[0])

    def _identity_status(
        self,
        token: str,
        name: str,
        symbol: str,
        now_ms: int,
    ) -> Optional[Tuple[str, int]]:
        collision_count = self._same_identity_contracts(name, symbol, now_ms)
        official = _official_address(name, symbol)
        if official is not None:
            if token != official:
                self.counters["owner_ca_mismatch"] += 1
                return None
            return "Owner确认CA", collision_count
        return (
            "同名待市值比选" if collision_count > 1 else "未验证",
            collision_count,
        )

    def _creator_launches(self, creator: str, now_ms: int) -> int:
        return int(
            self.shadow.execute(
                """
                SELECT count(*) FROM pons_curve_launches
                WHERE creator_address=? AND removed=0 AND block_timestamp_ms>=?
                """,
                (creator, now_ms - 3_600_000),
            ).fetchone()[0]
        )

    def _last_price(
        self,
        token: str,
        creator: str,
        pair: str,
        launched_at_ms: int,
    ) -> Optional[float]:
        row = self.shadow.execute(
            """
            SELECT quote_amount_raw,token_amount_raw FROM pons_curve_trades
            WHERE token_address=? AND removed=0 AND is_creator_initial=0
              AND trader_address<>? AND recipient_address<>?
              AND block_timestamp_ms BETWEEN ? AND ?
            ORDER BY block_number DESC,log_index DESC LIMIT 1
            """,
            (
                token,
                creator,
                creator,
                launched_at_ms,
                launched_at_ms + WINDOW_MS,
            ),
        ).fetchone()
        if row is None:
            return None
        _, quote_decimals = _quote_meta(pair)
        quote = int(row["quote_amount_raw"]) / (10 ** quote_decimals)
        tokens = int(row["token_amount_raw"]) / (10 ** 18)
        return quote / tokens if quote >= 0 and tokens > 0 else None

    def _candidate(self, row, now_ms: int) -> Optional[CurveCandidate]:
        notified = self.connection.execute(
            "SELECT 1 FROM pons_curve_notification_outbox WHERE token_address=?",
            (row["token_address"],),
        ).fetchone()
        if notified is not None:
            self.counters["already_notified"] += 1
            return None
        metadata = self._metadata(row["token_address"])
        if metadata is None:
            self.counters["metadata_missing"] += 1
            return None
        event_id, payload = metadata
        if self._explicitly_rejected(event_id):
            self.counters["explicit_reject"] += 1
            return None
        gmgn_size = self._gmgn_size(
            row["token_address"],
            int(row["block_timestamp_ms"]),
            now_ms,
        )
        if gmgn_size is None:
            return None
        name = _clean(payload.get("name"), 120)
        symbol = _clean(payload.get("symbol"), 32)
        identity = self._identity_status(
            row["token_address"], name, symbol, now_ms
        )
        if identity is None:
            return None
        burst, cross_chain = self._narrative(name, now_ms)
        if burst < MIN_NARRATIVE_BURST and cross_chain < 2:
            self.counters["narrative_below_gate"] += 1
            return None
        creator_launches = self._creator_launches(row["creator_address"], now_ms)
        buys, sells = int(row["buys"]), int(row["sells"])
        unique = int(row["unique_buyers"])
        buy_quote = float(row["buy_quote"])
        sell_quote = float(row["sell_quote"])
        creator_buys = int(row["creator_buys"] or 0)
        creator_sells = int(row["creator_sells"] or 0)
        creator_acquired = float(row["creator_acquired_tokens"] or 0)
        net_quote = buy_quote - sell_quote
        threshold = float(row["graduation_threshold_raw"])
        net_ratio = net_quote / buy_quote
        progress = net_quote / threshold
        launched_at_ms = int(row["block_timestamp_ms"])
        last_price = self._last_price(
            row["token_address"],
            row["creator_address"],
            row["pair_token_address"],
            launched_at_ms,
        )
        snapshot = self._token_snapshot(
            row["token_address"],
            row["creator_address"],
            now_ms,
        )
        quote_price = self._quote_price_usd(
            row["pair_token_address"], now_ms
        )
        if snapshot is None or last_price is None or quote_price is None:
            self.counters["market_cap_unknown"] += 1
            return None
        total_supply, token_decimals, creator_balance = snapshot
        curve_market_cap = (
            (total_supply / (10 ** token_decimals))
            * last_price
            * quote_price
        )
        market_cap = min(curve_market_cap, gmgn_size.market_cap_usd)
        creator_retention = None
        if creator_acquired > 0:
            observed_retention = creator_balance / creator_acquired
            creator_retention = min(1.0, observed_retention)
        gate_reasons = unified_safety_reasons(
            UnifiedSafetyEvidence(
                market_cap_usd=market_cap,
                creator_inbound_count=creator_buys,
                creator_outbound_count=creator_sells,
                creator_retention_ratio=creator_retention,
                creator_balance_present=creator_balance > 0,
            )
        )
        if gate_reasons:
            for reason in gate_reasons:
                self.counters[reason] += 1
            return None
        score = min(40, unique * 4)
        score += min(30, max(0, buys - sells) * 2)
        score += min(20, buys)
        score += 10 if now_ms - launched_at_ms <= MIN_AGE_MS else 0
        score -= min(30, max(0, creator_launches - 1) * 5)
        return CurveCandidate(
            token_address=row["token_address"],
            creator_address=row["creator_address"],
            pair_token_address=row["pair_token_address"],
            launched_at_ms=launched_at_ms,
            external_buys_5m=buys,
            unique_buyers_5m=unique,
            sells_5m=sells,
            buy_quote_raw=buy_quote,
            sell_quote_raw=sell_quote,
            creator_initial=bool(row["creator_initial"]),
            creator_buys=creator_buys,
            creator_sells=creator_sells,
            creator_retention_ratio=creator_retention,
            creator_launches_1h=creator_launches,
            name=name,
            symbol=symbol,
            event_id=event_id,
            narrative_burst=burst,
            cross_chain_count=cross_chain,
            last_price_quote=last_price,
            market_cap_usd=market_cap,
            holder_count=gmgn_size.holder_count,
            identity_status=identity[0],
            same_name_contracts=identity[1],
            net_buy_ratio=net_ratio,
            graduation_progress=progress,
            score=max(0, score),
        )

    def candidates(self, now_ms: int) -> List[CurveCandidate]:
        candidates = [
            candidate
            for row in self._market_rows(now_ms)
            for candidate in [self._candidate(row, now_ms)]
            if candidate is not None
        ]
        selected: List[CurveCandidate] = []
        for candidate in candidates:
            name_key = _identity_key(candidate.name)
            symbol_key = _identity_key(candidate.symbol)
            competitors = [
                other
                for other in candidates
                if other.token_address != candidate.token_address
                and (
                    (name_key and _identity_key(other.name) == name_key)
                    or (symbol_key and _identity_key(other.symbol) == symbol_key)
                )
            ]
            if len(competitors) < candidate.same_name_contracts - 1:
                self.counters["identity_market_cap_incomplete"] += 1
                continue
            if any(
                (other.market_cap_usd, other.token_address)
                > (candidate.market_cap_usd, candidate.token_address)
                for other in competitors
            ):
                self.counters["identity_lower_market_cap"] += 1
                continue
            selected.append(
                replace(candidate, identity_status="同名最高市值")
                if competitors
                else candidate
            )
        candidates = selected
        return sorted(
            candidates,
            key=lambda item: (
                item.score,
                item.unique_buyers_5m,
                item.external_buys_5m - item.sells_5m,
                -item.launched_at_ms,
            ),
            reverse=True,
        )

    def _global_capacity(self, now_ms: int) -> bool:
        sent = int(
            self.main.execute(
                """
                SELECT count(*) FROM telegram_outbox
                WHERE state='sent' AND sent_at_ms>?
                """,
                (now_ms - 3_600_000,),
            ).fetchone()[0]
        )
        return sent < GLOBAL_TELEGRAM_MAX_PER_HOUR

    def enqueue_best(self, now_ms: int) -> None:
        if not self._global_capacity(now_ms):
            self.counters["global_rate_limited"] += 1
            return
        for candidate in self.candidates(now_ms):
            status = claim_notification(
                self.connection,
                token_address=candidate.token_address,
                message_text=format_curve_sample(candidate, now_ms),
                now_ms=now_ms,
                max_per_hour=self.max_per_hour,
            )
            self.counters[status] += 1
            if status == "enqueued":
                self.last_candidate = candidate.token_address
                return
            if status == "rate_limited":
                return

    async def deliver_one(self, now_ms: int) -> None:
        item = claim_due_notification(
            self.connection, now_ms=now_ms, max_attempts=5
        )
        if item is None:
            return
        if self.dry_run:
            mark_notification_sent(
                self.connection, item["token_address"], now_ms
            )
            self.counters["dry_run_sent"] += 1
            self.last_success["delivery"] = now_ms
            return
        if self.telegram is None:
            mark_notification_retry(
                self.connection,
                item["token_address"],
                next_attempt_at_ms=now_ms + 60_000,
                error_code="TELEGRAM_DISABLED",
            )
            self.counters["delivery_disabled"] += 1
            return
        try:
            result = await asyncio.to_thread(
                self.telegram.send_message, item["message_text"]
            )
            mark_notification_sent(
                self.connection,
                item["token_address"],
                result.received_at_ms,
            )
            self.counters["telegram_sent"] += 1
            self.last_success["delivery"] = result.received_at_ms
        except Exception as exc:
            delay = min(3600, 30 * (2 ** (item["attempts"] - 1)))
            if isinstance(exc, HttpBoundaryError) and exc.status == 429:
                delay = max(60, delay)
                self.counters["telegram_429"] += 1
            mark_notification_retry(
                self.connection,
                item["token_address"],
                next_attempt_at_ms=int(time.time() * 1000) + delay * 1000,
                error_code=type(exc).__name__,
            )
            self.counters["delivery_errors"] += 1
            self.last_error = {
                "at_ms": int(time.time() * 1000),
                "type": type(exc).__name__,
            }

    def health_payload(self) -> Dict[str, Any]:
        now_ms = int(time.time() * 1000)
        return {
            "schema_version": 1,
            "policy_version": "pons_curve_push_v7",
            "status": self.status,
            "updated_at_ms": now_ms,
            "started_at_ms": self.started_at_ms,
            "activation_ms": self.activation_ms,
            "robinhood_curve_push": self.telegram_enabled,
            "dry_run": self.dry_run,
            "no_signing": True,
            "no_broadcast": True,
            "rpc_read_only": True,
            "main_db_read_only": True,
            "shadow_db_read_only": True,
            "collector_ready": self._collector_ready(now_ms),
            "curve_watermark_ms": self._curve_watermark(),
            "max_per_hour": self.max_per_hour,
            "min_market_cap_usd": MIN_MARKET_CAP_USD,
            "min_holder_count": MIN_HOLDER_COUNT,
            "market_cap_strictly_above": True,
            "holder_count_strictly_above": True,
            "size_recheck_minutes": [5, 10, 20, 30],
            "gmgn_budget": self.gmgn_budget.snapshot(),
            "full_creator_retention_block_ratio": (
                FULL_CREATOR_RETENTION_RATIO
            ),
            "outbox": notifier_counts(self.connection),
            "counters": dict(sorted(self.counters.items())),
            "last_success": dict(sorted(self.last_success.items())),
            "last_error": self.last_error,
            "last_candidate": self.last_candidate,
        }

    async def wait(self) -> None:
        try:
            await asyncio.wait_for(
                self.stop_event.wait(), timeout=self.poll_seconds
            )
        except asyncio.TimeoutError:
            pass

    async def run(self, run_seconds: int = 0) -> None:
        self.status = "running"
        write_health(self.health_path, self.health_payload())
        _log(
            "PONS_CURVE_NOTIFIER_START",
            dry_run=self.dry_run,
            robinhood_curve_push=self.telegram_enabled,
            no_signing=True,
            no_broadcast=True,
        )
        timer = None
        if run_seconds:
            async def timed_stop() -> None:
                await asyncio.sleep(run_seconds)
                self.stop_event.set()
            timer = asyncio.create_task(timed_stop())
        while not self.stop_event.is_set():
            now_ms = int(time.time() * 1000)
            try:
                self.enqueue_best(now_ms)
                await self.deliver_one(now_ms)
                self.last_success["poll"] = int(time.time() * 1000)
            except Exception as exc:
                self.counters["poll_errors"] += 1
                self.last_error = {
                    "at_ms": int(time.time() * 1000),
                    "type": type(exc).__name__,
                }
            write_health(self.health_path, self.health_payload())
            await self.wait()
        if timer is not None:
            timer.cancel()
            await asyncio.gather(timer, return_exceptions=True)
        self.status = "stopped"
        write_health(self.health_path, self.health_payload())
        _log(
            "PONS_CURVE_NOTIFIER_STOP",
            counters=dict(sorted(self.counters.items())),
        )


def arguments():
    parser = argparse.ArgumentParser(description="Pons curve gated notifier")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--rpc-env", type=Path, required=True)
    parser.add_argument("--shadow-db", type=Path, required=True)
    parser.add_argument("--shadow-health", type=Path, required=True)
    parser.add_argument("--main-db", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--health-path", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--activation-lookback-seconds", type=int, default=0)
    parser.add_argument("--run-seconds", type=int, default=0)
    return parser.parse_args()


async def async_main(args) -> int:
    paths = (
        args.rpc_env,
        args.shadow_db,
        args.shadow_health,
        args.main_db,
        args.db_path,
        args.health_path,
    )
    if any(not str(path) or path == Path("/") for path in paths):
        raise ValueError("unsafe notifier path")
    if len(
        {
            str(args.shadow_db.resolve()),
            str(args.main_db.resolve()),
            str(args.db_path.resolve()),
        }
    ) != 3:
        raise ValueError("notifier databases must be separate")
    if not 0 <= args.activation_lookback_seconds <= 600:
        raise ValueError("activation lookback outside 0..600")
    if not 0 <= args.run_seconds <= 86_400:
        raise ValueError("run-seconds outside 0..86400")
    notifier = PonsCurveNotifier(
        env_file=args.env_file,
        shadow_db=args.shadow_db,
        shadow_health=args.shadow_health,
        main_db=args.main_db,
        db_path=args.db_path,
        health_path=args.health_path,
        dry_run=args.dry_run,
        activation_lookback_seconds=args.activation_lookback_seconds,
        rpc_env=args.rpc_env,
    )
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, notifier.stop_event.set)
        except NotImplementedError:
            pass
    try:
        await notifier.run(args.run_seconds)
    finally:
        notifier.close()
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main(arguments()))
    except Exception as exc:
        _log(
            "PONS_CURVE_NOTIFIER_FATAL",
            error_type=type(exc).__name__,
            no_signing=True,
            no_broadcast=True,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
