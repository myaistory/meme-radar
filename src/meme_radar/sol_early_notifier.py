from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import os
import re
import signal
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional

from .credentials import load_credentials
from .http_json import BoundedJsonClient, HttpBoundaryError
from .runtime import write_health
from .sol_early_storage import (
    claim_candidate,
    claim_due,
    claim_notification,
    connect,
    counts,
    defer_candidate_without_attempt,
    finish_candidate,
    get_state,
    initialize,
    mark_unknown,
    mark_sent,
    provider_budget_state,
    provider_next_available_ms,
    recent_signals,
    reserve_provider_calls,
    schedule_candidate,
    set_provider_cooldown,
    set_state,
    store_signal,
)
from .sources import (
    GmgnCliError,
    GmgnSolanaSecuritySource,
    GmgnTokenInfoSource,
    GmgnTrenchesSource,
)
from .sources.dexscreener import DexScreenerSource
from .telegram import TelegramClient


OPEN_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(s|sec|secs|m|min|mins|h|hr|hrs|d|day|days)\s*ago$", re.I)
PERCENT_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*%$")
TOKEN_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
CURSOR_KEY = "gmgnsignals_fast_cursor_v1"


def _strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _number(value: Any, *, positive: bool = False) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result != result or result in (float("inf"), float("-inf")):
        return None
    if result < 0 or (positive and result <= 0):
        return None
    return result


def _open_minutes(value: Any) -> Optional[float]:
    if not isinstance(value, str):
        return None
    match = OPEN_RE.fullmatch(value.strip())
    if match is None:
        return None
    number = float(match.group(1))
    unit = match.group(2).lower()
    if unit in {"s", "sec", "secs"}:
        return number / 60
    if unit in {"h", "hr", "hrs"}:
        return number * 60
    if unit in {"d", "day", "days"}:
        return number * 1440
    return number


def _percentage_points_to_ratio(value: Any) -> Optional[float]:
    if not isinstance(value, str):
        return None
    match = PERCENT_RE.fullmatch(value.strip())
    if match is None:
        return None
    percentage = float(match.group(1))
    if not 0 <= percentage <= 100:
        return None
    return percentage / 100


def _latest(rows: list[Dict[str, Any]], key: str) -> Optional[float]:
    for row in reversed(rows):
        value = _number(row.get(key))
        if value is not None:
            return value
    return None


def _money(value: float) -> str:
    if value >= 1_000_000:
        return "$%.2fM" % (value / 1_000_000)
    if value >= 1_000:
        return "$%.2fK" % (value / 1_000)
    return "$%.0f" % value


def _telegram(env_file: Path, enabled: bool) -> Optional[TelegramClient]:
    if not enabled:
        return None
    credentials = load_credentials(env_file)
    if not credentials.telegram_bot_token or not credentials.telegram_chat_id:
        raise RuntimeError("Sol early Telegram credentials missing")
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


class SolEarlyNotifier:
    def __init__(
        self,
        *,
        raw_path: Path,
        db_path: Path,
        health_path: Path,
        policy_path: Path,
        telegram_env: Path,
        dry_run: bool = False,
    ) -> None:
        self.raw_path = raw_path
        self.health_path = health_path
        self.policy = json.loads(policy_path.read_text(encoding="utf-8"))
        if self.policy.get("schema_version") != "sol_early_information_policy_v1":
            raise ValueError("Sol early policy schema mismatch")
        self.connection = connect(db_path)
        initialize(self.connection)
        self.dry_run = dry_run
        flag = os.environ.get("MEME_RADAR_SOL_EARLY_TELEGRAM_ENABLED", "0").strip()
        if flag not in {"0", "1"}:
            raise ValueError("invalid Sol early Telegram flag")
        self.telegram_enabled = flag == "1" and not dry_run
        self.telegram = _telegram(telegram_env, self.telegram_enabled)
        poll = int(os.environ.get("MEME_RADAR_SOL_EARLY_POLL_SECONDS", "2"))
        provider_calls = int(
            os.environ.get("MEME_RADAR_SOL_EARLY_PROVIDER_CALLS_PER_HOUR", "24")
        )
        if not 1 <= poll <= 30 or not 3 <= provider_calls <= 120:
            raise ValueError("invalid Sol early runtime limits")
        self.poll_seconds = poll
        self.provider_call_limit = provider_calls
        self.info = GmgnTokenInfoSource(cache_seconds=60)
        self.trenches = GmgnTrenchesSource(cache_seconds=30)
        self.security = GmgnSolanaSecuritySource()
        self.dex = DexScreenerSource(
            BoundedJsonClient(
                allowed_hosts={"api.dexscreener.com"},
                timeout_seconds=10,
                max_bytes=512 * 1024,
            )
        )
        self.stop_event: Optional[asyncio.Event] = None
        self.counters: Counter[str] = Counter()
        self.last_error: Optional[Dict[str, Any]] = None
        self.last_success: Dict[str, int] = {}
        self.started_at_ms = int(time.time() * 1000)

    def initialize_cursor(self, now_ms: int) -> bool:
        if get_state(self.connection, CURSOR_KEY) is not None:
            return False
        stat = self.raw_path.stat()
        set_state(
            self.connection,
            CURSOR_KEY,
            {"device": stat.st_dev, "inode": stat.st_ino, "offset": stat.st_size},
            now_ms,
        )
        self.counters["cursor_initialized_at_eof"] += 1
        return True

    def read_new(
        self, now_ms: int, max_rows: int = 200
    ) -> list[tuple[Optional[dict[str, Any]], int, int, int]]:
        if self.initialize_cursor(now_ms):
            return []
        state = get_state(self.connection, CURSOR_KEY)
        offset = int(state.get("offset", 0))
        descriptor = os.open(
            self.raw_path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        opened = os.fstat(descriptor)
        if (state.get("device"), state.get("inode")) != (opened.st_dev, opened.st_ino):
            os.close(descriptor)
            raise RuntimeError("Sol early raw identity changed")
        if offset > opened.st_size:
            os.close(descriptor)
            raise RuntimeError("Sol early raw file shrank")
        rows: list[tuple[Optional[dict[str, Any]], int, int, int]] = []
        with os.fdopen(descriptor, "rb") as handle:
            handle.seek(offset)
            while len(rows) < max_rows:
                start = handle.tell()
                raw = handle.readline(1024 * 1024 + 1)
                if not raw:
                    break
                if len(raw) > 1024 * 1024 or not raw.endswith(b"\n"):
                    handle.seek(start)
                    break
                offset = handle.tell()
                try:
                    value = json.loads(raw.decode("utf-8"), object_pairs_hook=_strict_object)
                except (UnicodeDecodeError, ValueError):
                    self.counters["invalid_json"] += 1
                    rows.append((None, offset, opened.st_dev, opened.st_ino))
                    continue
                rows.append(
                    (
                        value if isinstance(value, dict) else None,
                        offset,
                        opened.st_dev,
                        opened.st_ino,
                    )
                )
        self.counters["raw_rows"] += len(rows)
        return rows

    def commit_cursor(
        self, offset: int, device: int, inode: int, now_ms: int
    ) -> None:
        stat = self.raw_path.lstat()
        if (stat.st_dev, stat.st_ino) != (device, inode):
            raise RuntimeError("Sol early raw changed before cursor commit")
        set_state(
            self.connection,
            CURSOR_KEY,
            {"device": device, "inode": inode, "offset": offset},
            now_ms,
        )

    def compact_signal(self, row: dict[str, Any], now_ms: int) -> Optional[dict[str, Any]]:
        source = self.policy["source"]
        if row.get("schema_version") != source["schema_version"]:
            self.counters["wrong_schema"] += 1
            return None
        if row.get("is_backfill") is True or str(row.get("mode", "")).lower() == "bootstrap":
            self.counters["backfill"] += 1
            return None
        if row.get("is_edit") is True:
            self.counters["edit"] += 1
            return None
        posted = row.get("posted_at_ms")
        seen = row.get("fast_seen_at_ms")
        if not isinstance(posted, int) or not isinstance(seen, int):
            self.counters["invalid_time"] += 1
            return None
        delay = seen - posted
        if delay > source["max_posted_to_seen_ms"] or delay < -source["max_future_skew_ms"]:
            self.counters["stale_ingest"] += 1
            return None
        if now_ms - seen > source["max_signal_age_ms"] or now_ms < seen - source["max_future_skew_ms"]:
            self.counters["stale_signal"] += 1
            return None
        parsed = row.get("parsed") if isinstance(row.get("parsed"), dict) else {}
        fields = row.get("structured_fields") if isinstance(row.get("structured_fields"), dict) else {}
        token = str(parsed.get("token_ca") or row.get("token_ca") or "")
        message = str(row.get("message_id") or "")
        card = str(parsed.get("card_type") or "").strip()
        age = _open_minutes(fields.get("open"))
        allowed = set(self.policy["discovery_gate"]["allowed_card_types"])
        if not TOKEN_RE.fullmatch(token) or not message or card not in allowed | {"Dev Sold"}:
            self.counters["invalid_identity_or_card"] += 1
            return None
        if age is None or not 0 <= age <= source["max_token_age_minutes"]:
            self.counters["token_age"] += 1
            return None
        signal_id = hashlib.sha256(f"{message}:{token}:{seen}".encode()).hexdigest()
        return {
            "signal_id": signal_id,
            "token_address": token,
            "seen_at_ms": seen,
            "card_type": card,
            "market_cap_usd": _number(parsed.get("market_cap"), positive=True),
            "liquidity_usd": _number(parsed.get("liquidity"), positive=True),
            "top10_percent": _percentage_points_to_ratio(
                fields.get("top10_percent")
            ),
            "total_fee_sol": _number(parsed.get("total_fee_sol")),
            "token_age_minutes": age,
        }

    def candidate(self, signal: dict[str, Any]) -> Optional[dict[str, Any]]:
        gate = self.policy["confirmed_gate"]
        rows = recent_signals(
            self.connection,
            signal["token_address"],
            signal["seen_at_ms"] - gate["rolling_window_minutes"] * 60_000,
        )
        safe = [row for row in rows if row["card_type"] != "Dev Sold"]
        cards = sorted({row["card_type"] for row in safe})
        combo = "+".join(cards)
        if any(row["card_type"] == "Dev Sold" for row in rows):
            self.counters["dev_sold"] += 1
            return None
        if len(safe) < gate["min_safe_events"] or len(cards) < gate["min_unique_card_types"]:
            return None
        if combo not in set(gate["allowed_card_combinations"]):
            self.counters["combination"] += 1
            return None
        result = dict(signal)
        market_cap = _latest(rows, "market_cap_usd")
        liquidity = _latest(rows, "liquidity_usd")
        top10 = _latest(rows, "top10_percent")
        discovery = self.policy["discovery_gate"]
        discovery_checks = (
            (
                "market_cap",
                market_cap,
                discovery["min_market_cap_usd"],
                discovery["max_market_cap_usd"],
            ),
            ("liquidity", liquidity, discovery["min_liquidity_usd"], None),
            ("top10", top10, None, discovery["max_top10_percent"]),
        )
        for name, value, minimum, maximum in discovery_checks:
            if value is not None and (
                (minimum is not None and value < minimum)
                or (maximum is not None and value > maximum)
            ):
                self.counters[f"discovery_{name}_blocked"] += 1
                return None
        result.update(
            {
                "cards": cards,
                "market_cap_usd": market_cap,
                "liquidity_usd": liquidity,
                "top10_percent": top10,
                "total_fee_sol": _latest(rows, "total_fee_sol"),
            }
        )
        return result

    async def evidence(
        self, candidate: dict[str, Any]
    ) -> tuple[Optional[dict[str, Any]], str, bool, Optional[int]]:
        token = candidate["token_address"]
        now_ms = int(time.time() * 1000)
        info_cost = 0 if self.info.has_fresh_cache("solana", token) else 1
        if info_cost and not reserve_provider_calls(
            self.connection, now_ms=now_ms, cost=info_cost,
            limit_per_hour=self.provider_call_limit,
        ):
            self.counters["provider_budget"] += 1
            return None, "PROVIDER_BUDGET", True, provider_next_available_ms(
                self.connection, now_ms=now_ms, cost=info_cost,
                limit_per_hour=self.provider_call_limit,
            )
        try:
            info = await asyncio.to_thread(self.info.token_info, "solana", token)
        except (GmgnCliError, HttpBoundaryError, OSError, RuntimeError, ValueError) as exc:
            return self._evidence_error(exc)
        if info is None:
            self.counters["info_missing"] += 1
            return None, "INFO_MISSING", True, None
        gate = self.policy["confirmed_gate"]
        info_fields = {
            "market_cap": info.market_cap_usd,
            "liquidity": info.liquidity_usd,
            "total_fee": info.total_fee,
        }
        missing_info = [name for name, value in info_fields.items() if value is None]
        if missing_info:
            for name in missing_info:
                self.counters[f"evidence_missing_{name}"] += 1
            return None, "MISSING:" + ",".join(missing_info), True, None
        info_quality = {
            "market_cap": gate["min_market_cap_usd"]
            <= info.market_cap_usd
            <= gate["max_market_cap_usd"],
            "liquidity": info.liquidity_usd >= gate["min_liquidity_usd"],
            "total_fee": gate["min_total_fee_sol"]
            <= info.total_fee
            <= gate["max_total_fee_sol"],
        }
        failed_quality = [name for name, passed in info_quality.items() if not passed]
        if failed_quality:
            for name in failed_quality:
                self.counters[f"quality_blocked_{name}"] += 1
            return None, "QUALITY:" + ",".join(failed_quality), False, None
        trenches_cost = 0 if self.trenches.has_fresh_cache("solana") else 1
        if trenches_cost and not reserve_provider_calls(
            self.connection, now_ms=now_ms, cost=trenches_cost,
            limit_per_hour=self.provider_call_limit,
        ):
            self.counters["provider_budget"] += 1
            return None, "PROVIDER_BUDGET", True, provider_next_available_ms(
                self.connection, now_ms=now_ms, cost=trenches_cost,
                limit_per_hour=self.provider_call_limit,
            )
        try:
            trenches = await asyncio.to_thread(
                self.trenches.token_safety, "solana", token
            )
        except (GmgnCliError, HttpBoundaryError, OSError, RuntimeError, ValueError) as exc:
            return self._evidence_error(exc)
        if trenches is None:
            self.counters["trenches_missing"] += 1
            return None, "TRENCHES_MISSING", True, None
        now_ms = int(time.time() * 1000)
        if not reserve_provider_calls(
            self.connection, now_ms=now_ms, cost=2,
            limit_per_hour=self.provider_call_limit,
        ):
            self.counters["provider_budget"] += 1
            return None, "PROVIDER_BUDGET", True, provider_next_available_ms(
                self.connection, now_ms=now_ms, cost=2,
                limit_per_hour=self.provider_call_limit,
            )
        try:
            security, market = await asyncio.gather(
                asyncio.to_thread(self.security.token_security, token),
                asyncio.to_thread(self.dex.token_market, "solana", token),
            )
        except (GmgnCliError, HttpBoundaryError, OSError, RuntimeError, ValueError) as exc:
            return self._evidence_error(exc)
        safety = self.policy["safety_gate"]
        now_ms = int(time.time() * 1000)
        cap = info.market_cap_usd
        liquidity = info.liquidity_usd
        top10 = trenches.top_10_holder_rate
        total_fee = info.total_fee
        risk_map = {
            "rug_ratio": trenches.rug_ratio,
            "bundler_rate": trenches.bundler_rate,
            "insider_rate": trenches.insider_rate,
            "entrapment_ratio": trenches.entrapment_ratio,
            "dev_team_hold_rate": trenches.dev_team_hold_rate,
        }
        required_ratios = safety["required_risk_ratios"]
        if not isinstance(required_ratios, list) or any(
            name not in risk_map for name in required_ratios
        ):
            raise ValueError("invalid Sol early risk-ratio policy")
        risk_values = [risk_map[name] for name in required_ratios]
        required = {
            "market_cap": cap,
            "liquidity": liquidity,
            "top10": top10,
            "total_fee": total_fee,
            "risk_level": security.risk_level,
            "honeypot": security.is_honeypot,
            "blacklist": security.is_blacklisted,
            "renounced_mint": security.renounced_mint,
            "renounced_freeze": security.renounced_freeze,
            "trenches_renounced_mint": trenches.renounced_mint,
            "trenches_renounced_freeze": trenches.renounced_freeze,
            "creator_status": trenches.creator_token_status or None,
            **risk_map,
        }
        missing = [name for name, value in required.items() if value is None]
        if missing:
            for name in missing:
                self.counters[f"evidence_missing_{name}"] += 1
            return None, "MISSING:" + ",".join(missing), True, None
        if market.pair_count == 0:
            self.counters["dex_pair_missing"] += 1
            return None, "DEX_PAIR_MISSING", True, None
        if market.buy_transactions_5m is None or market.sell_transactions_5m is None:
            self.counters["dex_activity_missing"] += 1
            return None, "DEX_ACTIVITY_MISSING", True, None
        future_skew = self.policy["source"]["max_future_skew_ms"]
        checks = {
            "identity": trenches.chain == "solana" and trenches.token_address == token,
            "info_identity": info.chain == "solana" and info.token_address == token,
            "security_identity": security.chain == "solana"
            and security.token_address == token,
            "market_identity": market.chain == "solana" and market.token_address == token,
            "market_cap": gate["min_market_cap_usd"] <= cap <= gate["max_market_cap_usd"],
            "liquidity": liquidity >= gate["min_liquidity_usd"],
            "top10": top10 <= gate["max_top10_percent"],
            "total_fee": gate["min_total_fee_sol"] <= total_fee <= gate["max_total_fee_sol"],
            "risk_level": security.risk_level == safety["required_risk_level"],
            "honeypot": security.is_honeypot is safety["is_honeypot"],
            "blacklist": security.is_blacklisted is safety["is_blacklisted"],
            "renounced": (
                security.renounced_mint is safety["renounced_mint"]
                and security.renounced_freeze is safety["renounced_freeze"]
                and trenches.renounced_mint is safety["renounced_mint"]
                and trenches.renounced_freeze is safety["renounced_freeze"]
            ),
            "dev_unsold": trenches.creator_token_status.lower() == "creator_hold",
            "risk_ratios": all(
                value is not None and value <= safety["max_risk_ratio"]
                for value in risk_values
            ),
            "dex_buys": market.buy_transactions_5m > 0,
            "dex_sells": market.sell_transactions_5m > 0,
            "trenches_fresh": -future_skew
            <= now_ms - trenches.observed_at_ms
            <= safety["gmgn_evidence_max_age_ms"],
            "info_fresh": -future_skew
            <= now_ms - info.observed_at_ms
            <= safety["gmgn_evidence_max_age_ms"],
            "security_fresh": -future_skew
            <= now_ms - security.observed_at_ms
            <= safety["gmgn_evidence_max_age_ms"],
            "dex_fresh": -future_skew
            <= now_ms - market.observed_at_ms
            <= safety["dex_evidence_max_age_ms"],
        }
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            for name in failed:
                self.counters[f"safety_blocked_{name}"] += 1
            return None, "SAFETY:" + ",".join(failed), False, None
        result = dict(candidate)
        result.update(
            {
                "market_cap_usd": cap,
                "liquidity_usd": liquidity,
                "top10_percent": top10,
                "total_fee_sol": total_fee,
                "buys_5m": market.buy_transactions_5m,
                "sells_5m": market.sell_transactions_5m,
                "valid_until_ms": min(
                    candidate["seen_at_ms"]
                    + self.policy["source"]["max_signal_age_ms"],
                    info.observed_at_ms + safety["gmgn_evidence_max_age_ms"],
                    trenches.observed_at_ms + safety["gmgn_evidence_max_age_ms"],
                    security.observed_at_ms + safety["gmgn_evidence_max_age_ms"],
                    market.observed_at_ms + safety["dex_evidence_max_age_ms"],
                ),
            }
        )
        return result, "PASS", False, None

    def _evidence_error(
        self, exc: Exception
    ) -> tuple[None, str, bool, None]:
        self.counters["evidence_error"] += 1
        failed_at = int(time.time() * 1000)
        if isinstance(exc, GmgnCliError) and exc.status in {401, 403}:
            set_provider_cooldown(self.connection, now_ms=failed_at, seconds=1800)
        elif (
            isinstance(exc, GmgnCliError) and exc.status == 429
        ) or (
            isinstance(exc, HttpBoundaryError) and exc.status == 429
        ):
            set_provider_cooldown(self.connection, now_ms=failed_at, seconds=300)
        self.last_error = {"at_ms": failed_at, "type": type(exc).__name__}
        return None, type(exc).__name__, True, None

    def format_message(self, value: dict[str, Any]) -> str:
        cards = " + ".join(html.escape(card) for card in value["cards"])
        token = html.escape(value["token_address"])
        return "\n".join(
            [
                "🚨 <b>SOL-EARLY-CONFIRMED</b>",
                f"📡 5分钟信号：{cards}",
                f"💰 市值 {_money(value['market_cap_usd'])} · 💧流动性 {_money(value['liquidity_usd'])}",
                f"👥 Top10 {value['top10_percent'] * 100:.1f}% · Fee {value['total_fee_sol']:.2f} SOL",
                f"📈 DEX 5m：{value['buys_5m']} 买 / {value['sells_5m']} 卖",
                "🛡️ GMGN安全、Mint/Freeze权限及DEX活动已通过",
                f"<code>{token}</code>",
                f"🔗 https://dexscreener.com/solana/{token}",
                "⚠️ 信息雷达提醒，不构成投资建议。",
            ]
        )[:4096]

    async def process_row(self, row: dict[str, Any], now_ms: int) -> None:
        signal_value = self.compact_signal(row, now_ms)
        if signal_value is None:
            return
        inserted = store_signal(self.connection, **{k: signal_value[k] for k in (
            "signal_id", "token_address", "seen_at_ms", "card_type",
            "market_cap_usd", "liquidity_usd", "top10_percent", "total_fee_sol"
        )})
        if not inserted:
            self.counters["duplicate_signal"] += 1
        else:
            self.counters["signals"] += 1
        candidate = self.candidate(signal_value)
        if candidate is None:
            return
        status = schedule_candidate(
            self.connection,
            token_address=candidate["token_address"],
            detected_at_ms=candidate["seen_at_ms"],
            expires_at_ms=candidate["seen_at_ms"]
            + self.policy["source"]["max_signal_age_ms"],
        )
        self.counters[status] += 1

    async def process_candidate(self, now_ms: int) -> None:
        job = claim_candidate(self.connection, now_ms=now_ms)
        if job is None:
            return
        token = job["token_address"]
        if job["state"] == "expired":
            finish_candidate(
                self.connection, token, state="expired", result="DEADLINE"
            )
            self.counters["candidate_expired"] += 1
            return
        candidate = self.candidate(
            {"token_address": token, "seen_at_ms": int(job["detected_at_ms"])}
        )
        if candidate is None:
            finish_candidate(
                self.connection, token, state="blocked", result="WINDOW_CHANGED"
            )
            return
        evidence, reason, retryable, retry_at_ms = await self.evidence(candidate)
        current_ms = int(time.time() * 1000)
        if current_ms > int(job["expires_at_ms"]):
            finish_candidate(
                self.connection, token, state="expired", result="EVIDENCE_DEADLINE"
            )
            return
        if evidence is None:
            if reason == "PROVIDER_BUDGET":
                defer_candidate_without_attempt(
                    self.connection,
                    token,
                    result=reason,
                    next_attempt_at_ms=min(
                        retry_at_ms or current_ms + 1_000,
                        int(job["expires_at_ms"]) + 1,
                    ),
                )
                self.counters["candidate_budget_deferred"] += 1
                return
            if retryable and int(job["attempts"]) < 3:
                finish_candidate(
                    self.connection,
                    token,
                    state="pending",
                    result=reason,
                    next_attempt_at_ms=current_ms + 15_000,
                )
            else:
                finish_candidate(
                    self.connection, token, state="blocked", result=reason
                )
            return
        status = claim_notification(
            self.connection,
            token_address=token,
            message_text=self.format_message(evidence),
            now_ms=current_ms,
            valid_until_ms=evidence["valid_until_ms"],
        )
        finish_candidate(
            self.connection, token, state="queued", result=status.upper()
        )
        self.counters[status] += 1

    async def deliver(self, now_ms: int) -> None:
        item = claim_due(self.connection, now_ms=now_ms)
        if item is None:
            return
        if self.dry_run:
            mark_sent(self.connection, item["token_address"], now_ms)
            self.counters["dry_run_sent"] += 1
            return
        if self.telegram is None:
            mark_unknown(
                self.connection,
                item["token_address"],
                error_code="TELEGRAM_DISABLED",
            )
            return
        if now_ms > int(item["valid_until_ms"]):
            mark_unknown(
                self.connection, item["token_address"], error_code="DELIVERY_EXPIRED"
            )
            self.counters["delivery_expired"] += 1
            return
        try:
            result = await asyncio.to_thread(
                self.telegram.send_message, item["message_text"]
            )
            mark_sent(self.connection, item["token_address"], result.received_at_ms)
            self.counters["telegram_sent"] += 1
            self.last_success["telegram"] = result.received_at_ms
        except Exception as exc:
            mark_unknown(
                self.connection,
                item["token_address"],
                error_code=type(exc).__name__,
            )
            self.counters["telegram_unknown"] += 1
            self.last_error = {"at_ms": now_ms, "type": type(exc).__name__}

    def health(self) -> Dict[str, Any]:
        cursor = get_state(self.connection, CURSOR_KEY, {})
        return {
            "schema_version": 1,
            "policy_version": self.policy["policy_version"],
            "status": "running",
            "updated_at_ms": int(time.time() * 1000),
            "started_at_ms": self.started_at_ms,
            "telegram_enabled": self.telegram_enabled,
            "rate_limit_enabled": False,
            "dedupe_enabled": True,
            "read_only_source": True,
            "no_signing": True,
            "no_broadcast": True,
            "cursor_offset": cursor.get("offset"),
            "counters": dict(sorted(self.counters.items())),
            "provider_budget": provider_budget_state(self.connection),
            "outbox": counts(self.connection),
            "last_success": self.last_success,
            "last_error": self.last_error,
        }

    async def run(self, run_seconds: int = 0) -> None:
        if self.stop_event is None:
            self.stop_event = asyncio.Event()
        deadline = time.monotonic() + run_seconds if run_seconds else None
        while not self.stop_event.is_set():
            now_ms = int(time.time() * 1000)
            for row, offset, device, inode in self.read_new(now_ms):
                if row is not None:
                    await self.process_row(row, int(time.time() * 1000))
                self.commit_cursor(
                    offset, device, inode, int(time.time() * 1000)
                )
            await self.process_candidate(int(time.time() * 1000))
            await self.deliver(int(time.time() * 1000))
            write_health(self.health_path, self.health())
            if deadline is not None and time.monotonic() >= deadline:
                break
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.poll_seconds)
            except asyncio.TimeoutError:
                pass


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--telegram-env", type=Path, required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--health-path", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--run-seconds", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> int:
    notifier = SolEarlyNotifier(
        raw_path=args.raw,
        db_path=args.db_path,
        health_path=args.health_path,
        policy_path=args.policy,
        telegram_env=args.telegram_env,
        dry_run=args.dry_run,
    )
    notifier.stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, notifier.stop_event.set)
        except NotImplementedError:
            pass
    await notifier.run(args.run_seconds)
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main(arguments()))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "event": "SOL_EARLY_FATAL",
                    "error_type": type(exc).__name__,
                    "no_signing": True,
                    "no_broadcast": True,
                },
                sort_keys=True,
            ),
            file=os.sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
    finish_candidate,
