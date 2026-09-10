from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from .credentials import load_credentials
from .http_json import BoundedJsonClient, HttpBoundaryError
from .runtime import write_health
from .telegram import TelegramClient


HEALTH_MAX_BYTES = 64 * 1024
HEALTH_STALE_MS = 60_000
WSS_IDLE_MS = 120_000
RECENT_ERROR_MS = 5 * 60_000


def _read_health(path: Path) -> Dict[str, Any]:
    info = path.stat()
    if not path.is_file() or info.st_size <= 0 or info.st_size > HEALTH_MAX_BYTES:
        raise ValueError("invalid health file")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid health payload")
    return payload


def _recent_rpc_issues(payload: Mapping[str, Any], now_ms: int) -> Dict[str, str]:
    issues = {}
    for stage, value in (payload.get("last_error") or {}).items():
        if str(stage).startswith("native_primary_probe_"):
            continue
        if not isinstance(value, dict):
            continue
        observed = value.get("at_ms")
        status = value.get("status")
        if not isinstance(observed, int) or now_ms - observed > RECENT_ERROR_MS:
            continue
        if status in {403, 429} or isinstance(status, int) and status >= 500:
            code = "RPC_ERROR_%s_%s" % (str(stage).upper(), status)
            issues[code] = "%s 返回 HTTP %s" % (stage, status)
    return issues


def collect_issues(
    *,
    now_ms: int,
    main: Optional[Mapping[str, Any]],
    curve: Optional[Mapping[str, Any]],
    notifier: Optional[Mapping[str, Any]],
    metric_state: Dict[str, Any],
) -> Dict[str, str]:
    issues: Dict[str, str] = {}
    for label, payload in (("MAIN", main), ("CURVE", curve), ("NOTIFIER", notifier)):
        if payload is None:
            issues["HEALTH_MISSING_" + label] = label + " health 不可读取"
            continue
        updated = payload.get("updated_at_ms")
        if not isinstance(updated, int) or now_ms - updated > HEALTH_STALE_MS:
            issues["HEALTH_STALE_" + label] = label + " health 超过60秒未更新"
    if main is not None:
        chains = ((main.get("native_rpc") or {}).get("chains") or {})
        for chain in ("bsc", "base", "robinhood"):
            status = chains.get(chain) or {}
            if status.get("connected") is not True:
                issues["RPC_DISCONNECTED_" + chain.upper()] = (
                    chain.upper() + " RPC/WSS 已断开"
                )
            if status.get("slot") == "standby":
                issues["RPC_FAILOVER_" + chain.upper()] = (
                    chain.upper()
                    + " 主节点异常，备用RPC仍在正常采集和推送"
                )
        issues.update(_recent_rpc_issues(main, now_ms))
        if main.get("gmgn_discovery_enabled") is True:
            started = main.get("started_at_ms")
            old_enough = (
                isinstance(started, int) and now_ms - started > 5 * 60_000
            )
            for chain in ("bsc", "base", "solana"):
                last = (main.get("last_success") or {}).get(
                    "gmgn_discovery_" + chain
                )
                stale = (
                    not isinstance(last, int)
                    or now_ms - last > 5 * 60_000
                )
                if old_enough and stale:
                    issues["GMGN_DISCOVERY_STALE_" + chain.upper()] = (
                        "GMGN %s成长候选源超过5分钟没有成功更新"
                        % chain.upper()
                    )
        pending = int((main.get("telegram_outbox") or {}).get("pending") or 0)
        if pending > 0:
            issues["MAIN_OUTBOX_PENDING"] = "主推送 outbox 待发送 %d 条" % pending
    if curve is not None:
        if curve.get("connected") is not True:
            issues["PONS_CURVE_DISCONNECTED"] = "Pons curve 实时采集源已断开"
        wss_logs = int((curve.get("counters") or {}).get("wss_logs") or 0)
        previous = metric_state.get("pons_wss_logs")
        if previous != wss_logs:
            metric_state["pons_wss_logs"] = wss_logs
            metric_state["pons_wss_logs_changed_at_ms"] = now_ms
        changed_at = int(metric_state.get("pons_wss_logs_changed_at_ms") or now_ms)
        if curve.get("connected") is True and now_ms - changed_at > WSS_IDLE_MS:
            issues["PONS_WSS_IDLE"] = "Pons 实时采集超过120秒无新日志"
        issues.update(_recent_rpc_issues(curve, now_ms))
    if notifier is not None:
        if notifier.get("collector_ready") is not True:
            issues["PONS_COLLECTOR_NOT_READY"] = "Pons notifier collector_ready=false"
        pending = int((notifier.get("outbox") or {}).get("pending") or 0)
        if pending > 0:
            issues["PONS_OUTBOX_PENDING"] = "Pons outbox 待发送 %d 条" % pending
    return issues


class HealthAlerter:
    def __init__(
        self,
        *,
        telegram_env: Path,
        main_health: Path,
        curve_health: Path,
        notifier_health: Path,
        state_path: Path,
        health_path: Path,
        poll_seconds: int,
        alert_after_seconds: int,
        cooldown_seconds: int,
        dry_run: bool,
        recovery_after_seconds: int = 120,
    ) -> None:
        credentials = load_credentials(telegram_env)
        self.telegram = None if dry_run else TelegramClient(
            BoundedJsonClient(
                allowed_hosts={"api.telegram.org"},
                timeout_seconds=15,
                max_bytes=128 * 1024,
            ),
            bot_token=credentials.telegram_bot_token,
            chat_id=credentials.telegram_chat_id,
            topic_id=credentials.telegram_topic_id,
        )
        self.paths = {
            "main": main_health,
            "curve": curve_health,
            "notifier": notifier_health,
        }
        self.state_path = state_path
        self.health_path = health_path
        self.poll_seconds = poll_seconds
        self.alert_after_ms = alert_after_seconds * 1000
        self.cooldown_ms = cooldown_seconds * 1000
        self.recovery_after_ms = recovery_after_seconds * 1000
        self.dry_run = dry_run
        self.stop_event = asyncio.Event()
        self.counters: Dict[str, int] = {}
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        if not self.state_path.exists():
            return {"issues": {}, "metrics": {}}
        try:
            payload = _read_health(self.state_path)
        except (OSError, ValueError, json.JSONDecodeError):
            return {"issues": {}, "metrics": {}}
        return (
            payload
            if isinstance(payload.get("issues"), dict)
            else {"issues": {}, "metrics": {}}
        )

    def _increment(self, key: str) -> None:
        self.counters[key] = self.counters.get(key, 0) + 1

    def _send(self, text: str) -> bool:
        if self.dry_run:
            self._increment("dry_run_messages")
            return True
        if self.telegram is None:
            return False
        try:
            self.telegram.send_message(text)
            self._increment("telegram_sent")
            return True
        except (HttpBoundaryError, RuntimeError, OSError, ValueError):
            self._increment("telegram_errors")
            return False

    def poll_once(self, now_ms: int) -> None:
        payloads = {}
        for label, path in self.paths.items():
            try:
                payloads[label] = _read_health(path)
            except (OSError, ValueError, json.JSONDecodeError):
                payloads[label] = None
        metrics = self.state.setdefault("metrics", {})
        current = collect_issues(now_ms=now_ms, metric_state=metrics, **payloads)
        known = self.state.setdefault("issues", {})
        for code, summary in current.items():
            issue = known.setdefault(
                code,
                {"first_seen_ms": now_ms, "last_sent_ms": 0, "summary": summary},
            )
            issue["summary"] = summary
            issue.pop("clear_seen_ms", None)
            if now_ms - int(issue["first_seen_ms"]) < self.alert_after_ms:
                continue
            if now_ms - int(issue.get("last_sent_ms") or 0) < self.cooldown_ms:
                continue
            if self._send("🚨 Meme Radar 数据源异常\n%s\n代码: %s" % (summary, code)):
                issue["last_sent_ms"] = now_ms
                self._increment("alerts")
        for code in set(known) - set(current):
            issue = known[code]
            clear_seen = int(issue.setdefault("clear_seen_ms", now_ms))
            if now_ms - clear_seen < self.recovery_after_ms:
                continue
            if int(issue.get("last_sent_ms") or 0) > 0:
                if not self._send(
                    "✅ Meme Radar 数据源恢复\n%s\n代码: %s"
                    % (issue["summary"], code)
                ):
                    continue
                self._increment("recoveries")
            del known[code]
        self.state["updated_at_ms"] = now_ms
        write_health(self.state_path, self.state)
        write_health(
            self.health_path,
            {
                "status": "running",
                "updated_at_ms": now_ms,
                "dry_run": self.dry_run,
                "active_issues": sorted(current),
                "counters": dict(sorted(self.counters.items())),
                "no_signing": True,
                "no_broadcast": True,
            },
        )

    async def run(self, run_seconds: int = 0) -> None:
        timer = None
        if run_seconds:
            async def timed_stop() -> None:
                await asyncio.sleep(run_seconds)
                self.stop_event.set()
            timer = asyncio.create_task(timed_stop())
        while not self.stop_event.is_set():
            self.poll_once(int(time.time() * 1000))
            try:
                await asyncio.wait_for(self.stop_event.wait(), self.poll_seconds)
            except asyncio.TimeoutError:
                pass
        if timer is not None:
            timer.cancel()
            await asyncio.gather(timer, return_exceptions=True)


def arguments():
    parser = argparse.ArgumentParser(description="Meme Radar health Telegram alerts")
    parser.add_argument("--telegram-env", type=Path, required=True)
    parser.add_argument("--main-health", type=Path, required=True)
    parser.add_argument("--curve-health", type=Path, required=True)
    parser.add_argument("--notifier-health", type=Path, required=True)
    parser.add_argument("--state-path", type=Path, required=True)
    parser.add_argument("--health-path", type=Path, required=True)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--alert-after-seconds", type=int, default=60)
    parser.add_argument("--cooldown-seconds", type=int, default=1800)
    parser.add_argument("--recovery-after-seconds", type=int, default=120)
    parser.add_argument("--run-seconds", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


async def async_main(args) -> int:
    if not 5 <= args.poll_seconds <= 60:
        raise ValueError("poll seconds outside 5..60")
    if not 30 <= args.alert_after_seconds <= 600:
        raise ValueError("alert delay outside 30..600")
    if not 300 <= args.cooldown_seconds <= 86_400:
        raise ValueError("cooldown outside 300..86400")
    if not 30 <= args.recovery_after_seconds <= 600:
        raise ValueError("recovery delay outside 30..600")
    alerter = HealthAlerter(
        telegram_env=args.telegram_env,
        main_health=args.main_health,
        curve_health=args.curve_health,
        notifier_health=args.notifier_health,
        state_path=args.state_path,
        health_path=args.health_path,
        poll_seconds=args.poll_seconds,
        alert_after_seconds=args.alert_after_seconds,
        cooldown_seconds=args.cooldown_seconds,
        dry_run=args.dry_run,
        recovery_after_seconds=args.recovery_after_seconds,
    )
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(name, alerter.stop_event.set)
        except NotImplementedError:
            pass
    await alerter.run(args.run_seconds)
    return 0


def main() -> int:
    try:
        return asyncio.run(async_main(arguments()))
    except Exception as exc:
        print(
            json.dumps(
                {"event": "HEALTH_ALERT_FATAL", "error_type": type(exc).__name__},
                separators=(",", ":"),
            ),
            flush=True,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
