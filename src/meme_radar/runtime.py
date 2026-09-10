from __future__ import annotations

import json
import os
import tempfile
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict

from .credentials import load_env_file


@dataclass(frozen=True)
class RuntimeSettings:
    db_path: Path
    health_path: Path
    fomo_poll_seconds: int
    clanker_poll_seconds: int
    enrich_max_per_hour: int
    backfill_max_minutes: int
    fomo_enabled: bool = True
    bitquery_enabled: bool = True
    sample_enabled: bool = False
    sample_max_per_hour: int = 3
    telegram_enabled: bool = False
    telegram_min_interval_seconds: int = 3
    telegram_max_attempts: int = 5
    telegram_max_per_hour: int = 20
    native_rpc_mode: str = "off"
    native_rpc_backfill_blocks: int = 500
    native_rpc_primary_probe_seconds: int = 60
    native_rpc_primary_recovery_successes: int = 3
    gmgn_enabled: bool = False
    gmgn_max_per_hour: int = 180
    gmgn_cache_seconds: int = 60
    gmgn_discovery_enabled: bool = False
    gmgn_discovery_poll_seconds: int = 120
    gmgn_discovery_max_per_hour: int = 90

    @classmethod
    def from_env_file(cls, path: Path) -> "RuntimeSettings":
        values = load_env_file(path)

        def value(key: str, default: str) -> str:
            return os.environ.get(key, values.get(key, default)).strip()

        telegram_enabled = value("MEME_RADAR_TELEGRAM_ENABLED", "0")
        if telegram_enabled not in {"0", "1"}:
            raise ValueError("invalid Telegram enabled flag")
        fomo_enabled = value("MEME_RADAR_FOMO_ENABLED", "1")
        if fomo_enabled not in {"0", "1"}:
            raise ValueError("invalid FOMO enabled flag")
        bitquery_enabled = value("MEME_RADAR_BITQUERY_ENABLED", "1")
        if bitquery_enabled not in {"0", "1"}:
            raise ValueError("invalid Bitquery enabled flag")
        sample_enabled = value("MEME_RADAR_SAMPLE_ENABLED", "0")
        if sample_enabled not in {"0", "1"}:
            raise ValueError("invalid sample enabled flag")
        native_rpc_mode = value("MEME_RADAR_NATIVE_RPC_MODE", "off")
        if native_rpc_mode not in {"off", "shadow", "primary"}:
            raise ValueError("invalid native RPC mode")
        gmgn_enabled = value("MEME_RADAR_GMGN_ENABLED", "0")
        if gmgn_enabled not in {"0", "1"}:
            raise ValueError("invalid GMGN enabled flag")
        gmgn_discovery_enabled = value(
            "MEME_RADAR_GMGN_DISCOVERY_ENABLED", "0"
        )
        if gmgn_discovery_enabled not in {"0", "1"}:
            raise ValueError("invalid GMGN discovery enabled flag")

        settings = cls(
            db_path=Path(value("MEME_RADAR_DB_PATH", "data/meme-radar.db")),
            health_path=Path(
                value("MEME_RADAR_HEALTH_PATH", "data/health.json")
            ),
            fomo_poll_seconds=int(
                value("MEME_RADAR_FOMO_POLL_SECONDS", "300")
            ),
            clanker_poll_seconds=int(
                value("MEME_RADAR_CLANKER_POLL_SECONDS", "60")
            ),
            enrich_max_per_hour=int(
                value("MEME_RADAR_ENRICH_MAX_PER_HOUR", "30")
            ),
            backfill_max_minutes=int(
                value("MEME_RADAR_BACKFILL_MAX_MINUTES", "10")
            ),
            fomo_enabled=fomo_enabled == "1",
            bitquery_enabled=bitquery_enabled == "1",
            sample_enabled=sample_enabled == "1",
            sample_max_per_hour=int(
                value("MEME_RADAR_SAMPLE_MAX_PER_HOUR", "3")
            ),
            telegram_enabled=telegram_enabled == "1",
            telegram_min_interval_seconds=int(
                value("MEME_RADAR_TELEGRAM_MIN_INTERVAL_SECONDS", "3")
            ),
            telegram_max_attempts=int(
                value("MEME_RADAR_TELEGRAM_MAX_ATTEMPTS", "5")
            ),
            telegram_max_per_hour=int(
                value("MEME_RADAR_TELEGRAM_MAX_PER_HOUR", "20")
            ),
            native_rpc_mode=native_rpc_mode,
            native_rpc_backfill_blocks=int(
                value("MEME_RADAR_NATIVE_RPC_BACKFILL_BLOCKS", "500")
            ),
            native_rpc_primary_probe_seconds=int(
                value("MEME_RADAR_NATIVE_RPC_PRIMARY_PROBE_SECONDS", "60")
            ),
            native_rpc_primary_recovery_successes=int(
                value("MEME_RADAR_NATIVE_RPC_PRIMARY_RECOVERY_SUCCESSES", "3")
            ),
            gmgn_enabled=gmgn_enabled == "1",
            gmgn_max_per_hour=int(
                value("MEME_RADAR_GMGN_MAX_PER_HOUR", "180")
            ),
            gmgn_cache_seconds=int(
                value("MEME_RADAR_GMGN_CACHE_SECONDS", "60")
            ),
            gmgn_discovery_enabled=gmgn_discovery_enabled == "1",
            gmgn_discovery_poll_seconds=int(
                value("MEME_RADAR_GMGN_DISCOVERY_POLL_SECONDS", "120")
            ),
            gmgn_discovery_max_per_hour=int(
                value("MEME_RADAR_GMGN_DISCOVERY_MAX_PER_HOUR", "90")
            ),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not 60 <= self.fomo_poll_seconds <= 3600:
            raise ValueError("FOMO poll interval outside 60..3600")
        if not 30 <= self.clanker_poll_seconds <= 3600:
            raise ValueError("Clanker poll interval outside 30..3600")
        if not 1 <= self.enrich_max_per_hour <= 300:
            raise ValueError("enrichment budget outside 1..300")
        if not 1 <= self.backfill_max_minutes <= 60:
            raise ValueError("backfill window outside 1..60")
        if not 1 <= self.telegram_min_interval_seconds <= 60:
            raise ValueError("Telegram interval outside 1..60")
        if not 1 <= self.telegram_max_attempts <= 10:
            raise ValueError("Telegram attempts outside 1..10")
        if not 1 <= self.telegram_max_per_hour <= 60:
            raise ValueError("Telegram hourly limit outside 1..60")
        if not 1 <= self.sample_max_per_hour <= 10:
            raise ValueError("sample hourly limit outside 1..10")
        if not 10 <= self.native_rpc_backfill_blocks <= 10_000:
            raise ValueError("native RPC backfill outside 10..10000")
        if not 15 <= self.native_rpc_primary_probe_seconds <= 600:
            raise ValueError("native RPC primary probe outside 15..600 seconds")
        if not 2 <= self.native_rpc_primary_recovery_successes <= 10:
            raise ValueError("native RPC recovery successes outside 2..10")
        if not 1 <= self.gmgn_max_per_hour <= 300:
            raise ValueError("GMGN budget outside 1..300")
        if not 30 <= self.gmgn_cache_seconds <= 600:
            raise ValueError("GMGN cache outside 30..600 seconds")
        if not 60 <= self.gmgn_discovery_poll_seconds <= 600:
            raise ValueError("GMGN discovery poll outside 60..600 seconds")
        if not 3 <= self.gmgn_discovery_max_per_hour <= 180:
            raise ValueError("GMGN discovery budget outside 3..180")
        if self.gmgn_discovery_enabled and not self.gmgn_enabled:
            raise ValueError("GMGN discovery requires GMGN enabled")
        if not self.bitquery_enabled and self.native_rpc_mode != "primary":
            raise ValueError(
                "Bitquery can be disabled only with native RPC primary"
            )
        for path in (self.db_path, self.health_path):
            if not str(path) or path == Path("/"):
                raise ValueError("unsafe runtime path")


class SlidingBudget:
    def __init__(
        self,
        max_calls: int,
        window_seconds: float = 3600,
        now=time.monotonic,
    ) -> None:
        if max_calls <= 0 or window_seconds <= 0:
            raise ValueError("invalid budget")
        self._max_calls = max_calls
        self._window_seconds = window_seconds
        self._now = now
        self._calls: Deque[float] = deque()
        self._cooldown_until = 0.0

    def _prune(self, now: float) -> None:
        while self._calls and self._calls[0] <= now - self._window_seconds:
            self._calls.popleft()

    def available(self) -> bool:
        now = self._now()
        self._prune(now)
        return now >= self._cooldown_until and len(self._calls) < self._max_calls

    def take(self) -> bool:
        now = self._now()
        self._prune(now)
        if now < self._cooldown_until or len(self._calls) >= self._max_calls:
            return False
        self._calls.append(now)
        return True

    def cooldown(self, seconds: float) -> None:
        if seconds <= 0:
            raise ValueError("invalid cooldown")
        self._cooldown_until = max(
            self._cooldown_until,
            self._now() + seconds,
        )

    def snapshot(self) -> Dict[str, Any]:
        now = self._now()
        self._prune(now)
        return {
            "used": len(self._calls),
            "limit": self._max_calls,
            "window_seconds": int(self._window_seconds),
            "cooldown_seconds": max(0, int(self._cooldown_until - now)),
        }


def write_health(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    descriptor, temporary = tempfile.mkstemp(
        prefix=".health.",
        dir=str(path.parent),
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise
