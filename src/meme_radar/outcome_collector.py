"""Post-hoc outcome collector sidecar.

Reads decisions the main radar has already made, freezes a stratified cohort,
and measures what happened to each member at fixed horizons. It never writes to
``radar_events``, ``decisions`` or ``telegram_outbox``, never sends anything,
and never influences scoring or delivery. Its only outputs are
``outcome_cohort`` and ``outcomes`` rows plus a health file.

The cohort design, the horizon definition and the "unavailable is not zero"
rule all live in :mod:`meme_radar.outcome_cohort`; this module is the loop and
the provider boundary around them.

Two operational properties matter.

*The market provider is budgeted.* Enrollment and measurement share one sliding
window budget. When it is exhausted the collector defers rather than dropping:
a horizon that is not read now stays due, and is recorded as ``expired`` with
its real elapsed time once it falls outside its tolerance. A budget shortfall
therefore shows up in the data as a visible gap instead of as missing rows.

*Enrollment is bounded by wall clock, not by backlog.* Only decisions from the
recent lookback window are enrolled, because the baseline can only be read
going forward. A collector that was down does not retroactively enroll the
decisions it missed; it starts clean, and the gap is visible in the cohort
timestamps.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .http_json import BoundedJsonClient, HttpBoundaryError
from .outcome_cohort import (
    COHORT_VERSION,
    HORIZON_MINUTES,
    cohort_counts,
    due_horizons,
    enroll_cohort_member,
    initialize_cohort,
    outcome_counts,
    select_enrollment_candidates,
    store_outcome,
)
from .runtime import SlidingBudget, write_health
from .normalize import normalize_address
from .sources.dexscreener import DEX_CHAIN_IDS, DexScreenerSource
from .storage import connect


SERVICE = "meme-radar-outcome-collector"
MARKET_SOURCE = "dexscreener"
DEXSCREENER_HOST = "api.dexscreener.com"

DEFAULT_POLL_SECONDS = 20
DEFAULT_ENROLL_LOOKBACK_SECONDS = 180
DEFAULT_PROVIDER_CALLS_PER_HOUR = 240
DEFAULT_CONTROL_SAMPLE_RATE = 0.001
DEFAULT_CONTROL_PER_CYCLE = 2
DEFAULT_ENROLL_PER_CYCLE = 25
DEFAULT_MEASURE_PER_CYCLE = 25


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError("invalid integer for %s" % name) from None
    if value < 0:
        raise ValueError("%s cannot be negative" % name)
    return value


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError("invalid number for %s" % name) from None
    if value != value or value < 0.0:
        raise ValueError("%s must be a non-negative number" % name)
    return value


class OutcomeCollector:
    def __init__(
        self,
        *,
        db_path: Path,
        health_path: Path,
        cohort_version: str = COHORT_VERSION,
        market: Optional[Any] = None,
        clock=None,
    ) -> None:
        self.db_path = Path(db_path)
        self.health_path = Path(health_path)
        self.cohort_version = cohort_version
        self.clock = clock or (lambda: int(time.time() * 1000))
        self.poll_seconds = _env_int(
            "MEME_RADAR_OUTCOME_POLL_SECONDS", DEFAULT_POLL_SECONDS
        )
        self.enroll_lookback_seconds = _env_int(
            "MEME_RADAR_OUTCOME_ENROLL_LOOKBACK_SECONDS",
            DEFAULT_ENROLL_LOOKBACK_SECONDS,
        )
        self.control_sample_rate = _env_float(
            "MEME_RADAR_OUTCOME_CONTROL_SAMPLE_RATE", DEFAULT_CONTROL_SAMPLE_RATE
        )
        if self.control_sample_rate > 1.0:
            raise ValueError("MEME_RADAR_OUTCOME_CONTROL_SAMPLE_RATE must be <= 1")
        self.control_per_cycle = _env_int(
            "MEME_RADAR_OUTCOME_CONTROL_PER_CYCLE", DEFAULT_CONTROL_PER_CYCLE
        )
        self.enroll_per_cycle = _env_int(
            "MEME_RADAR_OUTCOME_ENROLL_PER_CYCLE", DEFAULT_ENROLL_PER_CYCLE
        )
        self.measure_per_cycle = _env_int(
            "MEME_RADAR_OUTCOME_MEASURE_PER_CYCLE", DEFAULT_MEASURE_PER_CYCLE
        )
        if self.poll_seconds <= 0 or self.enroll_lookback_seconds <= 0:
            raise ValueError("poll and lookback must be positive")
        self.budget = SlidingBudget(
            _env_int(
                "MEME_RADAR_OUTCOME_PROVIDER_CALLS_PER_HOUR",
                DEFAULT_PROVIDER_CALLS_PER_HOUR,
            )
            or 1
        )
        self.market = market or DexScreenerSource(
            BoundedJsonClient(
                allowed_hosts=(DEXSCREENER_HOST,),
                timeout_seconds=10.0,
                max_bytes=512 * 1024,
            )
        )
        self.connection = None
        self.stop = False
        self.counters: Dict[str, int] = {}
        self.last_error: Dict[str, Any] = {}
        self.last_enrolled_at_ms: Optional[int] = None
        self.last_measured_at_ms: Optional[int] = None
        self.enroll_floor_ms: Optional[int] = None

    # -- boundary ---------------------------------------------------------

    def count(self, name: str, amount: int = 1) -> None:
        self.counters[name] = self.counters.get(name, 0) + amount

    def record_error(self, stage: str, code: str) -> None:
        self.last_error[stage] = {"at_ms": self.clock(), "code": code}
        self.count("error_" + code.lower())

    def read_market(self, chain: str, token_address: str) -> Dict[str, Any]:
        """One budgeted market read.

        Returns a status dict. ``deferred`` means the budget was exhausted and
        nothing was read; the caller must leave the work due rather than
        recording an empty measurement.
        """
        if chain not in DEX_CHAIN_IDS:
            self.count("skipped_unsupported_chain")
            return {"status": "unavailable", "error_code": "CHAIN_UNSUPPORTED"}
        try:
            normalize_address(chain, token_address)
        except ValueError:
            # Permanently unreadable, so it must not consume budget on every
            # horizon. Checked before take() for that reason.
            self.count("skipped_invalid_address")
            return {"status": "unavailable", "error_code": "ADDRESS_INVALID"}
        if not self.budget.take():
            self.count("budget_deferred")
            return {"status": "deferred", "error_code": None}
        try:
            snapshot = self.market.token_market(chain, token_address)
        except HttpBoundaryError as exc:
            code = str(getattr(exc, "code", "HTTP_BOUNDARY"))
            if getattr(exc, "status", None) == 429:
                self.budget.cooldown(300)
                self.count("provider_cooldown")
            self.record_error("market", code)
            return {"status": "error", "error_code": code[:64]}
        except (RuntimeError, ValueError) as exc:
            code = type(exc).__name__.upper()
            self.record_error("market", code)
            return {"status": "error", "error_code": code[:64]}
        self.count("provider_calls")
        if snapshot.pair_count == 0 or snapshot.price_usd is None:
            # No tradable pair yet, or a pair with no usable price. Absence of a
            # price is not a price of zero.
            return {"status": "unavailable", "error_code": "NO_PRICE"}
        return {
            "status": "ok",
            "error_code": None,
            "price_usd": snapshot.price_usd,
            "market_cap_usd": snapshot.market_cap_usd,
            "liquidity_usd": snapshot.liquidity_usd,
        }

    # -- cycle ------------------------------------------------------------

    def enroll_cycle(self) -> int:
        now_ms = self.clock()
        floor_ms = now_ms - self.enroll_lookback_seconds * 1000
        if self.enroll_floor_ms is not None:
            floor_ms = max(floor_ms, self.enroll_floor_ms)
        if now_ms <= floor_ms:
            return 0
        candidates = select_enrollment_candidates(
            self.connection,
            since_ms=floor_ms,
            until_ms=now_ms,
            cohort_version=self.cohort_version,
            control_sample_rate=self.control_sample_rate,
            control_limit=self.control_per_cycle,
            limit=self.enroll_per_cycle,
        )
        enrolled = 0
        for candidate in candidates:
            result = self.read_market(
                candidate["chain"], candidate["token_address"]
            )
            if result["status"] == "deferred":
                break
            baseline_at_ms = self.clock()
            stored = enroll_cohort_member(
                self.connection,
                candidate=candidate,
                baseline_at_ms=baseline_at_ms,
                enrolled_at_ms=baseline_at_ms,
                baseline_status=result["status"],
                price_usd=result.get("price_usd"),
                market_cap_usd=result.get("market_cap_usd"),
                liquidity_usd=result.get("liquidity_usd"),
                cohort_version=self.cohort_version,
            )
            if stored:
                enrolled += 1
                self.count("enrolled_" + candidate["stratum"])
                self.count("baseline_" + result["status"])
                self.last_enrolled_at_ms = baseline_at_ms
        # Never look further back than the newest decision already considered.
        # Without this the same un-enrollable rows are rescanned every cycle.
        self.enroll_floor_ms = now_ms
        return enrolled

    def measure_cycle(self) -> int:
        now_ms = self.clock()
        due = due_horizons(
            self.connection,
            now_ms=now_ms,
            cohort_version=self.cohort_version,
            limit=self.measure_per_cycle,
        )
        measured = 0
        for item in due:
            if item["expired"]:
                # The window has passed. Reading now would attribute a later
                # price to an earlier horizon, so record the gap instead.
                store_outcome(
                    self.connection,
                    event_id=item["event_id"],
                    ruleset_version=item["ruleset_version"],
                    horizon_minutes=item["horizon_minutes"],
                    scheduled_at_ms=item["scheduled_at_ms"],
                    checked_at_ms=now_ms,
                    source=MARKET_SOURCE,
                    outcome_label="expired",
                    error_code="HORIZON_WINDOW_MISSED",
                    cohort_version=self.cohort_version,
                )
                measured += 1
                self.count("outcome_expired")
                continue
            result = self.read_market(item["chain"], item["token_address"])
            if result["status"] == "deferred":
                break
            checked_at_ms = self.clock()
            store_outcome(
                self.connection,
                event_id=item["event_id"],
                ruleset_version=item["ruleset_version"],
                horizon_minutes=item["horizon_minutes"],
                scheduled_at_ms=item["scheduled_at_ms"],
                checked_at_ms=checked_at_ms,
                source=MARKET_SOURCE,
                outcome_label=result["status"],
                price_usd=result.get("price_usd"),
                market_cap_usd=result.get("market_cap_usd"),
                liquidity_usd=result.get("liquidity_usd"),
                error_code=result.get("error_code"),
                cohort_version=self.cohort_version,
            )
            measured += 1
            self.count("outcome_" + result["status"])
            self.last_measured_at_ms = checked_at_ms
        return measured

    def health_payload(self) -> Dict[str, Any]:
        return {
            "service": SERVICE,
            "cohort_version": self.cohort_version,
            "horizons_minutes": list(HORIZON_MINUTES),
            "updated_at_ms": self.clock(),
            "read_only_upstream": True,
            "no_signing": True,
            "no_broadcast": True,
            "no_telegram": True,
            "cohort": cohort_counts(
                self.connection, cohort_version=self.cohort_version
            ),
            "outcomes": outcome_counts(self.connection),
            "provider_budget": self.budget.snapshot(),
            "counters": dict(sorted(self.counters.items())),
            "last_error": self.last_error,
            "last_enrolled_at_ms": self.last_enrolled_at_ms,
            "last_measured_at_ms": self.last_measured_at_ms,
            "control_sample_rate": self.control_sample_rate,
        }

    def cycle(self) -> Dict[str, int]:
        enrolled = self.enroll_cycle()
        measured = self.measure_cycle()
        write_health(self.health_path, self.health_payload())
        return {"enrolled": enrolled, "measured": measured}

    def require_upstream(self) -> None:
        """Refuse to run against a database the main radar does not own.

        This service reads upstream decisions and must never create them. A
        wrong ``--db-path`` would otherwise leave an empty cohort looking like
        a quiet market instead of a misconfiguration.
        """
        present = {
            str(row["name"])
            for row in self.connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        for table in ("radar_events", "decisions", "provider_observations"):
            if table not in present:
                raise RuntimeError("upstream table missing: %s" % table)

    def run(self, run_seconds: int = 0) -> int:
        self.connection = connect(self.db_path)
        started = time.monotonic()
        try:
            # Inside the try so a refused database still closes its handle.
            self.require_upstream()
            initialize_cohort(self.connection)
            while not self.stop:
                self.cycle()
                if run_seconds and time.monotonic() - started >= run_seconds:
                    break
                waited = 0.0
                while not self.stop and waited < self.poll_seconds:
                    time.sleep(0.25)
                    waited += 0.25
                    if run_seconds and time.monotonic() - started >= run_seconds:
                        break
        finally:
            self.connection.close()
            self.connection = None
        return 0


def arguments(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=SERVICE)
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--health-path", type=Path, required=True)
    parser.add_argument("--run-seconds", type=int, default=0)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = arguments(argv)
        collector = OutcomeCollector(
            db_path=args.db_path,
            health_path=args.health_path,
        )
        for name in (signal.SIGINT, signal.SIGTERM):
            signal.signal(name, lambda *_: setattr(collector, "stop", True))
        return collector.run(args.run_seconds)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "event": "OUTCOME_COLLECTOR_FATAL",
                    "error_type": type(exc).__name__,
                    "no_signing": True,
                    "no_broadcast": True,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
