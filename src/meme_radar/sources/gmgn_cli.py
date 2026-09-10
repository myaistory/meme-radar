from __future__ import annotations

import json
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from ..models import ParseBatch, ParseIssue
from ..normalize import (
    build_event,
    normalize_address,
    normalize_chain,
    parse_timestamp_ms,
    payload_sha256,
)


_HTTP_STATUS = re.compile(r"HTTP\s+(\d{3})")
_CLI_PATH = Path("/usr/local/bin/gmgn-cli")
GMGN_TRENDING_SOURCE = "gmgn_trending_v2"


class GmgnCliError(RuntimeError):
    def __init__(self, code: str, status: Optional[int] = None) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass(frozen=True)
class GmgnSafetySnapshot:
    chain: str
    token_address: str
    observed_at_ms: int
    market_cap_usd: Optional[float]
    total_fee: Optional[float]
    creator_token_status: str
    creator_balance_rate: Optional[float]
    is_honeypot: Optional[bool]
    is_open_source: Optional[bool]
    owner_renounced: Optional[bool]
    is_wash_trading: Optional[bool]
    rug_ratio: Optional[float]
    top_10_holder_rate: Optional[float]
    bundler_rate: Optional[float]
    insider_rate: Optional[float]
    entrapment_ratio: Optional[float]
    bytes_read: int
    elapsed_ms: int
    liquidity_usd: Optional[float] = None
    dev_team_hold_rate: Optional[float] = None
    renounced_mint: Optional[bool] = None
    renounced_freeze: Optional[bool] = None


@dataclass(frozen=True)
class GmgnTokenInfoSnapshot:
    chain: str
    token_address: str
    observed_at_ms: int
    holder_count: Optional[int]
    market_cap_usd: Optional[float]
    price_usd: Optional[float]
    liquidity_usd: Optional[float]
    bytes_read: int
    elapsed_ms: int
    total_fee: Optional[float] = None
    dev_sold_all: Optional[bool] = None


@dataclass(frozen=True)
class GmgnSolanaSecuritySnapshot:
    chain: str
    token_address: str
    observed_at_ms: int
    is_honeypot: Optional[bool]
    is_blacklisted: Optional[bool]
    risk_level: Optional[str]
    top_10_holder_rate: Optional[float]
    renounced_mint: Optional[bool]
    renounced_freeze: Optional[bool]
    dev_sold_all: Optional[bool]
    rug_ratio: Optional[float]
    bundler_rate: Optional[float]
    insider_rate: Optional[float]
    entrapment_ratio: Optional[float]
    dev_team_hold_rate: Optional[float]
    bytes_read: int
    elapsed_ms: int


@dataclass(frozen=True)
class GmgnTrendingSnapshot:
    event_id: str
    chain: str
    token_address: str
    observed_at_ms: int
    holder_count: Optional[int]
    market_cap_usd: Optional[float]
    liquidity_usd: Optional[float]
    volume_5m_usd: Optional[float]
    swaps_5m: Optional[int]
    buys_5m: Optional[int]
    sells_5m: Optional[int]
    smart_degen_count: Optional[int]
    renowned_count: Optional[int]
    creator_close: Optional[bool]
    is_honeypot: Optional[bool]
    is_open_source: Optional[bool]
    owner_renounced: Optional[bool]
    renounced_mint: Optional[bool]
    renounced_freeze: Optional[bool]
    is_wash_trading: Optional[bool]
    rug_ratio: Optional[float]
    top_10_holder_rate: Optional[float]
    bundler_rate: Optional[float]
    insider_rate: Optional[float]
    entrapment_ratio: Optional[float]
    dev_team_hold_rate: Optional[float]
    bytes_read: int
    elapsed_ms: int


@dataclass(frozen=True)
class GmgnTrendingResult:
    payload: Dict[str, Any]
    batch: ParseBatch
    snapshots: Tuple[GmgnTrendingSnapshot, ...]
    observed_at_ms: int


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number < 0 or number != number or number in (float("inf"), float("-inf")):
        return None
    return number


def _flag(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return None


def _risk(value: Any) -> Optional[str]:
    normalized = str(value or "").strip().lower()
    if normalized in {"normal", "safe", "low", "low_risk"}:
        return "normal"
    if normalized in {"high", "danger", "unsafe", "high_risk", "malicious"}:
        return "high"
    return None


def _first_non_null(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None:
            return value
    return None


def _count(value: Any) -> Optional[int]:
    number = _number(value)
    if number is None or not number.is_integer():
        return None
    return int(number)


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


class GmgnTrenchesSource:
    def __init__(
        self,
        *,
        cache_seconds: int = 60,
        timeout_seconds: int = 12,
        max_bytes: int = 2 * 1024 * 1024,
        cli_path: Path = _CLI_PATH,
        runner: Callable = subprocess.run,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if cli_path != _CLI_PATH:
            raise ValueError("GMGN CLI path is not pinned")
        if not 30 <= cache_seconds <= 600:
            raise ValueError("GMGN cache outside 30..600 seconds")
        if not 1 <= timeout_seconds <= 30 or max_bytes <= 0:
            raise ValueError("invalid GMGN process boundary")
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._max_bytes = max_bytes
        self._cli_path = cli_path
        self._runner = runner
        self._monotonic = monotonic
        self._cache: Dict[str, Tuple[float, Dict[str, GmgnSafetySnapshot]]] = {}

    def has_fresh_cache(self, chain: str) -> bool:
        normalized = normalize_chain(chain)
        cached = self._cache.get(normalized)
        return bool(cached and self._monotonic() - cached[0] < self._cache_seconds)

    @staticmethod
    def _command(chain: str) -> list:
        cli_chain = "sol" if chain == "solana" else chain
        command = [
            str(_CLI_PATH),
            "market",
            "trenches",
            "--chain",
            cli_chain,
            "--limit",
            "80",
            "--raw",
        ]
        if chain == "bsc":
            types = ("completed",)
        elif chain == "solana":
            types = ("new_creation", "near_completion", "completed")
        else:
            types = ("new_creation", "completed")
        for category in types:
            command.extend(("--type", category))
        if chain == "solana":
            command.extend(
                (
                    "--max-created",
                    "120m",
                    "--min-marketcap",
                    "10000",
                    "--max-marketcap",
                    "100000",
                    "--min-liquidity",
                    "5000",
                    "--min-total-fee",
                    "1",
                    "--max-total-fee",
                    "10",
                )
            )
        if chain != "solana":
            command.extend(
                (
                    "--launchpad-platform",
                    "fourmeme" if chain == "bsc" else "clanker",
                )
            )
        return command

    @staticmethod
    def _environment() -> Dict[str, str]:
        return {
            "HOME": "/var/lib/meme-radar",
            "LANG": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "GMGN_RATE_LIMIT_AUTO_RETRY_MAX_WAIT_MS": "0",
        }

    def _run(self, chain: str) -> Tuple[bytes, int]:
        started = time.monotonic()
        try:
            result = self._runner(
                self._command(chain),
                capture_output=True,
                check=False,
                cwd="/",
                env=self._environment(),
                timeout=self._timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise GmgnCliError("TIMEOUT") from exc
        stdout = result.stdout if isinstance(result.stdout, bytes) else result.stdout.encode()
        stderr = result.stderr if isinstance(result.stderr, bytes) else result.stderr.encode()
        if len(stdout) > self._max_bytes or len(stderr) > 64 * 1024:
            raise GmgnCliError("OUTPUT_TOO_LARGE")
        if result.returncode != 0:
            text = stderr.decode("utf-8", errors="replace")
            match = _HTTP_STATUS.search(text)
            status = int(match.group(1)) if match else None
            if status in {401, 403}:
                raise GmgnCliError("AUTH_ERROR", status)
            if status == 429:
                raise GmgnCliError("RATE_LIMIT", status)
            raise GmgnCliError("CLI_ERROR", status)
        return stdout, int((time.monotonic() - started) * 1000)

    @staticmethod
    def _snapshot(chain: str, row: Dict[str, Any], size: int, elapsed: int):
        response_chain = row.get("chain")
        if response_chain is not None and normalize_chain(response_chain) != chain:
            raise ValueError("GMGN trenches chain mismatch")
        address = normalize_address(chain, row.get("address"))
        return GmgnSafetySnapshot(
            chain=chain,
            token_address=address,
            observed_at_ms=int(time.time() * 1000),
            market_cap_usd=_number(row.get("market_cap")),
            total_fee=_number(row.get("total_fee")),
            creator_token_status=str(row.get("creator_token_status", ""))[:32],
            creator_balance_rate=_number(row.get("creator_balance_rate")),
            is_honeypot=_flag(row.get("is_honeypot")),
            is_open_source=_flag(row.get("open_source")),
            owner_renounced=_flag(row.get("owner_renounced")),
            is_wash_trading=_flag(row.get("is_wash_trading")),
            rug_ratio=_number(row.get("rug_ratio")),
            top_10_holder_rate=_number(row.get("top_10_holder_rate")),
            bundler_rate=_number(
                _first_non_null(row, "bundler_rate", "bundler_trader_amount_rate")
            ),
            insider_rate=_number(
                _first_non_null(row, "insider_rate", "rat_trader_amount_rate")
            ),
            entrapment_ratio=_number(row.get("entrapment_ratio")),
            bytes_read=size,
            elapsed_ms=elapsed,
            liquidity_usd=_number(row.get("liquidity")),
            dev_team_hold_rate=_number(row.get("dev_team_hold_rate")),
            renounced_mint=_flag(row.get("renounced_mint")),
            renounced_freeze=_flag(row.get("renounced_freeze_account")),
        )

    def _refresh(self, chain: str) -> None:
        raw, elapsed = self._run(chain)
        try:
            payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise GmgnCliError("INVALID_JSON") from exc
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            payload = payload["data"]
        if not isinstance(payload, dict):
            raise GmgnCliError("INVALID_RESPONSE")
        if chain == "bsc":
            categories = ("completed",)
        elif chain == "solana":
            categories = ("new_creation", "near_completion", "completed")
        else:
            categories = ("new_creation", "completed")
        rows = []
        for category in categories:
            items = payload.get(category, [])
            if not isinstance(items, list) or len(items) > 80:
                raise GmgnCliError("INVALID_RESPONSE")
            rows.extend(item for item in items if isinstance(item, dict))
        snapshots = {}
        for row in rows:
            try:
                snapshot = self._snapshot(chain, row, len(raw), elapsed)
            except (TypeError, ValueError):
                continue
            current = snapshots.get(snapshot.token_address)
            current_cap = current.market_cap_usd if current else None
            if current is None or (snapshot.market_cap_usd or -1) > (current_cap or -1):
                snapshots[snapshot.token_address] = snapshot
        self._cache[chain] = (self._monotonic(), snapshots)

    def token_safety(self, chain: str, token_address: str) -> Optional[GmgnSafetySnapshot]:
        normalized_chain = normalize_chain(chain)
        if normalized_chain not in {"bsc", "base", "solana"}:
            raise ValueError("GMGN unified gate chain unsupported")
        normalized_address = normalize_address(normalized_chain, token_address)
        if not self.has_fresh_cache(normalized_chain):
            self._refresh(normalized_chain)
        return self._cache[normalized_chain][1].get(normalized_address)


class GmgnTrendingSource:
    def __init__(
        self,
        *,
        timeout_seconds: int = 15,
        max_bytes: int = 2 * 1024 * 1024,
        cli_path: Path = _CLI_PATH,
        runner: Callable = subprocess.run,
        wall_time: Callable[[], float] = time.time,
    ) -> None:
        if cli_path != _CLI_PATH:
            raise ValueError("GMGN CLI path is not pinned")
        if not 1 <= timeout_seconds <= 30 or max_bytes <= 0:
            raise ValueError("invalid GMGN process boundary")
        self._timeout_seconds = timeout_seconds
        self._max_bytes = max_bytes
        self._runner = runner
        self._wall_time = wall_time
        self._config_checked = False

    @staticmethod
    def _cli_chain(chain: str) -> str:
        return "sol" if chain == "solana" else chain

    @classmethod
    def _command(cls, chain: str) -> list:
        normalized = normalize_chain(chain)
        if normalized not in {"bsc", "base", "solana"}:
            raise ValueError("GMGN discovery chain unsupported")
        command = [
            str(_CLI_PATH),
            "market",
            "trending",
            "--chain",
            cls._cli_chain(normalized),
            "--interval",
            "5m",
            "--order-by",
            "volume",
            "--direction",
            "desc",
            "--min-marketcap",
            "100000.01",
            "--min-holder-count",
            "101",
            "--min-liquidity",
            "10000",
            "--min-swaps",
            "10",
            "--max-created",
            "2h",
            "--limit",
            "50",
            "--raw",
        ]
        filters = (
            ("renounced", "frozen", "not_wash_trading")
            if normalized == "solana"
            else ("not_honeypot", "verified", "renounced")
        )
        for filter_name in filters:
            command.extend(("--filter", filter_name))
        return command

    def _run(self, chain: str) -> Tuple[bytes, int]:
        if not self._config_checked:
            try:
                checked = self._runner(
                    [str(_CLI_PATH), "config", "--check"],
                    capture_output=True,
                    check=False,
                    cwd="/",
                    env=GmgnTrenchesSource._environment(),
                    timeout=self._timeout_seconds,
                )
            except subprocess.TimeoutExpired as exc:
                raise GmgnCliError("CONFIG_CHECK_TIMEOUT") from exc
            if checked.returncode != 0:
                raise GmgnCliError("CONFIG_CHECK_FAILED")
            self._config_checked = True
        started = time.monotonic()
        try:
            result = self._runner(
                self._command(chain),
                capture_output=True,
                check=False,
                cwd="/",
                env=GmgnTrenchesSource._environment(),
                timeout=self._timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise GmgnCliError("TIMEOUT") from exc
        stdout = (
            result.stdout
            if isinstance(result.stdout, bytes)
            else result.stdout.encode()
        )
        stderr = (
            result.stderr
            if isinstance(result.stderr, bytes)
            else result.stderr.encode()
        )
        if len(stdout) > self._max_bytes or len(stderr) > 64 * 1024:
            raise GmgnCliError("OUTPUT_TOO_LARGE")
        if result.returncode != 0:
            text = stderr.decode("utf-8", errors="replace")
            match = _HTTP_STATUS.search(text)
            status = int(match.group(1)) if match else None
            if status in {401, 403}:
                raise GmgnCliError("AUTH_ERROR", status)
            if status == 429:
                raise GmgnCliError("RATE_LIMIT", status)
            raise GmgnCliError("CLI_ERROR", status)
        return stdout, int((time.monotonic() - started) * 1000)

    @staticmethod
    def _creator_close(row: Dict[str, Any]) -> Optional[bool]:
        direct = _flag(row.get("creator_close"))
        if direct is not None:
            return direct
        status = str(row.get("creator_token_status", "")).strip().lower()
        return True if status == "creator_close" else False if status else None

    def fetch(self, chain: str) -> GmgnTrendingResult:
        normalized = normalize_chain(chain)
        raw, elapsed = self._run(normalized)
        try:
            payload = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_no_duplicate_keys,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise GmgnCliError("INVALID_JSON") from exc
        if not isinstance(payload, dict) or payload.get("code") != 0:
            raise GmgnCliError("INVALID_RESPONSE")
        data = payload.get("data")
        rows = data.get("rank") if isinstance(data, dict) else None
        if not isinstance(rows, list) or len(rows) > 50:
            raise GmgnCliError("INVALID_RESPONSE")
        observed_at_ms = int(self._wall_time() * 1000)
        digest = payload_sha256(payload)
        events = []
        issues = []
        snapshots = []
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                issues.append(ParseIssue(index, "INVALID_ROW", "not an object"))
                continue
            try:
                row_chain = normalize_chain(row.get("chain"))
                if row_chain != normalized:
                    raise ValueError("chain mismatch")
                address = normalize_address(normalized, row.get("address"))
                creator = normalize_address(normalized, row.get("creator"))
                if normalized != "solana" and int(creator, 16) == 0:
                    raise ValueError("zero creator")
                created_at_ms = parse_timestamp_ms(
                    row.get("creation_timestamp") or row.get("open_timestamp")
                )
                holder_count = _count(row.get("holder_count"))
                market_cap_usd = _number(row.get("market_cap"))
                liquidity_usd = _number(row.get("liquidity"))
                swaps_5m = _count(row.get("swaps"))
                age_ms = observed_at_ms - created_at_ms
                if (
                    holder_count is None
                    or holder_count <= 100
                    or market_cap_usd is None
                    or market_cap_usd <= 100_000
                    or liquidity_usd is None
                    or liquidity_usd < 10_000
                    or swaps_5m is None
                    or swaps_5m < 10
                    or age_ms < -120_000
                    or age_ms > 2 * 60 * 60 * 1000
                ):
                    raise ValueError("server filter mismatch")
                event = build_event(
                    source=GMGN_TRENDING_SOURCE,
                    source_event_id="%s:%s" % (normalized, address),
                    event_kind="launch",
                    chain=normalized,
                    token_address=address,
                    source_published_at_ms=created_at_ms,
                    observed_at_ms=observed_at_ms,
                    received_at_ms=observed_at_ms,
                    launchpad=row.get("launchpad_platform") or "gmgn_trending",
                    token_created_at_ms=created_at_ms,
                    name=row.get("name"),
                    symbol=row.get("symbol"),
                    creator=creator,
                    raw_sha256=digest,
                )
                snapshot = GmgnTrendingSnapshot(
                    event_id=event.event_id,
                    chain=normalized,
                    token_address=address,
                    observed_at_ms=observed_at_ms,
                    holder_count=holder_count,
                    market_cap_usd=market_cap_usd,
                    liquidity_usd=liquidity_usd,
                    volume_5m_usd=_number(row.get("volume")),
                    swaps_5m=swaps_5m,
                    buys_5m=_count(row.get("buys")),
                    sells_5m=_count(row.get("sells")),
                    smart_degen_count=_count(row.get("smart_degen_count")),
                    renowned_count=_count(row.get("renowned_count")),
                    creator_close=self._creator_close(row),
                    is_honeypot=_flag(row.get("is_honeypot")),
                    is_open_source=_flag(row.get("is_open_source")),
                    owner_renounced=_flag(row.get("is_renounced")),
                    renounced_mint=_flag(row.get("renounced_mint")),
                    renounced_freeze=_flag(
                        row.get("renounced_freeze_account")
                    ),
                    is_wash_trading=_flag(row.get("is_wash_trading")),
                    rug_ratio=_number(row.get("rug_ratio")),
                    top_10_holder_rate=_number(
                        row.get("top_10_holder_rate")
                    ),
                    bundler_rate=_number(row.get("bundler_rate")),
                    insider_rate=_number(row.get("rat_trader_amount_rate")),
                    entrapment_ratio=_number(row.get("entrapment_ratio")),
                    dev_team_hold_rate=_number(
                        row.get("dev_team_hold_rate")
                    ),
                    bytes_read=len(raw),
                    elapsed_ms=elapsed,
                )
            except (TypeError, ValueError) as exc:
                issues.append(
                    ParseIssue(index, "INVALID_ROW", type(exc).__name__)
                )
                continue
            events.append(event)
            snapshots.append(snapshot)
        return GmgnTrendingResult(
            payload=payload,
            batch=ParseBatch(tuple(events), tuple(issues), digest),
            snapshots=tuple(snapshots),
            observed_at_ms=observed_at_ms,
        )


class GmgnTokenInfoSource:
    def __init__(
        self,
        *,
        cache_seconds: int = 60,
        timeout_seconds: int = 12,
        max_bytes: int = 512 * 1024,
        cli_path: Path = _CLI_PATH,
        runner: Callable = subprocess.run,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if cli_path != _CLI_PATH:
            raise ValueError("GMGN CLI path is not pinned")
        if not 30 <= cache_seconds <= 600:
            raise ValueError("GMGN cache outside 30..600 seconds")
        if not 1 <= timeout_seconds <= 30 or max_bytes <= 0:
            raise ValueError("invalid GMGN process boundary")
        self._cache_seconds = cache_seconds
        self._timeout_seconds = timeout_seconds
        self._max_bytes = max_bytes
        self._runner = runner
        self._monotonic = monotonic
        self._cache: Dict[
            Tuple[str, str], Tuple[float, Optional[GmgnTokenInfoSnapshot]]
        ] = {}

    def has_fresh_cache(self, chain: str, token_address: str) -> bool:
        key = (
            normalize_chain(chain),
            normalize_address(normalize_chain(chain), token_address),
        )
        cached = self._cache.get(key)
        return bool(cached and self._monotonic() - cached[0] < self._cache_seconds)

    @staticmethod
    def _command(chain: str, token_address: str) -> list:
        return [
            str(_CLI_PATH),
            "token",
            "info",
            "--chain",
            chain,
            "--address",
            token_address,
            "--raw",
        ]

    def _run(self, chain: str, token_address: str) -> Tuple[bytes, int]:
        started = time.monotonic()
        try:
            result = self._runner(
                self._command(chain, token_address),
                capture_output=True,
                check=False,
                cwd="/",
                env=GmgnTrenchesSource._environment(),
                timeout=self._timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise GmgnCliError("TIMEOUT") from exc
        stdout = result.stdout if isinstance(result.stdout, bytes) else result.stdout.encode()
        stderr = result.stderr if isinstance(result.stderr, bytes) else result.stderr.encode()
        if len(stdout) > self._max_bytes or len(stderr) > 64 * 1024:
            raise GmgnCliError("OUTPUT_TOO_LARGE")
        if result.returncode != 0:
            text = stderr.decode("utf-8", errors="replace")
            match = _HTTP_STATUS.search(text)
            status = int(match.group(1)) if match else None
            if status in {401, 403}:
                raise GmgnCliError("AUTH_ERROR", status)
            if status == 429:
                raise GmgnCliError("RATE_LIMIT", status)
            raise GmgnCliError("CLI_ERROR", status)
        return stdout, int((time.monotonic() - started) * 1000)

    @staticmethod
    def _parse(
        chain: str,
        token_address: str,
        raw: bytes,
        elapsed_ms: int,
    ) -> Optional[GmgnTokenInfoSnapshot]:
        try:
            payload = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_no_duplicate_keys,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise GmgnCliError("INVALID_JSON") from exc
        if not isinstance(payload, dict):
            raise GmgnCliError("INVALID_RESPONSE")
        data = payload.get("data")
        if isinstance(data, dict):
            nested = data.get("token")
            payload = nested if isinstance(nested, dict) else data
        response_address = payload.get("address") or payload.get("token_address")
        if response_address is not None:
            try:
                if normalize_address(chain, response_address) != token_address:
                    raise GmgnCliError("IDENTITY_MISMATCH")
            except ValueError as exc:
                raise GmgnCliError("IDENTITY_MISMATCH") from exc
        response_chain = str(payload.get("chain") or "").strip().lower()
        if response_chain and response_chain not in {chain, "sol" if chain == "solana" else chain}:
            raise GmgnCliError("IDENTITY_MISMATCH")
        if not str(payload.get("symbol", "")).strip():
            return None
        price_block = payload.get("price")
        price_block = price_block if isinstance(price_block, dict) else {}
        price = _number(price_block.get("price"))
        supply = _number(
            payload.get("circulating_supply") or payload.get("total_supply")
        )
        direct_market_cap = _number(payload.get("market_cap"))
        market_cap = direct_market_cap
        if market_cap is None and price is not None and supply is not None:
            market_cap = price * supply
        return GmgnTokenInfoSnapshot(
            chain=chain,
            token_address=token_address,
            observed_at_ms=int(time.time() * 1000),
            holder_count=_count(payload.get("holder_count")),
            market_cap_usd=market_cap,
            price_usd=price,
            liquidity_usd=_number(payload.get("liquidity")),
            bytes_read=len(raw),
            elapsed_ms=elapsed_ms,
            total_fee=_number(payload.get("total_fee")),
            dev_sold_all=_flag(payload.get("dev_sold_all")),
        )

    def token_info(
        self,
        chain: str,
        token_address: str,
    ) -> Optional[GmgnTokenInfoSnapshot]:
        normalized_chain = normalize_chain(chain)
        if normalized_chain not in {"bsc", "base", "robinhood", "solana"}:
            raise ValueError("GMGN size gate chain unsupported")
        normalized_address = normalize_address(normalized_chain, token_address)
        key = (normalized_chain, normalized_address)
        if self.has_fresh_cache(*key):
            return self._cache[key][1]
        cli_chain = "sol" if normalized_chain == "solana" else normalized_chain
        raw, elapsed = self._run(cli_chain, normalized_address)
        snapshot = self._parse(normalized_chain, normalized_address, raw, elapsed)
        self._cache[key] = (self._monotonic(), snapshot)
        return snapshot


class GmgnSolanaSecuritySource:
    def __init__(
        self,
        *,
        timeout_seconds: int = 12,
        max_bytes: int = 512 * 1024,
        cli_path: Path = _CLI_PATH,
        runner: Callable = subprocess.run,
    ) -> None:
        if cli_path != _CLI_PATH:
            raise ValueError("GMGN CLI path is not pinned")
        if not 1 <= timeout_seconds <= 30 or max_bytes <= 0:
            raise ValueError("invalid GMGN security boundary")
        self._timeout_seconds = timeout_seconds
        self._max_bytes = max_bytes
        self._cli_path = cli_path
        self._runner = runner

    def token_security(self, token_address: str) -> GmgnSolanaSecuritySnapshot:
        address = normalize_address("solana", token_address)
        command = [
            str(self._cli_path),
            "token",
            "security",
            "--chain",
            "sol",
            "--address",
            address,
            "--raw",
        ]
        started = time.monotonic()
        try:
            result = self._runner(
                command,
                capture_output=True,
                check=False,
                cwd="/",
                env=GmgnTrenchesSource._environment(),
                timeout=self._timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise GmgnCliError("TIMEOUT") from exc
        stdout = result.stdout if isinstance(result.stdout, bytes) else result.stdout.encode()
        stderr = result.stderr if isinstance(result.stderr, bytes) else result.stderr.encode()
        if len(stdout) > self._max_bytes or len(stderr) > 64 * 1024:
            raise GmgnCliError("OUTPUT_TOO_LARGE")
        if result.returncode != 0:
            text = stderr.decode("utf-8", errors="replace")
            match = _HTTP_STATUS.search(text)
            status = int(match.group(1)) if match else None
            if status in {401, 403}:
                raise GmgnCliError("AUTH_ERROR", status)
            if status == 429:
                raise GmgnCliError("RATE_LIMIT", status)
            raise GmgnCliError("CLI_ERROR", status)
        try:
            payload = json.loads(stdout.decode("utf-8"), object_pairs_hook=_no_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise GmgnCliError("INVALID_JSON") from exc
        if not isinstance(payload, dict):
            raise GmgnCliError("INVALID_RESPONSE")
        data = payload.get("data")
        if isinstance(data, dict):
            nested = data.get("token")
            payload = nested if isinstance(nested, dict) else data
        if not payload:
            raise GmgnCliError("INVALID_RESPONSE")
        response_address = payload.get("address") or payload.get("token_address")
        if response_address is not None:
            try:
                if normalize_address("solana", response_address) != address:
                    raise GmgnCliError("IDENTITY_MISMATCH")
            except ValueError as exc:
                raise GmgnCliError("IDENTITY_MISMATCH") from exc
        response_chain = str(payload.get("chain") or "").strip().lower()
        if response_chain and response_chain not in {"sol", "solana"}:
            raise GmgnCliError("IDENTITY_MISMATCH")
        honeypot = _flag(_first_non_null(payload, "is_honeypot", "honeypot"))
        blacklisted = _flag(
            _first_non_null(
                payload, "is_blacklisted", "is_blacklist", "blacklist"
            )
        )
        risk_raw = _first_non_null(payload, "risk_level", "risk")
        risk_present = risk_raw not in (None, "")
        explicit_risk = _risk(risk_raw)
        risk_level = "unrecognized" if risk_present and explicit_risk is None else explicit_risk
        if not risk_present and risk_level is None and honeypot is not None and blacklisted is not None:
            risk_level = "high" if honeypot or blacklisted else "normal"
        if honeypot is True or blacklisted is True:
            risk_level = "high"
        return GmgnSolanaSecuritySnapshot(
            chain="solana",
            token_address=address,
            observed_at_ms=int(time.time() * 1000),
            is_honeypot=honeypot,
            is_blacklisted=blacklisted,
            risk_level=risk_level,
            top_10_holder_rate=_number(
                payload.get("top10_holder_rate", payload.get("top_10_holder_rate"))
            ),
            renounced_mint=_flag(
                payload.get("renounced_mint", payload.get("mint_renounced"))
            ),
            renounced_freeze=_flag(
                payload.get(
                    "renounced_freeze_account",
                    payload.get("renounced_freeze", payload.get("freeze_renounced")),
                )
            ),
            dev_sold_all=_flag(payload.get("dev_sold_all")),
            rug_ratio=_number(payload.get("rug_ratio")),
            bundler_rate=_number(
                payload.get("bundler_rate", payload.get("bundler_trader_amount_rate"))
            ),
            insider_rate=_number(
                payload.get("insider_rate", payload.get("rat_trader_amount_rate"))
            ),
            entrapment_ratio=_number(payload.get("entrapment_ratio")),
            dev_team_hold_rate=_number(payload.get("dev_team_hold_rate")),
            bytes_read=len(stdout),
            elapsed_ms=int((time.monotonic() - started) * 1000),
        )
