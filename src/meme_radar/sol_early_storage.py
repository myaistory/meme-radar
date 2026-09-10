from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


SCHEMA_VERSION = 2
TOKEN_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
SCHEMA = """
CREATE TABLE IF NOT EXISTS sol_early_signals (
    signal_id TEXT PRIMARY KEY,
    token_address TEXT NOT NULL,
    seen_at_ms INTEGER NOT NULL,
    card_type TEXT NOT NULL,
    market_cap_usd REAL,
    liquidity_usd REAL,
    top10_percent REAL,
    total_fee_sol REAL
);
CREATE INDEX IF NOT EXISTS idx_sol_early_signal_window
  ON sol_early_signals(token_address,seen_at_ms);
CREATE TABLE IF NOT EXISTS sol_early_candidates (
    token_address TEXT PRIMARY KEY,
    detected_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','processing','queued','blocked','expired')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 10),
    next_attempt_at_ms INTEGER NOT NULL,
    last_result TEXT
);
CREATE INDEX IF NOT EXISTS idx_sol_early_candidate_due
  ON sol_early_candidates(state,next_attempt_at_ms);
CREATE TABLE IF NOT EXISTS sol_early_outbox (
    token_address TEXT PRIMARY KEY,
    claimed_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','sending','sent','unknown')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 10),
    next_attempt_at_ms INTEGER NOT NULL,
    valid_until_ms INTEGER NOT NULL,
    last_error TEXT,
    message_text TEXT NOT NULL,
    sent_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_sol_early_outbox_due
  ON sol_early_outbox(state,next_attempt_at_ms);
CREATE TABLE IF NOT EXISTS sol_early_state (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
"""


def connect(path: Path) -> sqlite3.Connection:
    existed = path.exists()
    connection = sqlite3.connect(str(path), timeout=10)
    if not existed:
        path.chmod(0o600)
    connection.row_factory = sqlite3.Row
    if str(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower() != "wal":
        connection.close()
        raise RuntimeError("SQLite WAL unavailable")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    current = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if current not in {0, 1, SCHEMA_VERSION}:
        raise RuntimeError("unsupported Sol early schema")
    with connection:
        connection.executescript(SCHEMA)
        if current == 1:
            invalid = connection.execute(
                """
                SELECT 1 FROM sol_early_signals
                WHERE top10_percent IS NOT NULL
                  AND (top10_percent < 0 OR top10_percent > 100)
                LIMIT 1
                """
            ).fetchone()
            if invalid is not None:
                raise RuntimeError("invalid legacy Sol early Top10 percentage")
            connection.execute(
                """
                UPDATE sol_early_signals
                SET top10_percent=top10_percent/100.0
                WHERE top10_percent IS NOT NULL
                """
            )
        connection.execute("PRAGMA user_version=%d" % SCHEMA_VERSION)
        connection.execute(
            """
            UPDATE sol_early_candidates
            SET state=CASE WHEN attempts>=10 THEN 'blocked' ELSE 'pending' END,
                last_result=CASE WHEN attempts>=10 THEN 'ATTEMPT_LIMIT' ELSE 'PROCESS_INTERRUPTED' END
            WHERE state='processing'
            """
        )
        connection.execute(
            """
            UPDATE sol_early_outbox SET state='unknown',last_error='SEND_INTERRUPTED'
            WHERE state='sending'
            """
        )


def get_state(connection: sqlite3.Connection, key: str, default=None):
    row = connection.execute(
        "SELECT value_json FROM sol_early_state WHERE key=?", (key,)
    ).fetchone()
    return default if row is None else json.loads(row["value_json"])


def set_state(
    connection: sqlite3.Connection, key: str, value: Any, now_ms: int
) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"))
    with connection:
        connection.execute(
            """
            INSERT INTO sol_early_state(key,value_json,updated_at_ms)
            VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET
              value_json=excluded.value_json,updated_at_ms=excluded.updated_at_ms
            """,
            (key, encoded, now_ms),
        )


def reserve_provider_calls(
    connection: sqlite3.Connection,
    *,
    now_ms: int,
    cost: int,
    limit_per_hour: int,
) -> bool:
    if cost < 1 or limit_per_hour < cost:
        raise ValueError("invalid Sol early provider budget")
    key = "provider_budget_v1"
    with connection:
        row = connection.execute(
            "SELECT value_json FROM sol_early_state WHERE key=?", (key,)
        ).fetchone()
        value = json.loads(row["value_json"]) if row is not None else {}
        window = int(value.get("window_started_at_ms", now_ms))
        used = int(value.get("used", 0))
        cooldown = int(value.get("cooldown_until_ms", 0))
        if now_ms < cooldown:
            return False
        if now_ms >= window + 3_600_000:
            window, used = now_ms, 0
        if used + cost > limit_per_hour:
            return False
        encoded = json.dumps(
            {
                "window_started_at_ms": window,
                "used": used + cost,
                "limit": limit_per_hour,
                "cooldown_until_ms": cooldown,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        connection.execute(
            """
            INSERT INTO sol_early_state(key,value_json,updated_at_ms)
            VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET
              value_json=excluded.value_json,updated_at_ms=excluded.updated_at_ms
            """,
            (key, encoded, now_ms),
        )
    return True


def set_provider_cooldown(
    connection: sqlite3.Connection, *, now_ms: int, seconds: int
) -> None:
    if seconds < 1:
        raise ValueError("invalid Sol early provider cooldown")
    key = "provider_budget_v1"
    value = get_state(connection, key, {})
    value["cooldown_until_ms"] = max(
        int(value.get("cooldown_until_ms", 0)), now_ms + seconds * 1000
    )
    set_state(connection, key, value, now_ms)


def provider_budget_state(connection: sqlite3.Connection) -> Dict[str, Any]:
    return dict(get_state(connection, "provider_budget_v1", {}))


def provider_next_available_ms(
    connection: sqlite3.Connection,
    *,
    now_ms: int,
    cost: int,
    limit_per_hour: int,
) -> int:
    if cost < 1 or limit_per_hour < cost:
        raise ValueError("invalid Sol early provider budget")
    value = provider_budget_state(connection)
    cooldown = int(value.get("cooldown_until_ms", 0))
    window = int(value.get("window_started_at_ms", now_ms))
    used = int(value.get("used", 0))
    if now_ms >= window + 3_600_000:
        return max(now_ms + 1_000, cooldown)
    if used + cost <= limit_per_hour:
        return max(now_ms + 1_000, cooldown)
    return max(now_ms + 1_000, cooldown, window + 3_600_000)


def store_signal(
    connection: sqlite3.Connection,
    *,
    signal_id: str,
    token_address: str,
    seen_at_ms: int,
    card_type: str,
    market_cap_usd: Optional[float],
    liquidity_usd: Optional[float],
    top10_percent: Optional[float],
    total_fee_sol: Optional[float],
) -> bool:
    invalid_top10 = (
        top10_percent is not None
        and (
            isinstance(top10_percent, bool)
            or not isinstance(top10_percent, (int, float))
            or top10_percent != top10_percent
            or not 0 <= top10_percent <= 1
        )
    )
    if not signal_id or not TOKEN_RE.fullmatch(token_address) or invalid_top10:
        raise ValueError("invalid Sol early signal identity")
    with connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO sol_early_signals
              (signal_id,token_address,seen_at_ms,card_type,market_cap_usd,
               liquidity_usd,top10_percent,total_fee_sol)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                signal_id,
                token_address,
                seen_at_ms,
                card_type,
                market_cap_usd,
                liquidity_usd,
                top10_percent,
                total_fee_sol,
            ),
        )
    return cursor.rowcount == 1


def recent_signals(
    connection: sqlite3.Connection, token_address: str, since_ms: int
) -> list[Dict[str, Any]]:
    return [
        dict(row)
        for row in connection.execute(
            """
            SELECT signal_id,seen_at_ms,card_type,market_cap_usd,liquidity_usd,
                   top10_percent,total_fee_sol
            FROM sol_early_signals
            WHERE token_address=? AND seen_at_ms>=?
            ORDER BY seen_at_ms,signal_id
            """,
            (token_address, since_ms),
        )
    ]


def schedule_candidate(
    connection: sqlite3.Connection,
    *,
    token_address: str,
    detected_at_ms: int,
    expires_at_ms: int,
) -> str:
    if not TOKEN_RE.fullmatch(token_address) or expires_at_ms <= detected_at_ms:
        raise ValueError("invalid Sol early candidate")
    with connection:
        delivered = connection.execute(
            "SELECT 1 FROM sol_early_outbox WHERE token_address=?", (token_address,)
        ).fetchone()
        if delivered is not None:
            return "candidate_deduplicated"
        cursor = connection.execute(
            """
            INSERT INTO sol_early_candidates
              (token_address,detected_at_ms,expires_at_ms,state,attempts,next_attempt_at_ms)
            VALUES (?,?,?,'pending',0,?)
            ON CONFLICT(token_address) DO UPDATE SET
              detected_at_ms=excluded.detected_at_ms,
              expires_at_ms=excluded.expires_at_ms,
              state='pending',attempts=0,
              next_attempt_at_ms=excluded.next_attempt_at_ms,
              last_result='NEW_EVIDENCE'
            WHERE sol_early_candidates.state IN ('blocked','expired')
            """,
            (token_address, detected_at_ms, expires_at_ms, detected_at_ms),
        )
    return "scheduled" if cursor.rowcount == 1 else "candidate_deduplicated"


def claim_candidate(
    connection: sqlite3.Connection, *, now_ms: int
) -> Optional[Dict[str, Any]]:
    with connection:
        row = connection.execute(
            """
            SELECT token_address,detected_at_ms,expires_at_ms,attempts
            FROM sol_early_candidates
            WHERE state='pending' AND next_attempt_at_ms<=? AND attempts<10
            ORDER BY next_attempt_at_ms,detected_at_ms LIMIT 1
            """,
            (now_ms,),
        ).fetchone()
        if row is None:
            return None
        state = "expired" if now_ms > int(row["expires_at_ms"]) else "processing"
        connection.execute(
            """
            UPDATE sol_early_candidates SET state=?,attempts=attempts+1
            WHERE token_address=? AND state='pending'
            """,
            (state, row["token_address"]),
        )
    result = dict(row)
    result["state"] = state
    result["attempts"] = int(result["attempts"]) + 1
    return result


def finish_candidate(
    connection: sqlite3.Connection,
    token_address: str,
    *,
    state: str,
    result: str,
    next_attempt_at_ms: Optional[int] = None,
) -> None:
    if state not in {"pending", "queued", "blocked", "expired"}:
        raise ValueError("invalid Sol early candidate result")
    with connection:
        cursor = connection.execute(
            """
            UPDATE sol_early_candidates
            SET state=?,last_result=?,next_attempt_at_ms=COALESCE(?,next_attempt_at_ms)
            WHERE token_address=? AND state IN ('processing','expired')
            """,
            (state, str(result)[:128], next_attempt_at_ms, token_address),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Sol early candidate is not claimable")


def defer_candidate_without_attempt(
    connection: sqlite3.Connection,
    token_address: str,
    *,
    result: str,
    next_attempt_at_ms: int,
) -> None:
    with connection:
        cursor = connection.execute(
            """
            UPDATE sol_early_candidates
            SET state='pending',attempts=CASE WHEN attempts>0 THEN attempts-1 ELSE 0 END,
                last_result=?,next_attempt_at_ms=?
            WHERE token_address=? AND state='processing'
            """,
            (str(result)[:128], next_attempt_at_ms, token_address),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Sol early candidate is not processing")


def claim_notification(
    connection: sqlite3.Connection,
    *,
    token_address: str,
    message_text: str,
    now_ms: int,
    valid_until_ms: int,
) -> str:
    if (
        not TOKEN_RE.fullmatch(token_address)
        or not 0 < len(message_text) <= 4096
        or valid_until_ms < now_ms
    ):
        raise ValueError("invalid Sol early notification")
    with connection:
        existing = connection.execute(
            "SELECT 1 FROM sol_early_outbox WHERE token_address=?", (token_address,)
        ).fetchone()
        if existing is not None:
            return "deduplicated"
        connection.execute(
            """
            INSERT INTO sol_early_outbox
              (token_address,claimed_at_ms,state,attempts,next_attempt_at_ms,
               valid_until_ms,message_text)
            VALUES (?,?,'pending',0,?,?,?)
            """,
            (token_address, now_ms, now_ms, valid_until_ms, message_text),
        )
    return "enqueued"


def claim_due(
    connection: sqlite3.Connection, *, now_ms: int, max_attempts: int = 5
) -> Optional[Dict[str, Any]]:
    with connection:
        row = connection.execute(
            """
            SELECT token_address,attempts,message_text,valid_until_ms
            FROM sol_early_outbox
            WHERE state='pending' AND next_attempt_at_ms<=? AND attempts<?
            ORDER BY next_attempt_at_ms,claimed_at_ms LIMIT 1
            """,
            (now_ms, max_attempts),
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            """
            UPDATE sol_early_outbox SET state='sending',attempts=attempts+1,next_attempt_at_ms=?
            WHERE token_address=? AND state='pending'
            """,
            (now_ms + 60_000, row["token_address"]),
        )
    result = dict(row)
    result["attempts"] = int(result["attempts"]) + 1
    return result


def mark_sent(
    connection: sqlite3.Connection, token_address: str, sent_at_ms: int
) -> None:
    with connection:
        cursor = connection.execute(
            """
            UPDATE sol_early_outbox SET state='sent',sent_at_ms=?,last_error=NULL
            WHERE token_address=? AND state='sending'
            """,
            (sent_at_ms, token_address),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Sol early notification is not pending")


def mark_unknown(
    connection: sqlite3.Connection,
    token_address: str,
    *,
    error_code: str,
) -> None:
    with connection:
        cursor = connection.execute(
            """
            UPDATE sol_early_outbox SET state='unknown',last_error=?
            WHERE token_address=? AND state='sending'
            """,
            (str(error_code)[:128], token_address),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Sol early notification is not sending")


def counts(connection: sqlite3.Connection) -> Dict[str, int]:
    result = {"pending": 0, "sending": 0, "sent": 0, "unknown": 0}
    for row in connection.execute(
        "SELECT state,count(*) AS count FROM sol_early_outbox GROUP BY state"
    ):
        result[str(row["state"])] = int(row["count"])
    return result
