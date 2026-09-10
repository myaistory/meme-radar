from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


NOTIFIER_SCHEMA_VERSION = 1

NOTIFIER_SCHEMA = """
CREATE TABLE IF NOT EXISTS pons_curve_notification_outbox (
    token_address TEXT PRIMARY KEY,
    claimed_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','sent')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 10),
    next_attempt_at_ms INTEGER NOT NULL,
    last_error TEXT,
    message_text TEXT NOT NULL,
    sent_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS pons_curve_notifier_state (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pons_curve_notification_due
  ON pons_curve_notification_outbox(state,next_attempt_at_ms);
"""


def connect_notifier(path: Path) -> sqlite3.Connection:
    existed = path.exists()
    connection = sqlite3.connect(str(path), timeout=10)
    if not existed:
        path.chmod(0o600)
    connection.row_factory = sqlite3.Row
    mode = str(connection.execute("PRAGMA journal_mode=WAL").fetchone()[0])
    if mode.lower() != "wal":
        connection.close()
        raise RuntimeError("SQLite WAL unavailable")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


def initialize_notifier(connection: sqlite3.Connection) -> None:
    current = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if current not in {0, NOTIFIER_SCHEMA_VERSION}:
        raise RuntimeError("unsupported Pons notifier schema")
    with connection:
        connection.executescript(NOTIFIER_SCHEMA)
        connection.execute("PRAGMA user_version=%d" % NOTIFIER_SCHEMA_VERSION)


def get_notifier_state(connection: sqlite3.Connection, key: str, default=None):
    row = connection.execute(
        "SELECT value_json FROM pons_curve_notifier_state WHERE key=?", (key,)
    ).fetchone()
    return default if row is None else json.loads(row["value_json"])


def set_notifier_state(
    connection: sqlite3.Connection,
    key: str,
    value: Any,
    now_ms: int,
) -> None:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"))
    with connection:
        connection.execute(
            """
            INSERT INTO pons_curve_notifier_state(key,value_json,updated_at_ms)
            VALUES (?,?,?) ON CONFLICT(key) DO UPDATE SET
              value_json=excluded.value_json,updated_at_ms=excluded.updated_at_ms
            """,
            (key, encoded, now_ms),
        )


def claim_notification(
    connection: sqlite3.Connection,
    *,
    token_address: str,
    message_text: str,
    now_ms: int,
    max_per_hour: int,
) -> str:
    if not token_address.startswith("0x") or len(token_address) != 42:
        raise ValueError("invalid notification token")
    if not message_text or len(message_text) > 4096:
        raise ValueError("invalid notification message")
    if not 1 <= max_per_hour <= 5:
        raise ValueError("invalid notifier hourly limit")
    with connection:
        existing = connection.execute(
            "SELECT 1 FROM pons_curve_notification_outbox WHERE token_address=?",
            (token_address,),
        ).fetchone()
        if existing is not None:
            return "deduplicated"
        recent = int(
            connection.execute(
                """
                SELECT count(*) FROM pons_curve_notification_outbox
                WHERE claimed_at_ms>?
                """,
                (now_ms - 3_600_000,),
            ).fetchone()[0]
        )
        if recent >= max_per_hour:
            return "rate_limited"
        connection.execute(
            """
            INSERT INTO pons_curve_notification_outbox
              (token_address,claimed_at_ms,state,attempts,next_attempt_at_ms,
               message_text)
            VALUES (?,?,'pending',0,?,?)
            """,
            (token_address, now_ms, now_ms, message_text),
        )
    return "enqueued"


def claim_due_notification(
    connection: sqlite3.Connection,
    *,
    now_ms: int,
    max_attempts: int,
) -> Optional[Dict[str, Any]]:
    if not 1 <= max_attempts <= 10:
        raise ValueError("invalid notifier attempts")
    with connection:
        row = connection.execute(
            """
            SELECT token_address,claimed_at_ms,attempts,message_text
            FROM pons_curve_notification_outbox
            WHERE state='pending' AND next_attempt_at_ms<=? AND attempts<?
            ORDER BY next_attempt_at_ms,claimed_at_ms LIMIT 1
            """,
            (now_ms, max_attempts),
        ).fetchone()
        if row is None:
            return None
        connection.execute(
            """
            UPDATE pons_curve_notification_outbox
            SET attempts=attempts+1,next_attempt_at_ms=?
            WHERE token_address=? AND state='pending'
            """,
            (now_ms + 60_000, row["token_address"]),
        )
    result = dict(row)
    result["attempts"] = int(result["attempts"]) + 1
    return result


def mark_notification_sent(
    connection: sqlite3.Connection,
    token_address: str,
    sent_at_ms: int,
) -> None:
    with connection:
        cursor = connection.execute(
            """
            UPDATE pons_curve_notification_outbox
            SET state='sent',sent_at_ms=?,last_error=NULL
            WHERE token_address=? AND state='pending'
            """,
            (sent_at_ms, token_address),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Pons notification is not pending")


def mark_notification_retry(
    connection: sqlite3.Connection,
    token_address: str,
    *,
    next_attempt_at_ms: int,
    error_code: str,
) -> None:
    safe_error = str(error_code)[:128]
    if not safe_error:
        raise ValueError("notifier retry error required")
    with connection:
        cursor = connection.execute(
            """
            UPDATE pons_curve_notification_outbox
            SET next_attempt_at_ms=?,last_error=?
            WHERE token_address=? AND state='pending'
            """,
            (next_attempt_at_ms, safe_error, token_address),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Pons notification is not pending")


def notifier_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    counts = {"pending": 0, "sent": 0}
    for row in connection.execute(
        """
        SELECT state,count(*) AS count
        FROM pons_curve_notification_outbox GROUP BY state
        """
    ):
        counts[str(row["state"])] = int(row["count"])
    return counts
