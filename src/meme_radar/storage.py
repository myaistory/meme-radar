from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from .models import Decision, FeatureSnapshot, RadarEvent
from .normalize import canonical_json, payload_sha256


# enrichment_jobs and the telegram_outbox terminal state are additive
# extensions. Keep v4 so the current rollback release can reopen the database:
# it ignores the extra table, and it never writes the extra state, so a
# database migrated here stays readable by the previous release.
SCHEMA_VERSION = 4


class EventIdentityConflict(RuntimeError):
    """An event with this identity is stored already, with different content.

    The stored row stays authoritative, so the source log *is* durably
    persisted. Callers that track "every log below this point is stored" may
    advance past it; only a failure to persist may stall them.
    """


# Kept separate so the terminal-state migration can rebuild exactly this table.
TELEGRAM_OUTBOX_SCHEMA = """
CREATE TABLE IF NOT EXISTS telegram_outbox (
    event_id TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    evaluated_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','sent','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at_ms INTEGER NOT NULL,
    last_error TEXT,
    message_text TEXT NOT NULL,
    sent_at_ms INTEGER,
    failed_at_ms INTEGER,
    PRIMARY KEY (event_id, ruleset_version, feature_version),
    FOREIGN KEY (event_id, ruleset_version, feature_version, evaluated_at_ms)
      REFERENCES decisions(event_id, ruleset_version, feature_version, evaluated_at_ms)
);
CREATE INDEX IF NOT EXISTS idx_telegram_outbox_due
  ON telegram_outbox(state, next_attempt_at_ms);
"""

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS raw_payloads (
    raw_sha256 TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS raw_batches (
    source TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    raw_sha256 TEXT NOT NULL REFERENCES raw_payloads(raw_sha256),
    observed_at_ms INTEGER NOT NULL,
    received_at_ms INTEGER NOT NULL,
    is_backfill INTEGER NOT NULL CHECK (is_backfill IN (0,1)),
    PRIMARY KEY (source, batch_id)
);
CREATE TABLE IF NOT EXISTS radar_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    chain TEXT NOT NULL,
    chain_id INTEGER NOT NULL,
    token_address TEXT NOT NULL,
    source_published_at_ms INTEGER NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    received_at_ms INTEGER NOT NULL,
    token_created_at_ms INTEGER,
    is_backfill INTEGER NOT NULL CHECK (is_backfill IN (0,1)),
    raw_sha256 TEXT NOT NULL REFERENCES raw_payloads(raw_sha256),
    event_json TEXT NOT NULL,
    UNIQUE (source, source_event_id)
);
CREATE TABLE IF NOT EXISTS decisions (
    event_id TEXT NOT NULL REFERENCES radar_events(event_id),
    ruleset_version TEXT NOT NULL,
    feature_version TEXT NOT NULL,
    evaluated_at_ms INTEGER NOT NULL,
    risk_verdict TEXT NOT NULL CHECK (risk_verdict IN ('pass','review','reject')),
    confidence_score INTEGER NOT NULL CHECK (confidence_score BETWEEN 0 AND 100),
    opportunity_score INTEGER NOT NULL CHECK (opportunity_score BETWEEN 0 AND 100),
    delivery TEXT NOT NULL CHECK (delivery IN ('strong','medium','weak','suppress')),
    decision_json TEXT NOT NULL,
    PRIMARY KEY (event_id, ruleset_version, feature_version, evaluated_at_ms)
);
CREATE TABLE IF NOT EXISTS telegram_delivery_claims (
    dedupe_key TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    claimed_at_ms INTEGER NOT NULL,
    FOREIGN KEY (event_id) REFERENCES radar_events(event_id)
);
CREATE TABLE IF NOT EXISTS outcomes (
    event_id TEXT NOT NULL REFERENCES radar_events(event_id),
    ruleset_version TEXT NOT NULL,
    horizon_minutes INTEGER NOT NULL,
    scheduled_at_ms INTEGER NOT NULL,
    checked_at_ms INTEGER NOT NULL,
    actual_elapsed_ms INTEGER NOT NULL,
    source TEXT NOT NULL,
    price_usd REAL,
    liquidity_usd REAL,
    outcome_label TEXT NOT NULL,
    error_code TEXT,
    PRIMARY KEY (event_id, ruleset_version, horizon_minutes)
);
CREATE TABLE IF NOT EXISTS provider_observations (
    event_id TEXT NOT NULL REFERENCES radar_events(event_id),
    provider TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok','unavailable','error')),
    payload_sha256 TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    summary_json TEXT NOT NULL,
    error_code TEXT,
    PRIMARY KEY (event_id, provider, observed_at_ms)
);
CREATE TABLE IF NOT EXISTS enrichment_jobs (
    event_id TEXT NOT NULL REFERENCES radar_events(event_id),
    priority_version TEXT NOT NULL,
    priority_score INTEGER NOT NULL CHECK (priority_score BETWEEN 0 AND 100),
    priority_reasons_json TEXT NOT NULL,
    feature_json TEXT NOT NULL,
    stage TEXT NOT NULL CHECK (stage IN ('dex_probe','dex_recheck','security_check')),
    dex_attempts INTEGER NOT NULL DEFAULT 0 CHECK (dex_attempts BETWEEN 0 AND 3),
    security_attempts INTEGER NOT NULL DEFAULT 0 CHECK (security_attempts BETWEEN 0 AND 2),
    market_json TEXT,
    due_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','processing','done')),
    result_code TEXT,
    last_error TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (event_id, priority_version)
);
CREATE TABLE IF NOT EXISTS runtime_state (
    key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_token_time
  ON radar_events(chain, token_address, observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_events_seed_reconcile
  ON radar_events(source, chain, event_kind, received_at_ms,
                  token_created_at_ms, event_id);
CREATE INDEX IF NOT EXISTS idx_decisions_delivery
  ON decisions(delivery, evaluated_at_ms);
CREATE INDEX IF NOT EXISTS idx_enrichment_jobs_due
  ON enrichment_jobs(state, due_at_ms, priority_score DESC);
"""


def connect(path: Path) -> sqlite3.Connection:
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
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def _migrate_telegram_outbox_terminal_state(
    connection: sqlite3.Connection,
) -> bool:
    """Give ``telegram_outbox`` a ``failed`` terminal state.

    SQLite cannot widen a CHECK constraint in place, so an existing table is
    rebuilt. The migration is idempotent: it inspects the stored DDL and is a
    no-op once the terminal state is present.
    """
    row = connection.execute(
        """
        SELECT sql FROM sqlite_master
        WHERE type='table' AND name='telegram_outbox'
        """
    ).fetchone()
    if row is None or "'failed'" in str(row["sql"] or ""):
        return False
    columns = (
        "event_id, ruleset_version, feature_version, evaluated_at_ms, "
        "state, attempts, next_attempt_at_ms, last_error, message_text, "
        "sent_at_ms"
    )
    connection.execute("PRAGMA foreign_keys = OFF")
    try:
        with connection:
            connection.execute(
                "ALTER TABLE telegram_outbox RENAME TO telegram_outbox_pre_failed"
            )
            connection.executescript(TELEGRAM_OUTBOX_SCHEMA)
            connection.execute(
                "INSERT INTO telegram_outbox (%s) SELECT %s "
                "FROM telegram_outbox_pre_failed" % (columns, columns)
            )
            connection.execute("DROP TABLE telegram_outbox_pre_failed")
        violations = connection.execute("PRAGMA foreign_key_check").fetchone()
        if violations is not None:
            raise RuntimeError("telegram outbox migration broke references")
    finally:
        connection.execute("PRAGMA foreign_keys = ON")
    return True


def initialize(connection: sqlite3.Connection) -> None:
    current = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if current < 0 or current > SCHEMA_VERSION:
        raise RuntimeError("unsupported database schema")
    with connection:
        connection.executescript(SCHEMA)
        connection.executescript(TELEGRAM_OUTBOX_SCHEMA)
        connection.execute(
            """
            UPDATE enrichment_jobs
            SET state='pending', last_error='RECOVERED_AFTER_RESTART'
            WHERE state='processing'
            """
        )
        connection.execute("PRAGMA user_version = %d" % SCHEMA_VERSION)
    _migrate_telegram_outbox_terminal_state(connection)


def store_raw_payload(
    connection: sqlite3.Connection,
    *,
    source: str,
    batch_id: str,
    observed_at_ms: int,
    received_at_ms: int,
    is_backfill: bool,
    payload: Dict[str, Any],
) -> str:
    encoded = canonical_json(payload)
    digest = payload_sha256(payload)
    with connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO raw_payloads (raw_sha256, payload_json)
            VALUES (?, ?)
            """,
            (digest, encoded),
        )
        existing_payload = connection.execute(
            "SELECT payload_json FROM raw_payloads WHERE raw_sha256=?",
            (digest,),
        ).fetchone()
        if existing_payload is None or existing_payload["payload_json"] != encoded:
            raise RuntimeError("raw payload hash collision")
        existing_batch = connection.execute(
            """
            SELECT raw_sha256, observed_at_ms, received_at_ms, is_backfill
            FROM raw_batches WHERE source=? AND batch_id=?
            """,
            (source, batch_id),
        ).fetchone()
        expected = (digest, observed_at_ms, received_at_ms, int(is_backfill))
        if existing_batch is not None:
            actual = tuple(existing_batch)
            if actual != expected:
                raise RuntimeError("conflicting raw batch identity")
            return digest
        connection.execute(
            """
            INSERT INTO raw_batches
              (source, batch_id, raw_sha256, observed_at_ms, received_at_ms,
               is_backfill)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                source,
                batch_id,
                digest,
                observed_at_ms,
                received_at_ms,
                int(is_backfill),
            ),
        )
    return digest


def store_event(connection: sqlite3.Connection, event: RadarEvent) -> bool:
    encoded = canonical_json(event.to_dict())
    with connection:
        existing = connection.execute(
            """
            SELECT event_json FROM radar_events
            WHERE source=? AND source_event_id=?
            """,
            (event.source, event.source_event_id),
        ).fetchone()
        if existing is not None:
            existing_value = json.loads(existing["event_json"])
            incoming_value = event.to_dict()
            for key in (
                "observed_at_ms",
                "received_at_ms",
                "raw_sha256",
                "is_backfill",
            ):
                existing_value.pop(key, None)
                incoming_value.pop(key, None)
            if existing_value != incoming_value:
                raise EventIdentityConflict("conflicting event identity")
            return False
        cursor = connection.execute(
            """
            INSERT INTO radar_events
              (event_id, source, source_event_id, event_kind, chain, chain_id,
               token_address, source_published_at_ms, observed_at_ms,
               received_at_ms, token_created_at_ms, is_backfill, raw_sha256,
               event_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.source,
                event.source_event_id,
                event.event_kind,
                event.chain,
                event.chain_id,
                event.token_address,
                event.source_published_at_ms,
                event.observed_at_ms,
                event.received_at_ms,
                event.token_created_at_ms,
                int(event.is_backfill),
                event.raw_sha256,
                encoded,
            ),
        )
    return cursor.rowcount == 1


def store_decision(connection: sqlite3.Connection, decision: Decision) -> bool:
    encoded = canonical_json(decision.to_dict())
    with connection:
        existing = connection.execute(
            """
            SELECT decision_json FROM decisions
            WHERE event_id=? AND ruleset_version=? AND feature_version=?
              AND evaluated_at_ms=?
            """,
            (
                decision.event_id,
                decision.ruleset_version,
                decision.feature_version,
                decision.evaluated_at_ms,
            ),
        ).fetchone()
        if existing is not None:
            if existing["decision_json"] != encoded:
                raise RuntimeError("conflicting decision identity")
            return False
        cursor = connection.execute(
            """
            INSERT INTO decisions
              (event_id, ruleset_version, feature_version, evaluated_at_ms,
               risk_verdict, confidence_score, opportunity_score, delivery,
               decision_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision.event_id,
                decision.ruleset_version,
                decision.feature_version,
                decision.evaluated_at_ms,
                decision.risk_verdict,
                decision.confidence_score,
                decision.opportunity_score,
                decision.delivery,
                encoded,
            ),
        )
    return cursor.rowcount == 1


def store_provider_observation(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    provider: str,
    observed_at_ms: int,
    status: str,
    payload: Dict[str, Any],
    summary: Dict[str, Any],
    error_code: Optional[str] = None,
) -> bool:
    if status not in {"ok", "unavailable", "error"}:
        raise ValueError("invalid provider status")
    payload_json = canonical_json(payload)
    summary_json = canonical_json(summary)
    digest = payload_sha256(payload)
    identity = (event_id, provider, observed_at_ms)
    with connection:
        existing = connection.execute(
            """
            SELECT status, payload_sha256, payload_json, summary_json, error_code
            FROM provider_observations
            WHERE event_id=? AND provider=? AND observed_at_ms=?
            """,
            identity,
        ).fetchone()
        expected = (status, digest, payload_json, summary_json, error_code)
        if existing is not None:
            if tuple(existing) != expected:
                raise RuntimeError("conflicting provider observation")
            return False
        connection.execute(
            """
            INSERT INTO provider_observations
              (event_id, provider, observed_at_ms, status, payload_sha256,
               payload_json, summary_json, error_code)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            identity
            + (
                status,
                digest,
                payload_json,
                summary_json,
                error_code,
            ),
        )
    return True


def schedule_enrichment_job(
    connection: sqlite3.Connection,
    *,
    event: RadarEvent,
    features: FeatureSnapshot,
    priority_version: str,
    priority_score: int,
    priority_reasons: tuple,
    due_at_ms: int,
    expires_at_ms: int,
) -> bool:
    if event.event_id != features.event_id:
        raise ValueError("event and feature identities do not match")
    if not priority_version or not 0 <= priority_score <= 100:
        raise ValueError("invalid enrichment priority")
    if due_at_ms <= 0 or expires_at_ms <= due_at_ms:
        raise ValueError("invalid enrichment schedule")
    reasons_json = canonical_json(list(priority_reasons))
    feature_json = canonical_json(features.to_dict())
    immutable = (
        priority_score,
        reasons_json,
        feature_json,
        due_at_ms,
        expires_at_ms,
    )
    values = (
        event.event_id,
        priority_version,
    ) + immutable + (
        event.received_at_ms,
        event.received_at_ms,
    )
    with connection:
        existing = connection.execute(
            """
            SELECT priority_score, priority_reasons_json, feature_json,
                   due_at_ms, expires_at_ms
            FROM enrichment_jobs
            WHERE event_id=? AND priority_version=?
            """,
            (event.event_id, priority_version),
        ).fetchone()
        if existing is not None:
            if tuple(existing) != immutable:
                raise RuntimeError("conflicting enrichment job identity")
            return False
        active = connection.execute(
            """
            SELECT j.event_id,j.priority_score,j.state
            FROM enrichment_jobs j
            JOIN radar_events e ON e.event_id=j.event_id
            WHERE j.priority_version=? AND j.state IN ('pending','processing')
              AND e.chain=? AND e.token_address=?
            ORDER BY CASE j.state WHEN 'processing' THEN 0 ELSE 1 END,
                     j.priority_score DESC
            LIMIT 1
            """,
            (priority_version, event.chain, event.token_address),
        ).fetchone()
        if active is not None:
            if (
                active["state"] == "pending"
                and priority_score > int(active["priority_score"])
            ):
                connection.execute(
                    """
                    UPDATE enrichment_jobs
                    SET state='done',result_code='TOKEN_COALESCED',updated_at_ms=?
                    WHERE event_id=? AND priority_version=? AND state='pending'
                    """,
                    (
                        event.received_at_ms,
                        active["event_id"],
                        priority_version,
                    ),
                )
            else:
                return False
        cursor = connection.execute(
            """
            INSERT INTO enrichment_jobs
              (event_id, priority_version, priority_score,
               priority_reasons_json, feature_json, stage, due_at_ms,
               expires_at_ms, state, created_at_ms, updated_at_ms)
            VALUES (?, ?, ?, ?, ?, 'dex_probe', ?, ?, 'pending', ?, ?)
            """,
            values,
        )
    return cursor.rowcount == 1


def claim_due_enrichment(
    connection: sqlite3.Connection,
    *,
    now_ms: int,
    initial_available: bool = True,
    initial_chains: tuple = ("bsc", "base", "robinhood"),
    size_recheck_available: bool = True,
    dex_recheck_available: bool = True,
    dex_available: bool = True,
    goplus_available: bool = True,
) -> Optional[Dict[str, Any]]:
    with connection:
        connection.execute(
            """
            UPDATE enrichment_jobs
            SET state='done', result_code='EXPIRED', updated_at_ms=?
            WHERE state='pending' AND expires_at_ms <= ?
            """,
            (now_ms, now_ms),
        )
        row = connection.execute(
            """
            SELECT j.*, e.event_json
            FROM enrichment_jobs AS j
            JOIN radar_events AS e ON e.event_id=j.event_id
            WHERE j.state='pending' AND j.due_at_ms <= ?
              AND j.expires_at_ms > ?
              AND ((j.stage='security_check' AND ?)
                   OR (j.stage='dex_recheck' AND ? AND ?)
                   OR (j.stage='dex_probe' AND (
                       EXISTS (
                         SELECT 1 FROM provider_observations p
                         WHERE p.event_id=j.event_id
                           AND p.provider='gmgn_size'
                       ) AND ?
                       OR (? AND ?
                           AND ((e.chain='bsc' AND ?)
                                OR (e.chain='base' AND ?)
                                OR (e.chain='robinhood' AND ?))))))
            ORDER BY CASE j.stage
                       WHEN 'security_check' THEN 0
                       WHEN 'dex_recheck' THEN 1
                       ELSE 2
                     END,
                     j.priority_score DESC, j.due_at_ms, j.event_id
            LIMIT 1
            """,
            (
                now_ms,
                now_ms,
                int(goplus_available),
                int(dex_available),
                int(dex_recheck_available),
                int(size_recheck_available),
                int(dex_available),
                int(initial_available),
                int("bsc" in initial_chains),
                int("base" in initial_chains),
                int("robinhood" in initial_chains),
            ),
        ).fetchone()
        if row is None:
            return None
        cursor = connection.execute(
            """
            UPDATE enrichment_jobs SET state='processing', updated_at_ms=?
            WHERE event_id=? AND priority_version=? AND state='pending'
            """,
            (now_ms, row["event_id"], row["priority_version"]),
        )
        if cursor.rowcount != 1:
            return None
    result = dict(row)
    result["event"] = RadarEvent(**json.loads(result.pop("event_json")))
    result["features"] = FeatureSnapshot(**json.loads(result["feature_json"]))
    result["priority_reasons"] = tuple(
        json.loads(result["priority_reasons_json"])
    )
    return result


def enrichment_queue_under_pressure(
    connection: sqlite3.Connection,
    *,
    threshold: int = 100,
) -> bool:
    if threshold <= 0:
        raise ValueError("invalid enrichment pressure threshold")
    row = connection.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT 1 FROM enrichment_jobs WHERE state='pending' LIMIT ?
        )
        """,
        (threshold,),
    ).fetchone()
    return int(row[0]) >= threshold


def reschedule_enrichment_job(
    connection: sqlite3.Connection,
    job: Dict[str, Any],
    *,
    stage: str,
    dex_attempts: int,
    security_attempts: int,
    due_at_ms: int,
    now_ms: int,
    market: Optional[Dict[str, Any]] = None,
    last_error: Optional[str] = None,
) -> None:
    if stage not in {"dex_probe", "dex_recheck", "security_check"}:
        raise ValueError("invalid enrichment stage")
    safe_error = str(last_error)[:128] if last_error else None
    market_json = canonical_json(market) if market is not None else None
    with connection:
        cursor = connection.execute(
            """
            UPDATE enrichment_jobs
            SET state='pending', stage=?, dex_attempts=?, security_attempts=?,
                due_at_ms=?, market_json=?, last_error=?, updated_at_ms=?
            WHERE event_id=? AND priority_version=? AND state='processing'
            """,
            (
                stage,
                dex_attempts,
                security_attempts,
                due_at_ms,
                market_json,
                safe_error,
                now_ms,
                job["event_id"],
                job["priority_version"],
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("enrichment job is not processing")


def finish_enrichment_job(
    connection: sqlite3.Connection,
    job: Dict[str, Any],
    *,
    result_code: str,
    now_ms: int,
) -> None:
    safe_result = str(result_code)[:128]
    if not safe_result:
        raise ValueError("enrichment result required")
    with connection:
        cursor = connection.execute(
            """
            UPDATE enrichment_jobs
            SET state='done', result_code=?, updated_at_ms=?
            WHERE event_id=? AND priority_version=? AND state='processing'
            """,
            (
                safe_result,
                now_ms,
                job["event_id"],
                job["priority_version"],
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("enrichment job is not processing")


def enrichment_job_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    counts = {"pending": 0, "processing": 0, "done": 0}
    for row in connection.execute(
        "SELECT state, COUNT(*) AS count FROM enrichment_jobs GROUP BY state"
    ):
        counts[str(row["state"])] = int(row["count"])
    return counts


def enqueue_telegram(
    connection: sqlite3.Connection,
    *,
    decision: Decision,
    dedupe_key: str,
    message_text: str,
    now_ms: int,
) -> bool:
    if decision.delivery != "strong" or decision.risk_verdict != "pass":
        raise ValueError("only strong pass decisions can be enqueued")
    if not dedupe_key or len(dedupe_key) > 256:
        raise ValueError("invalid Telegram dedupe key")
    if not message_text or len(message_text) > 4096:
        raise ValueError("invalid Telegram message")
    with connection:
        claimed = connection.execute(
            """
            INSERT OR IGNORE INTO telegram_delivery_claims
              (dedupe_key, event_id, ruleset_version, claimed_at_ms)
            VALUES (?, ?, ?, ?)
            """,
            (
                dedupe_key,
                decision.event_id,
                decision.ruleset_version,
                now_ms,
            ),
        )
        if claimed.rowcount != 1:
            return False
        connection.execute(
            """
            INSERT INTO telegram_outbox
              (event_id, ruleset_version, feature_version, evaluated_at_ms,
               state, attempts, next_attempt_at_ms, message_text)
            VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
            """,
            (
                decision.event_id,
                decision.ruleset_version,
                decision.feature_version,
                decision.evaluated_at_ms,
                now_ms,
                message_text,
            ),
        )
    return True


def enqueue_observation_sample(
    connection: sqlite3.Connection,
    *,
    decision: Decision,
    dedupe_key: str,
    message_text: str,
    now_ms: int,
    max_per_hour: int,
) -> str:
    if decision.risk_verdict != "review" or decision.delivery != "weak":
        raise ValueError("only review weak decisions can be sampled")
    sample_version = dedupe_key.split(":", 1)[0]
    if (
        sample_version not in {
            "sample_v1",
            "sample_v2_unified",
            "sample_v3_size_gate",
            "sample_v4_size_recheck",
            "sample_v5_gmgn_growth",
            "sample_v6_ratio_any_one",
        }
        or len(dedupe_key) > 256
    ):
        raise ValueError("invalid sample dedupe key")
    if not message_text or len(message_text) > 4096:
        raise ValueError("invalid sample message")
    if not 1 <= max_per_hour <= 10:
        raise ValueError("invalid sample hourly limit")
    with connection:
        count = connection.execute(
            """
            SELECT COUNT(*) FROM telegram_delivery_claims
            WHERE dedupe_key LIKE 'sample_v%:%' AND claimed_at_ms > ?
            """,
            (now_ms - 3_600_000,),
        ).fetchone()[0]
        if int(count) >= max_per_hour:
            return "rate_limited"
        claimed = connection.execute(
            """
            INSERT OR IGNORE INTO telegram_delivery_claims
              (dedupe_key, event_id, ruleset_version, claimed_at_ms)
            VALUES (?, ?, ?, ?)
            """,
            (
                dedupe_key,
                decision.event_id,
                decision.ruleset_version,
                now_ms,
            ),
        )
        if claimed.rowcount != 1:
            return "deduplicated"
        connection.execute(
            """
            INSERT INTO telegram_outbox
              (event_id, ruleset_version, feature_version, evaluated_at_ms,
               state, attempts, next_attempt_at_ms, message_text)
            VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
            """,
            (
                decision.event_id,
                decision.ruleset_version,
                decision.feature_version,
                decision.evaluated_at_ms,
                now_ms,
                message_text,
            ),
        )
    return "enqueued"


def claim_due_telegram(
    connection: sqlite3.Connection,
    *,
    now_ms: int,
    max_attempts: int,
) -> Optional[Dict[str, Any]]:
    if max_attempts < 1:
        raise ValueError("invalid Telegram max attempts")
    with connection:
        connection.execute(
            """
            UPDATE telegram_outbox
            SET state='failed', failed_at_ms=?,
                last_error=COALESCE(last_error, 'MAX_ATTEMPTS_EXHAUSTED')
            WHERE state='pending' AND attempts >= ?
            """,
            (now_ms, max_attempts),
        )
        row = connection.execute(
            """
            SELECT event_id, ruleset_version, feature_version, evaluated_at_ms,
                   attempts, message_text
            FROM telegram_outbox
            WHERE state='pending' AND next_attempt_at_ms <= ? AND attempts < ?
            ORDER BY next_attempt_at_ms, evaluated_at_ms
            LIMIT 1
            """,
            (now_ms, max_attempts),
        ).fetchone()
        if row is None:
            return None
        identity = (
            row["event_id"],
            row["ruleset_version"],
            row["feature_version"],
        )
        connection.execute(
            """
            UPDATE telegram_outbox
            SET attempts=attempts+1, next_attempt_at_ms=?
            WHERE event_id=? AND ruleset_version=? AND feature_version=?
            """,
            (now_ms + 60_000,) + identity,
        )
    result = dict(row)
    result["attempts"] = int(result["attempts"]) + 1
    return result


def mark_telegram_sent(
    connection: sqlite3.Connection,
    item: Dict[str, Any],
    *,
    sent_at_ms: int,
) -> None:
    with connection:
        cursor = connection.execute(
            """
            UPDATE telegram_outbox
            SET state='sent', sent_at_ms=?, last_error=NULL
            WHERE event_id=? AND ruleset_version=? AND feature_version=?
              AND state='pending'
            """,
            (
                sent_at_ms,
                item["event_id"],
                item["ruleset_version"],
                item["feature_version"],
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Telegram outbox item is not pending")


def mark_telegram_retry(
    connection: sqlite3.Connection,
    item: Dict[str, Any],
    *,
    next_attempt_at_ms: int,
    error_code: str,
) -> None:
    safe_error = str(error_code)[:128]
    if not safe_error:
        raise ValueError("Telegram retry error required")
    with connection:
        cursor = connection.execute(
            """
            UPDATE telegram_outbox
            SET next_attempt_at_ms=?, last_error=?
            WHERE event_id=? AND ruleset_version=? AND feature_version=?
              AND state='pending'
            """,
            (
                next_attempt_at_ms,
                safe_error,
                item["event_id"],
                item["ruleset_version"],
                item["feature_version"],
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Telegram outbox item is not pending")


def mark_telegram_failed(
    connection: sqlite3.Connection,
    item: Dict[str, Any],
    *,
    failed_at_ms: int,
    error_code: str,
) -> None:
    """Retire a pending item permanently, without waiting for more retries."""
    safe_error = str(error_code)[:128]
    if not safe_error:
        raise ValueError("Telegram failure error required")
    with connection:
        cursor = connection.execute(
            """
            UPDATE telegram_outbox
            SET state='failed', failed_at_ms=?, last_error=?
            WHERE event_id=? AND ruleset_version=? AND feature_version=?
              AND state='pending'
            """,
            (
                failed_at_ms,
                safe_error,
                item["event_id"],
                item["ruleset_version"],
                item["feature_version"],
            ),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Telegram outbox item is not pending")


def telegram_outbox_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    counts = {"pending": 0, "sent": 0, "failed": 0}
    for row in connection.execute(
        "SELECT state, COUNT(*) AS count FROM telegram_outbox GROUP BY state"
    ):
        counts[str(row["state"])] = int(row["count"])
    return counts


def get_runtime_state(
    connection: sqlite3.Connection,
    key: str,
    default: Any = None,
) -> Any:
    row = connection.execute(
        "SELECT value_json FROM runtime_state WHERE key=?",
        (key,),
    ).fetchone()
    if row is None:
        return default
    return json.loads(row["value_json"])


def set_runtime_state(
    connection: sqlite3.Connection,
    key: str,
    value: Any,
    updated_at_ms: int,
) -> None:
    encoded = canonical_json(value)
    with connection:
        connection.execute(
            """
            INSERT INTO runtime_state (key, value_json, updated_at_ms)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
              value_json=excluded.value_json,
              updated_at_ms=excluded.updated_at_ms
            """,
            (key, encoded, updated_at_ms),
        )
