"""Frozen post-hoc cohort and horizon measurement storage.

The radar records why a candidate was accepted or rejected but never recorded
what happened afterwards, so no threshold can be justified with evidence. This
module defines the cohort that makes that measurement possible and the storage
for the horizon samples taken against it.

Three design decisions are load bearing.

**Stratification.** A cohort of delivered candidates alone cannot separate "the
gate is selective" from "the market went up for everything". The cohort
therefore carries three strata:

``delivered``
    ``strong + pass``. Every one is enrolled.
``evaluated_rejected``
    Reached the unified hard gate and was blocked. Every one is enrolled. This
    is the comparison that actually tests gate selectivity, because these
    candidates had complete evidence and were judged.
``not_evaluated``
    Never reached the unified gate, almost always because the enrichment budget
    expired. In the 2026-09-10 production baseline this is 99.5% of all events,
    so it is *sampled*, never taken whole, and it is a measurement of the
    budget shortfall rather than of any judgement. Mixing it into the
    comparison group would drown the signal; ``outcome_report`` keeps it
    separate for that reason.

**Unavailable is not zero.** A provider that returns no pair, no price, or an
error produces a row with a label and a ``NULL`` price. It never produces 0.0.
A missing measurement and a total loss are different facts and any aggregate
that conflates them is wrong.

**The baseline is when it was measured, not when it was decided.** Price
history is not available from the keyless market endpoint, so a baseline can
only be taken going forward. ``baseline_at_ms`` is the moment the baseline was
actually read and every horizon is counted from it; ``decision_evaluated_at_ms``
is kept alongside so the enrollment lag is visible instead of hidden.
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any, Dict, List, Optional, Sequence, Tuple


COHORT_VERSION = "outcome_cohort_v1"

STRATA = ("delivered", "evaluated_rejected", "not_evaluated")

UNIFIED_GATE_PROVIDER = "unified_gate"

# Horizons in minutes. README documents T+1m/5m/15m/1h/6h/24h as the product
# contract, so all six are measured rather than the five named in the layer
# plan. One shared definition: the sol-early cohort reuses this tuple.
HORIZON_MINUTES: Tuple[int, ...] = (1, 5, 15, 60, 360, 1440)

# How late a horizon may be read and still count as that horizon. A sample read
# outside its window is recorded as EXPIRED with the real elapsed time rather
# than silently relabelled, because a gap has to stay visible.
HORIZON_TOLERANCE_MS: Dict[int, int] = {
    1: 60_000,
    5: 120_000,
    15: 180_000,
    60: 300_000,
    360: 600_000,
    1440: 900_000,
}

OUTCOME_LABELS = ("ok", "unavailable", "error", "expired")

COHORT_SCHEMA = """
CREATE TABLE IF NOT EXISTS outcome_cohort (
    event_id TEXT NOT NULL REFERENCES radar_events(event_id),
    ruleset_version TEXT NOT NULL,
    cohort_version TEXT NOT NULL,
    stratum TEXT NOT NULL
        CHECK (stratum IN ('delivered','evaluated_rejected','not_evaluated')),
    chain TEXT NOT NULL,
    source TEXT NOT NULL,
    token_address TEXT NOT NULL,
    decision_evaluated_at_ms INTEGER NOT NULL,
    baseline_at_ms INTEGER NOT NULL,
    enrolled_at_ms INTEGER NOT NULL,
    risk_verdict TEXT NOT NULL,
    delivery TEXT NOT NULL,
    confidence_score INTEGER NOT NULL,
    opportunity_score INTEGER NOT NULL,
    baseline_status TEXT NOT NULL
        CHECK (baseline_status IN ('ok','unavailable','error')),
    baseline_price_usd REAL,
    baseline_market_cap_usd REAL,
    baseline_liquidity_usd REAL,
    PRIMARY KEY (event_id, ruleset_version, cohort_version)
);
CREATE INDEX IF NOT EXISTS idx_outcome_cohort_due
    ON outcome_cohort (cohort_version, baseline_at_ms);
CREATE INDEX IF NOT EXISTS idx_outcome_cohort_stratum
    ON outcome_cohort (cohort_version, stratum, baseline_at_ms);
CREATE INDEX IF NOT EXISTS idx_outcomes_cohort
    ON outcomes (ruleset_version, horizon_minutes);
"""


def initialize_cohort(connection: sqlite3.Connection) -> bool:
    """Add the cohort tables and the ``outcomes`` columns this module needs.

    Additive only, and the schema version is deliberately not bumped: the
    rollback release ignores the extra table and never writes ``outcomes``, so
    a database prepared here stays readable by it.
    """
    changed = False
    with connection:
        connection.executescript(COHORT_SCHEMA)
        existing = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(outcomes)")
        }
        for column, kind in (
            ("market_cap_usd", "REAL"),
            ("cohort_version", "TEXT"),
        ):
            if column not in existing:
                connection.execute(
                    "ALTER TABLE outcomes ADD COLUMN %s %s" % (column, kind)
                )
                changed = True
    return changed


def sample_fraction(event_id: str, *, cohort_version: str = COHORT_VERSION) -> float:
    """Stable position of ``event_id`` in [0, 1).

    Sampling the control stratum by a hash of the identity rather than by
    arrival order keeps the sample reproducible and independent of when the
    collector happened to be running.
    """
    digest = hashlib.sha256(
        ("%s|%s" % (cohort_version, event_id)).encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big") / float(1 << 64)


def _row_stratum(row: sqlite3.Row) -> str:
    if row["delivery"] == "strong" and row["risk_verdict"] == "pass":
        return "delivered"
    if int(row["gate_observations"] or 0) > 0:
        return "evaluated_rejected"
    return "not_evaluated"


def select_enrollment_candidates(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    cohort_version: str = COHORT_VERSION,
    control_sample_rate: float = 0.0,
    control_limit: int = 0,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """Decisions in ``[since_ms, until_ms)`` that are not enrolled yet.

    ``delivered`` and ``evaluated_rejected`` are taken whole. ``not_evaluated``
    is admitted only when its stable sample position falls under
    ``control_sample_rate``, and never more than ``control_limit`` per call.
    """
    if until_ms <= since_ms:
        raise ValueError("enrollment window must be positive")
    if not 0.0 <= control_sample_rate <= 1.0:
        raise ValueError("invalid control sample rate")
    rows = _unenrolled_decisions(
        connection,
        since_ms=since_ms,
        until_ms=until_ms,
        cohort_version=cohort_version,
    )
    selected: List[Dict[str, Any]] = []
    controls = 0
    for row in rows:
        stratum = _row_stratum(row)
        if stratum == "not_evaluated":
            if controls >= control_limit:
                continue
            position = sample_fraction(
                str(row["event_id"]),
                cohort_version=cohort_version,
            )
            if position >= control_sample_rate:
                continue
            controls += 1
        selected.append(_candidate(row, stratum))
        if len(selected) >= limit:
            break
    return selected


def _candidate(row: sqlite3.Row, stratum: str) -> Dict[str, Any]:
    return {
        "event_id": str(row["event_id"]),
        "ruleset_version": str(row["ruleset_version"]),
        "stratum": stratum,
        "chain": str(row["chain"]),
        "source": str(row["source"]),
        "token_address": str(row["token_address"]),
        "decision_evaluated_at_ms": int(row["evaluated_at_ms"]),
        "risk_verdict": str(row["risk_verdict"]),
        "delivery": str(row["delivery"]),
        "confidence_score": int(row["confidence_score"]),
        "opportunity_score": int(row["opportunity_score"]),
    }


def _unenrolled_decisions(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    cohort_version: str,
) -> List[sqlite3.Row]:
    return connection.execute(
        """
        SELECT d.event_id AS event_id,
               d.ruleset_version AS ruleset_version,
               d.evaluated_at_ms AS evaluated_at_ms,
               d.risk_verdict AS risk_verdict,
               d.delivery AS delivery,
               d.confidence_score AS confidence_score,
               d.opportunity_score AS opportunity_score,
               e.chain AS chain,
               e.source AS source,
               e.token_address AS token_address,
               e.is_backfill AS is_backfill,
               (SELECT COUNT(*) FROM provider_observations o
                 WHERE o.event_id = d.event_id
                   AND o.provider = ?) AS gate_observations
        FROM decisions d
        JOIN radar_events e ON e.event_id = d.event_id
        WHERE d.evaluated_at_ms >= ? AND d.evaluated_at_ms < ?
          AND e.is_backfill = 0
          AND NOT EXISTS (
              SELECT 1 FROM outcome_cohort c
              WHERE c.event_id = d.event_id
                AND c.ruleset_version = d.ruleset_version
                AND c.cohort_version = ?
          )
        ORDER BY d.evaluated_at_ms, d.event_id
        """,
        (UNIFIED_GATE_PROVIDER, since_ms, until_ms, cohort_version),
    ).fetchall()


def enroll_cohort_member(
    connection: sqlite3.Connection,
    *,
    candidate: Dict[str, Any],
    baseline_at_ms: int,
    enrolled_at_ms: int,
    baseline_status: str,
    price_usd: Optional[float],
    market_cap_usd: Optional[float],
    liquidity_usd: Optional[float],
    cohort_version: str = COHORT_VERSION,
) -> bool:
    """Freeze one cohort member. Returns False when already enrolled."""
    if baseline_status not in {"ok", "unavailable", "error"}:
        raise ValueError("invalid baseline status")
    if candidate["stratum"] not in STRATA:
        raise ValueError("invalid cohort stratum")
    if baseline_status != "ok" and price_usd is not None:
        raise ValueError("non-ok baseline cannot carry a price")
    with connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO outcome_cohort
              (event_id, ruleset_version, cohort_version, stratum, chain,
               source, token_address, decision_evaluated_at_ms, baseline_at_ms,
               enrolled_at_ms, risk_verdict, delivery, confidence_score,
               opportunity_score, baseline_status, baseline_price_usd,
               baseline_market_cap_usd, baseline_liquidity_usd)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate["event_id"],
                candidate["ruleset_version"],
                cohort_version,
                candidate["stratum"],
                candidate["chain"],
                candidate["source"],
                candidate["token_address"],
                int(candidate["decision_evaluated_at_ms"]),
                int(baseline_at_ms),
                int(enrolled_at_ms),
                candidate["risk_verdict"],
                candidate["delivery"],
                int(candidate["confidence_score"]),
                int(candidate["opportunity_score"]),
                baseline_status,
                price_usd,
                market_cap_usd,
                liquidity_usd,
            ),
        )
    return cursor.rowcount == 1


def due_horizons(
    connection: sqlite3.Connection,
    *,
    now_ms: int,
    cohort_version: str = COHORT_VERSION,
    horizons: Sequence[int] = HORIZON_MINUTES,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """Cohort members with a horizon that is due or already overdue.

    Overdue rows are returned too, tagged ``expired``, so the collector records
    the gap instead of leaving a hole that later looks like missing data.
    """
    members = connection.execute(
        """
        SELECT event_id, ruleset_version, chain, token_address, baseline_at_ms
        FROM outcome_cohort
        WHERE cohort_version = ?
          AND baseline_at_ms <= ?
        ORDER BY baseline_at_ms
        """,
        (cohort_version, now_ms),
    ).fetchall()
    done = {
        (str(row["event_id"]), str(row["ruleset_version"]), int(row["horizon_minutes"]))
        for row in connection.execute(
            "SELECT event_id, ruleset_version, horizon_minutes FROM outcomes"
        )
    }
    due: List[Dict[str, Any]] = []
    for row in members:
        baseline = int(row["baseline_at_ms"])
        for horizon in horizons:
            key = (str(row["event_id"]), str(row["ruleset_version"]), int(horizon))
            if key in done:
                continue
            scheduled = baseline + horizon * 60_000
            if now_ms < scheduled:
                continue
            tolerance = HORIZON_TOLERANCE_MS.get(int(horizon), 60_000)
            due.append(
                {
                    "event_id": str(row["event_id"]),
                    "ruleset_version": str(row["ruleset_version"]),
                    "chain": str(row["chain"]),
                    "token_address": str(row["token_address"]),
                    "horizon_minutes": int(horizon),
                    "scheduled_at_ms": scheduled,
                    "expired": now_ms > scheduled + tolerance,
                }
            )
            if len(due) >= limit:
                return due
    return due


def store_outcome(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    ruleset_version: str,
    horizon_minutes: int,
    scheduled_at_ms: int,
    checked_at_ms: int,
    source: str,
    outcome_label: str,
    price_usd: Optional[float] = None,
    market_cap_usd: Optional[float] = None,
    liquidity_usd: Optional[float] = None,
    error_code: Optional[str] = None,
    cohort_version: str = COHORT_VERSION,
) -> bool:
    """Record one horizon sample. Returns False when it already exists.

    ``price_usd`` stays ``NULL`` for every label but ``ok``. A provider that
    could not answer is not a price of zero.
    """
    if outcome_label not in OUTCOME_LABELS:
        raise ValueError("invalid outcome label")
    if outcome_label != "ok" and price_usd is not None:
        raise ValueError("only an ok outcome can carry a price")
    with connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO outcomes
              (event_id, ruleset_version, horizon_minutes, scheduled_at_ms,
               checked_at_ms, actual_elapsed_ms, source, price_usd,
               liquidity_usd, outcome_label, error_code, market_cap_usd,
               cohort_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                ruleset_version,
                int(horizon_minutes),
                int(scheduled_at_ms),
                int(checked_at_ms),
                int(checked_at_ms) - int(scheduled_at_ms),
                source,
                price_usd,
                liquidity_usd,
                outcome_label,
                error_code,
                market_cap_usd,
                cohort_version,
            ),
        )
    return cursor.rowcount == 1


def cohort_counts(
    connection: sqlite3.Connection,
    *,
    cohort_version: str = COHORT_VERSION,
) -> Dict[str, int]:
    counts = {stratum: 0 for stratum in STRATA}
    for row in connection.execute(
        """
        SELECT stratum, COUNT(*) AS count
        FROM outcome_cohort WHERE cohort_version = ?
        GROUP BY stratum
        """,
        (cohort_version,),
    ):
        counts[str(row["stratum"])] = int(row["count"])
    return counts


def outcome_counts(connection: sqlite3.Connection) -> Dict[str, int]:
    counts = {label: 0 for label in OUTCOME_LABELS}
    for row in connection.execute(
        "SELECT outcome_label, COUNT(*) AS count FROM outcomes GROUP BY outcome_label"
    ):
        counts[str(row["outcome_label"])] = int(row["count"])
    return counts
