"""Read-only funnel and gate-selectivity aggregation.

The radar persists every gate outcome it evaluates: enrichment stage results in
``enrichment_jobs.result_code``, unified hard-gate reason codes in
``provider_observations`` under the ``unified_gate`` provider, and final scoring
reason codes in ``decisions.decision_json``. This module joins those three
records into a single funnel so that gate selectivity can be measured before any
threshold is changed.

Nothing here writes to the database. The connection is opened with
``mode=ro`` and ``PRAGMA query_only`` so it is safe against a live production
file.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple


FUNNEL_REPORT_VERSION = "funnel_report_v1"

UNIFIED_GATE_PROVIDER = "unified_gate"
GATE_PASS_REASON = "RISK_CHECKS_PASSED"


def open_readonly(path: Path) -> sqlite3.Connection:
    """Open ``path`` read-only. Raises when the file is missing."""
    resolved = Path(path)
    if not resolved.is_file():
        raise RuntimeError("funnel report database not found")
    connection = sqlite3.connect(
        "file:%s?mode=ro" % resolved.as_posix(),
        uri=True,
        timeout=10,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = 1")
    connection.execute("PRAGMA busy_timeout = 10000")
    return connection


def _window_clause(
    column: str,
    since_ms: int,
    until_ms: int,
) -> Tuple[str, List[Any]]:
    return "%s >= ? AND %s < ?" % (column, column), [since_ms, until_ms]


def _scope_clause(
    chain: Optional[str],
    source: Optional[str],
    prefix: str = "e.",
) -> Tuple[str, List[Any]]:
    parts: List[str] = []
    params: List[Any] = []
    if chain:
        parts.append("%schain = ?" % prefix)
        params.append(chain)
    if source:
        parts.append("%ssource = ?" % prefix)
        params.append(source)
    return ("" if not parts else " AND " + " AND ".join(parts)), params


def _reason_codes(payload: Optional[str]) -> Tuple[str, ...]:
    if not payload:
        return ()
    try:
        decoded = json.loads(payload)
    except (TypeError, ValueError):
        return ()
    if not isinstance(decoded, dict):
        return ()
    codes = decoded.get("reason_codes")
    if not isinstance(codes, list):
        return ()
    return tuple(str(code) for code in codes if isinstance(code, str))


def _tally(counter: Dict[str, int], key: str, amount: int = 1) -> None:
    counter[key] = counter.get(key, 0) + amount


def _rank(
    counter: Mapping[str, int],
    *,
    total: int,
    sole: Optional[Mapping[str, int]] = None,
    top: int = 0,
) -> List[Dict[str, Any]]:
    rows = [
        {
            "reason": reason,
            "count": count,
            "share": round(count / total, 4) if total else 0.0,
            "sole_blocker": 0 if sole is None else int(sole.get(reason, 0)),
        }
        for reason, count in counter.items()
    ]
    rows.sort(key=lambda row: (-row["count"], row["reason"]))
    return rows if top <= 0 else rows[:top]


def collect_events(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    chain: Optional[str],
    source: Optional[str],
) -> Dict[str, Any]:
    where, params = _window_clause("e.received_at_ms", since_ms, until_ms)
    scope, scope_params = _scope_clause(chain, source)
    rows = connection.execute(
        """
        SELECT e.source AS source, e.chain AS chain,
               COUNT(*) AS events,
               SUM(e.is_backfill) AS backfill
        FROM radar_events e
        WHERE %s%s
        GROUP BY e.source, e.chain
        """
        % (where, scope),
        params + scope_params,
    ).fetchall()
    by_scope = {
        "%s|%s" % (row["source"], row["chain"]): {
            "events": int(row["events"]),
            "backfill": int(row["backfill"] or 0),
        }
        for row in rows
    }
    return {
        "total": sum(item["events"] for item in by_scope.values()),
        "backfill": sum(item["backfill"] for item in by_scope.values()),
        "by_source_chain": dict(sorted(by_scope.items())),
    }


def collect_enrichment(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    chain: Optional[str],
    source: Optional[str],
) -> Dict[str, Any]:
    where, params = _window_clause("j.created_at_ms", since_ms, until_ms)
    scope, scope_params = _scope_clause(chain, source)
    rows = connection.execute(
        """
        SELECT j.state AS state,
               COALESCE(j.result_code, 'NONE') AS result_code,
               COUNT(*) AS count
        FROM enrichment_jobs j
        JOIN radar_events e ON e.event_id = j.event_id
        WHERE %s%s
        GROUP BY j.state, j.result_code
        """
        % (where, scope),
        params + scope_params,
    ).fetchall()
    by_state: Dict[str, int] = {}
    by_result: Dict[str, int] = {}
    for row in rows:
        _tally(by_state, str(row["state"]), int(row["count"]))
        _tally(by_result, str(row["result_code"]), int(row["count"]))
    return {
        "total": sum(by_state.values()),
        "by_state": dict(sorted(by_state.items())),
        "by_result_code": dict(
            sorted(by_result.items(), key=lambda item: (-item[1], item[0]))
        ),
    }


def collect_unified_gate(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    chain: Optional[str],
    source: Optional[str],
    top: int,
) -> Dict[str, Any]:
    where, params = _window_clause("o.observed_at_ms", since_ms, until_ms)
    scope, scope_params = _scope_clause(chain, source)
    rows = connection.execute(
        """
        SELECT o.summary_json AS summary_json, e.chain AS chain,
               e.source AS source
        FROM provider_observations o
        JOIN radar_events e ON e.event_id = o.event_id
        WHERE o.provider = ? AND %s%s
        """
        % (where, scope),
        [UNIFIED_GATE_PROVIDER] + params + scope_params,
    ).fetchall()
    blocked_by: Dict[str, int] = {}
    sole_by: Dict[str, int] = {}
    per_scope: Dict[str, Dict[str, int]] = {}
    passed = 0
    for row in rows:
        codes = _reason_codes(row["summary_json"])
        key = "%s|%s" % (row["source"], row["chain"])
        bucket = per_scope.setdefault(key, {"observed": 0, "passed": 0})
        bucket["observed"] += 1
        if not codes:
            passed += 1
            bucket["passed"] += 1
            continue
        unique = sorted(set(codes))
        for reason in unique:
            _tally(blocked_by, reason)
        if len(unique) == 1:
            _tally(sole_by, unique[0])
    observed = len(rows)
    return {
        "observations": observed,
        "passed": passed,
        "blocked": observed - passed,
        "pass_rate": round(passed / observed, 4) if observed else 0.0,
        "by_source_chain": dict(sorted(per_scope.items())),
        "gates": _rank(blocked_by, total=observed, sole=sole_by, top=top),
    }


def collect_decisions(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    chain: Optional[str],
    source: Optional[str],
    ruleset: Optional[str],
    top: int,
) -> Dict[str, Any]:
    where, params = _window_clause("d.evaluated_at_ms", since_ms, until_ms)
    scope, scope_params = _scope_clause(chain, source)
    if ruleset:
        scope += " AND d.ruleset_version = ?"
        scope_params = scope_params + [ruleset]
    rows = connection.execute(
        """
        SELECT d.ruleset_version AS ruleset_version,
               d.risk_verdict AS risk_verdict,
               d.delivery AS delivery,
               d.confidence_score AS confidence_score,
               d.opportunity_score AS opportunity_score,
               d.decision_json AS decision_json,
               e.chain AS chain, e.source AS source
        FROM decisions d
        JOIN radar_events e ON e.event_id = d.event_id
        WHERE %s%s
        """
        % (where, scope),
        params + scope_params,
    ).fetchall()
    by_ruleset: Dict[str, int] = {}
    by_verdict: Dict[str, int] = {}
    by_delivery: Dict[str, int] = {}
    reasons: Dict[str, int] = {}
    sole: Dict[str, int] = {}
    per_scope: Dict[str, Dict[str, int]] = {}
    for row in rows:
        _tally(by_ruleset, str(row["ruleset_version"]))
        _tally(by_verdict, str(row["risk_verdict"]))
        _tally(by_delivery, str(row["delivery"]))
        key = "%s|%s" % (row["source"], row["chain"])
        bucket = per_scope.setdefault(
            key,
            {"decisions": 0, "pass": 0, "strong": 0},
        )
        bucket["decisions"] += 1
        if row["risk_verdict"] == "pass":
            bucket["pass"] += 1
        if row["delivery"] == "strong":
            bucket["strong"] += 1
        codes = [
            code
            for code in _reason_codes(row["decision_json"])
            if not code.startswith(("CONFIDENCE_", "OPPORTUNITY_", "DELIVERY_"))
        ]
        unique = sorted({code for code in codes if code != GATE_PASS_REASON})
        for reason in unique:
            _tally(reasons, reason)
        if len(unique) == 1:
            _tally(sole, unique[0])
    total = len(rows)
    return {
        "total": total,
        "by_ruleset": dict(sorted(by_ruleset.items())),
        "by_verdict": dict(sorted(by_verdict.items())),
        "by_delivery": dict(sorted(by_delivery.items())),
        "by_source_chain": dict(sorted(per_scope.items())),
        "risk_reasons": _rank(reasons, total=total, sole=sole, top=top),
    }


def collect_delivery(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
) -> Dict[str, Any]:
    where, params = _window_clause("claimed_at_ms", since_ms, until_ms)
    claims = connection.execute(
        """
        SELECT SUBSTR(dedupe_key, 1, INSTR(dedupe_key || ':', ':') - 1) AS kind,
               COUNT(*) AS count
        FROM telegram_delivery_claims
        WHERE %s
        GROUP BY kind
        """
        % where,
        params,
    ).fetchall()
    outbox = connection.execute(
        """
        SELECT state, COUNT(*) AS count
        FROM telegram_outbox
        WHERE evaluated_at_ms >= ? AND evaluated_at_ms < ?
        GROUP BY state
        """,
        (since_ms, until_ms),
    ).fetchall()
    return {
        "claims_by_ruleset": {
            str(row["kind"]): int(row["count"]) for row in claims
        },
        "claims": sum(int(row["count"]) for row in claims),
        "outbox_by_state": {
            str(row["state"]): int(row["count"]) for row in outbox
        },
    }


def collect_hourly(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    chain: Optional[str],
    source: Optional[str],
) -> List[Dict[str, Any]]:
    where, params = _window_clause("e.received_at_ms", since_ms, until_ms)
    scope, scope_params = _scope_clause(chain, source)
    rows = connection.execute(
        """
        SELECT e.received_at_ms / 3600000 AS hour_index,
               COUNT(*) AS events,
               SUM(CASE WHEN d.delivery = 'strong' THEN 1 ELSE 0 END) AS strong,
               SUM(CASE WHEN d.risk_verdict = 'pass' THEN 1 ELSE 0 END) AS passed
        FROM radar_events e
        LEFT JOIN decisions d ON d.event_id = e.event_id
        WHERE %s%s
        GROUP BY hour_index
        ORDER BY hour_index
        """
        % (where, scope),
        params + scope_params,
    ).fetchall()
    return [
        {
            "hour_start_ms": int(row["hour_index"]) * 3_600_000,
            "events": int(row["events"]),
            "pass": int(row["passed"] or 0),
            "strong": int(row["strong"] or 0),
        }
        for row in rows
    ]


def build_report(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    chain: Optional[str] = None,
    source: Optional[str] = None,
    ruleset: Optional[str] = None,
    top: int = 25,
    hourly: bool = False,
) -> Dict[str, Any]:
    if until_ms <= since_ms:
        raise ValueError("funnel window must be positive")
    scope = {
        "since_ms": since_ms,
        "until_ms": until_ms,
        "chain": chain,
        "source": source,
        "ruleset_version": ruleset,
    }
    events = collect_events(
        connection,
        since_ms=since_ms,
        until_ms=until_ms,
        chain=chain,
        source=source,
    )
    enrichment = collect_enrichment(
        connection,
        since_ms=since_ms,
        until_ms=until_ms,
        chain=chain,
        source=source,
    )
    gate = collect_unified_gate(
        connection,
        since_ms=since_ms,
        until_ms=until_ms,
        chain=chain,
        source=source,
        top=top,
    )
    decisions = collect_decisions(
        connection,
        since_ms=since_ms,
        until_ms=until_ms,
        chain=chain,
        source=source,
        ruleset=ruleset,
        top=top,
    )
    delivery = collect_delivery(
        connection,
        since_ms=since_ms,
        until_ms=until_ms,
    )
    report = {
        "version": FUNNEL_REPORT_VERSION,
        "window_hours": round((until_ms - since_ms) / 3_600_000, 3),
        "filters": scope,
        "events": events,
        "enrichment": enrichment,
        "unified_gate": gate,
        "decisions": decisions,
        "delivery": delivery,
        "funnel": [
            {"stage": "events", "count": events["total"]},
            {"stage": "enrichment_jobs", "count": enrichment["total"]},
            {"stage": "unified_gate_observed", "count": gate["observations"]},
            {"stage": "unified_gate_passed", "count": gate["passed"]},
            {"stage": "decisions", "count": decisions["total"]},
            {
                "stage": "risk_pass",
                "count": decisions["by_verdict"].get("pass", 0),
            },
            {
                "stage": "delivery_strong",
                "count": decisions["by_delivery"].get("strong", 0),
            },
            {"stage": "telegram_claims", "count": delivery["claims"]},
        ],
    }
    if hourly:
        report["hourly"] = collect_hourly(
            connection,
            since_ms=since_ms,
            until_ms=until_ms,
            chain=chain,
            source=source,
        )
    return report


def _table(title: str, rows: Mapping[str, Any]) -> List[str]:
    lines = ["", title]
    if not rows:
        lines.append("  (empty)")
        return lines
    width = max(len(str(key)) for key in rows)
    for key, value in rows.items():
        lines.append("  %-*s  %s" % (width, key, value))
    return lines


def render_text(report: Mapping[str, Any]) -> str:
    lines = [
        "Meme Radar funnel report (%s)" % report["version"],
        "window: %s hours  filters: %s"
        % (
            report["window_hours"],
            ", ".join(
                "%s=%s" % (key, value)
                for key, value in sorted(report["filters"].items())
                if value is not None
            )
            or "none",
        ),
        "",
        "funnel",
    ]
    stage_width = max(len(item["stage"]) for item in report["funnel"])
    head = report["funnel"][0]["count"] or 0
    previous = None
    for item in report["funnel"]:
        count = item["count"]
        step = "-" if previous in (None, 0) else "%.4f" % (count / previous)
        overall = "-" if not head else "%.6f" % (count / head)
        lines.append(
            "  %-*s  %8d  step=%-8s  of_events=%s"
            % (stage_width, item["stage"], count, step, overall)
        )
        previous = count
    lines.extend(
        _table(
            "events by source|chain",
            {
                key: "events=%d backfill=%d"
                % (value["events"], value["backfill"])
                for key, value in report["events"]["by_source_chain"].items()
            },
        )
    )
    lines.extend(
        _table(
            "decisions by source|chain",
            {
                key: "decisions=%d pass=%d strong=%d"
                % (value["decisions"], value["pass"], value["strong"])
                for key, value in report["decisions"]["by_source_chain"].items()
            },
        )
    )
    lines.extend(_table("enrichment result codes", report["enrichment"]["by_result_code"]))
    lines.extend(_table("decision delivery", report["decisions"]["by_delivery"]))
    lines.extend(_table("decision verdict", report["decisions"]["by_verdict"]))
    lines.append("")
    lines.append(
        "unified gate selectivity (observed=%d passed=%d rate=%s)"
        % (
            report["unified_gate"]["observations"],
            report["unified_gate"]["passed"],
            report["unified_gate"]["pass_rate"],
        )
    )
    lines.extend(_gate_rows(report["unified_gate"]["gates"]))
    lines.append("")
    lines.append("risk reason selectivity (decisions=%d)" % report["decisions"]["total"])
    lines.extend(_gate_rows(report["decisions"]["risk_reasons"]))
    lines.extend(
        _table("telegram claims by ruleset", report["delivery"]["claims_by_ruleset"])
    )
    lines.extend(_table("telegram outbox", report["delivery"]["outbox_by_state"]))
    if "hourly" in report:
        lines.extend(
            _table(
                "hourly events/pass/strong",
                {
                    str(item["hour_start_ms"]): "events=%d pass=%d strong=%d"
                    % (item["events"], item["pass"], item["strong"])
                    for item in report["hourly"]
                },
            )
        )
    return "\n".join(lines) + "\n"


def _gate_rows(gates: List[Dict[str, Any]]) -> List[str]:
    if not gates:
        return ["  (no blocking reason recorded)"]
    width = max(len(str(gate["reason"])) for gate in gates)
    return [
        "  %-*s  blocked=%7d  share=%-8s  sole=%d"
        % (width, gate["reason"], gate["count"], gate["share"], gate["sole_blocker"])
        for gate in gates
    ]
