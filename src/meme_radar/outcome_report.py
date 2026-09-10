"""Read-only post-hoc outcome distribution.

Reports what happened to each cohort stratum at each horizon, per ruleset
version. Nothing here writes to the database.

The report is deliberately loud about what it cannot tell you. Three
annotations are attached to every run because omitting them is how a post-hoc
table becomes a wrong decision:

``SURVIVORSHIP``
    The ``delivered`` stratum was selected by the very gates being evaluated.
    Its return distribution is conditional on passing them and cannot be read
    as the return of the strategy.
``CONTROL_IS_SAMPLED``
    ``not_evaluated`` is a hash-sampled fraction of a population dominated by
    enrichment-budget expiry. It measures the size of the budget shortfall. It
    is not a matched control group and a level comparison against it is invalid.
``SAMPLE_TOO_SMALL``
    Attached per row whenever the number of computable returns is below
    ``min_samples``. A median over nine tokens is not evidence.

``coverage`` is reported separately from the distribution. A horizon whose
measurements are mostly ``unavailable`` or ``expired`` has a distribution drawn
from a biased remainder — the tokens that still had a priced pair — and the
coverage number is what makes that visible.
"""

from __future__ import annotations

import sqlite3
from statistics import median
from typing import Any, Dict, List, Optional, Sequence

from .funnel_report import open_readonly  # noqa: F401  (re-exported for tools)
from .outcome_cohort import COHORT_VERSION, HORIZON_MINUTES, OUTCOME_LABELS, STRATA


OUTCOME_REPORT_VERSION = "outcome_report_v1"

DEFAULT_MIN_SAMPLES = 30

ANNOTATIONS = {
    "SURVIVORSHIP": (
        "delivered 层由被评估的门本身筛出，其收益分布以通过这些门为条件，"
        "不能读作策略收益。"
    ),
    "CONTROL_IS_SAMPLED": (
        "not_evaluated 层是按身份哈希抽样的，母体由补全预算过期主导。"
        "它度量预算缺口规模，不是匹配对照组，与其做水平比较无效。"
    ),
    "SAMPLE_TOO_SMALL": (
        "该行可计算收益样本数低于 min_samples，分位数不构成证据。"
    ),
    "COVERAGE_LOW": (
        "该行超过一半的度量是 unavailable/expired，剩余样本偏向"
        "仍有报价交易对的代币。"
    ),
}


def _quantile(values: Sequence[float], fraction: float) -> Optional[float]:
    """Nearest-rank quantile. No interpolation, no scipy."""
    if not values:
        return None
    ordered = sorted(values)
    index = int(round(fraction * (len(ordered) - 1)))
    return ordered[max(0, min(index, len(ordered) - 1))]


def _distribution(returns: List[float], *, min_samples: int) -> Dict[str, Any]:
    count = len(returns)
    result: Dict[str, Any] = {
        "computable": count,
        "median_return": None,
        "p25_return": None,
        "p75_return": None,
        "max_return": None,
        "share_up": None,
        "share_double": None,
        "share_halved": None,
        "annotations": [],
    }
    if count == 0:
        return result
    result["median_return"] = round(median(returns), 4)
    result["p25_return"] = round(_quantile(returns, 0.25), 4)
    result["p75_return"] = round(_quantile(returns, 0.75), 4)
    result["max_return"] = round(max(returns), 4)
    result["share_up"] = round(
        sum(1 for value in returns if value > 1.0) / count, 4
    )
    result["share_double"] = round(
        sum(1 for value in returns if value >= 2.0) / count, 4
    )
    result["share_halved"] = round(
        sum(1 for value in returns if value <= 0.5) / count, 4
    )
    if count < min_samples:
        result["annotations"].append("SAMPLE_TOO_SMALL")
    return result


def collect_cohort(
    connection: sqlite3.Connection,
    *,
    cohort_version: str,
    since_ms: int,
    until_ms: int,
) -> Dict[str, Any]:
    rows = connection.execute(
        """
        SELECT ruleset_version, stratum, chain, source, baseline_status,
               COUNT(*) AS members,
               MIN(baseline_at_ms) AS first_ms,
               MAX(baseline_at_ms) AS last_ms,
               SUM(baseline_at_ms - decision_evaluated_at_ms) AS lag_sum
        FROM outcome_cohort
        WHERE cohort_version = ?
          AND baseline_at_ms >= ? AND baseline_at_ms < ?
        GROUP BY ruleset_version, stratum, chain, source, baseline_status
        """,
        (cohort_version, since_ms, until_ms),
    ).fetchall()
    by_stratum = {stratum: 0 for stratum in STRATA}
    by_baseline: Dict[str, int] = {}
    by_scope: Dict[str, int] = {}
    total = 0
    lag_sum = 0
    first_ms: Optional[int] = None
    last_ms: Optional[int] = None
    for row in rows:
        count = int(row["members"])
        total += count
        lag_sum += int(row["lag_sum"] or 0)
        by_stratum[str(row["stratum"])] = (
            by_stratum.get(str(row["stratum"]), 0) + count
        )
        key = str(row["baseline_status"])
        by_baseline[key] = by_baseline.get(key, 0) + count
        scope = "%s|%s" % (row["source"], row["chain"])
        by_scope[scope] = by_scope.get(scope, 0) + count
        if first_ms is None or int(row["first_ms"]) < first_ms:
            first_ms = int(row["first_ms"])
        if last_ms is None or int(row["last_ms"]) > last_ms:
            last_ms = int(row["last_ms"])
    return {
        "members": total,
        "by_stratum": by_stratum,
        "by_baseline_status": dict(sorted(by_baseline.items())),
        "by_source_chain": dict(sorted(by_scope.items())),
        "first_baseline_at_ms": first_ms,
        "last_baseline_at_ms": last_ms,
        "span_hours": (
            None
            if first_ms is None or last_ms is None
            else round((last_ms - first_ms) / 3_600_000, 2)
        ),
        "mean_enrollment_lag_ms": None if total == 0 else int(lag_sum / total),
    }


def _horizon_rows(
    connection: sqlite3.Connection,
    *,
    cohort_version: str,
    since_ms: int,
    until_ms: int,
    ruleset: Optional[str],
) -> List[sqlite3.Row]:
    clause = ""
    params: List[Any] = [cohort_version, since_ms, until_ms]
    if ruleset:
        clause = " AND c.ruleset_version = ?"
        params.append(ruleset)
    return connection.execute(
        """
        SELECT c.ruleset_version AS ruleset_version,
               c.stratum AS stratum,
               o.horizon_minutes AS horizon_minutes,
               o.outcome_label AS outcome_label,
               o.price_usd AS price_usd,
               c.baseline_price_usd AS baseline_price_usd,
               c.baseline_status AS baseline_status
        FROM outcome_cohort c
        JOIN outcomes o
          ON o.event_id = c.event_id
         AND o.ruleset_version = c.ruleset_version
        WHERE c.cohort_version = ?
          AND c.baseline_at_ms >= ? AND c.baseline_at_ms < ?%s
        """
        % clause,
        params,
    ).fetchall()


def _bucket_measurements(
    rows: Sequence[sqlite3.Row],
    horizons: Sequence[int],
) -> Dict[Any, Dict[str, Any]]:
    """Group measurements by ruleset, stratum and horizon.

    A measured point with no usable baseline is counted in
    ``baseline_missing`` rather than contributing a return, so it can never be
    mistaken for a flat one.
    """
    buckets: Dict[Any, Dict[str, Any]] = {}
    for row in rows:
        horizon = int(row["horizon_minutes"])
        if horizon not in horizons:
            continue
        key = (str(row["ruleset_version"]), str(row["stratum"]), horizon)
        bucket = buckets.setdefault(
            key,
            {
                "measured": 0,
                "labels": {label: 0 for label in OUTCOME_LABELS},
                "baseline_missing": 0,
                "returns": [],
            },
        )
        bucket["measured"] += 1
        bucket["labels"][str(row["outcome_label"])] += 1
        if str(row["outcome_label"]) != "ok":
            continue
        baseline = row["baseline_price_usd"]
        price = row["price_usd"]
        if (
            str(row["baseline_status"]) != "ok"
            or baseline is None
            or price is None
            or float(baseline) <= 0.0
        ):
            bucket["baseline_missing"] += 1
            continue
        bucket["returns"].append(float(price) / float(baseline))
    return buckets


def _annotate(row_out: Dict[str, Any], bucket: Dict[str, Any]) -> None:
    """Attach the warnings that make a row safe to read."""
    usable = bucket["labels"]["ok"]
    measured = bucket["measured"]
    coverage = 0.0 if measured == 0 else round(usable / measured, 4)
    row_out["coverage"] = coverage
    notes = list(row_out["annotations"])
    if coverage < 0.5:
        notes.append("COVERAGE_LOW")
    if row_out["stratum"] == "delivered":
        notes.append("SURVIVORSHIP")
    if row_out["stratum"] == "not_evaluated":
        notes.append("CONTROL_IS_SAMPLED")
    row_out["annotations"] = notes


def collect_horizons(
    connection: sqlite3.Connection,
    *,
    cohort_version: str,
    since_ms: int,
    until_ms: int,
    horizons: Sequence[int],
    min_samples: int,
    ruleset: Optional[str],
) -> List[Dict[str, Any]]:
    buckets = _bucket_measurements(
        _horizon_rows(
            connection,
            cohort_version=cohort_version,
            since_ms=since_ms,
            until_ms=until_ms,
            ruleset=ruleset,
        ),
        horizons,
    )
    result: List[Dict[str, Any]] = []
    for key in sorted(buckets):
        ruleset_version, stratum, horizon = key
        bucket = buckets[key]
        row_out: Dict[str, Any] = {
            "ruleset_version": ruleset_version,
            "stratum": stratum,
            "horizon_minutes": horizon,
            "measured": bucket["measured"],
            "labels": dict(sorted(bucket["labels"].items())),
            "baseline_missing": bucket["baseline_missing"],
        }
        row_out.update(
            _distribution(bucket["returns"], min_samples=min_samples)
        )
        _annotate(row_out, bucket)
        result.append(row_out)
    return result


def collect_gaps(
    connection: sqlite3.Connection,
    *,
    cohort_version: str,
    since_ms: int,
    until_ms: int,
    horizons: Sequence[int],
) -> Dict[str, Any]:
    """Measurement completeness, so an incomplete cohort cannot look finished."""
    members = connection.execute(
        """
        SELECT COUNT(*) AS members FROM outcome_cohort
        WHERE cohort_version = ?
          AND baseline_at_ms >= ? AND baseline_at_ms < ?
        """,
        (cohort_version, since_ms, until_ms),
    ).fetchone()
    complete = connection.execute(
        """
        SELECT COUNT(*) AS complete FROM (
            SELECT c.event_id
            FROM outcome_cohort c
            JOIN outcomes o
              ON o.event_id = c.event_id
             AND o.ruleset_version = c.ruleset_version
            WHERE c.cohort_version = ?
              AND c.baseline_at_ms >= ? AND c.baseline_at_ms < ?
              AND o.outcome_label = 'ok'
            GROUP BY c.event_id, c.ruleset_version
            HAVING COUNT(DISTINCT o.horizon_minutes) >= ?
        )
        """,
        (cohort_version, since_ms, until_ms, len(horizons)),
    ).fetchone()
    total = int(members["members"])
    full = int(complete["complete"])
    return {
        "cohort_members": total,
        "gap_free_members": full,
        "gap_free_share": round(full / total, 4) if total else 0.0,
        "required_horizons": list(horizons),
    }


def build_report(
    connection: sqlite3.Connection,
    *,
    since_ms: int,
    until_ms: int,
    cohort_version: str = COHORT_VERSION,
    ruleset: Optional[str] = None,
    horizons: Sequence[int] = HORIZON_MINUTES,
    min_samples: int = DEFAULT_MIN_SAMPLES,
) -> Dict[str, Any]:
    if until_ms <= since_ms:
        raise ValueError("outcome window must be positive")
    if min_samples < 1:
        raise ValueError("min_samples must be positive")
    horizons = tuple(int(item) for item in horizons)
    report = {
        "version": OUTCOME_REPORT_VERSION,
        "cohort_version": cohort_version,
        "window_hours": round((until_ms - since_ms) / 3_600_000, 3),
        "filters": {
            "since_ms": since_ms,
            "until_ms": until_ms,
            "ruleset_version": ruleset,
            "min_samples": min_samples,
        },
        "cohort": collect_cohort(
            connection,
            cohort_version=cohort_version,
            since_ms=since_ms,
            until_ms=until_ms,
        ),
        "horizons": collect_horizons(
            connection,
            cohort_version=cohort_version,
            since_ms=since_ms,
            until_ms=until_ms,
            horizons=horizons,
            min_samples=min_samples,
            ruleset=ruleset,
        ),
        "completeness": collect_gaps(
            connection,
            cohort_version=cohort_version,
            since_ms=since_ms,
            until_ms=until_ms,
            horizons=horizons,
        ),
    }
    used = {"SURVIVORSHIP", "CONTROL_IS_SAMPLED"}
    for row in report["horizons"]:
        used.update(row["annotations"])
    report["annotations"] = {
        name: ANNOTATIONS[name] for name in sorted(used) if name in ANNOTATIONS
    }
    return report


def _render_cohort(report: Dict[str, Any]) -> List[str]:
    cohort = report["cohort"]
    completeness = report["completeness"]
    lines = [
        "Meme Radar outcome report (%s / %s)"
        % (report["version"], report["cohort_version"]),
        "window: %s hours  members=%d  span=%s hours  mean enrollment lag=%s ms"
        % (
            report["window_hours"],
            cohort["members"],
            cohort["span_hours"],
            cohort["mean_enrollment_lag_ms"],
        ),
        "",
        "cohort by stratum",
    ]
    for stratum in STRATA:
        lines.append("  %-20s %d" % (stratum, cohort["by_stratum"].get(stratum, 0)))
    lines.append("")
    lines.append("baseline status")
    for status, count in cohort["by_baseline_status"].items():
        lines.append("  %-20s %d" % (status, count))
    lines.append("")
    lines.append(
        "completeness: %d/%d members have every horizon measured (%.4f)"
        % (
            completeness["gap_free_members"],
            completeness["cohort_members"],
            completeness["gap_free_share"],
        )
    )
    return lines


def _render_distribution(report: Dict[str, Any]) -> List[str]:
    lines = ["return distribution by stratum and horizon"]
    header = (
        "  %-24s %-19s %6s %8s %8s %8s %8s %8s %8s"
        % (
            "ruleset",
            "stratum",
            "h(min)",
            "n",
            "median",
            "p25",
            "p75",
            "up",
            "cover",
        )
    )
    lines.append(header)
    for row in report["horizons"]:
        lines.append(
            "  %-24s %-19s %6d %8d %8s %8s %8s %8s %8s"
            % (
                row["ruleset_version"][:24],
                row["stratum"],
                row["horizon_minutes"],
                row["computable"],
                "-" if row["median_return"] is None else row["median_return"],
                "-" if row["p25_return"] is None else row["p25_return"],
                "-" if row["p75_return"] is None else row["p75_return"],
                "-" if row["share_up"] is None else row["share_up"],
                row["coverage"],
            )
        )
        if row["annotations"]:
            lines.append("      ! " + " ".join(sorted(set(row["annotations"]))))
    lines.append("")
    lines.append("annotations")
    for name, note in report["annotations"].items():
        lines.append("  %s: %s" % (name, note))
    return lines


def render_text(report: Dict[str, Any]) -> str:
    lines = _render_cohort(report)
    lines.append("")
    lines.extend(_render_distribution(report))
    return "\n".join(lines) + "\n"
