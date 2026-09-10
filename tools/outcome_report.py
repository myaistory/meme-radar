#!/usr/bin/env python3
"""Read-only post-hoc outcome report CLI.

Opens the radar database read-only and prints the outcome distribution per
ruleset version and stratum. Writes nothing.

    PYTHONPATH=src python3 tools/outcome_report.py \
        --db-path /var/lib/meme-radar/radar.db --hours 72
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Sequence

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from meme_radar.funnel_report import open_readonly
from meme_radar.outcome_cohort import COHORT_VERSION, HORIZON_MINUTES
from meme_radar.outcome_report import (
    DEFAULT_MIN_SAMPLES,
    build_report,
    render_text,
)


MAX_HOURS = 2160


def arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Meme Radar outcome report")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--hours", type=float, default=72.0)
    parser.add_argument("--since-ms", type=int, default=0)
    parser.add_argument("--until-ms", type=int, default=0)
    parser.add_argument("--cohort-version", default=COHORT_VERSION)
    parser.add_argument("--ruleset", default="")
    parser.add_argument("--min-samples", type=int, default=DEFAULT_MIN_SAMPLES)
    parser.add_argument("--horizons", default="")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def _window(namespace: argparse.Namespace) -> Dict[str, int]:
    if namespace.until_ms:
        until_ms = int(namespace.until_ms)
    else:
        until_ms = int(time.time() * 1000)
    if namespace.since_ms:
        since_ms = int(namespace.since_ms)
    else:
        if not 0 < namespace.hours <= MAX_HOURS:
            raise ValueError("hours must be in (0, %d]" % MAX_HOURS)
        since_ms = until_ms - int(namespace.hours * 3_600_000)
    return {"since_ms": since_ms, "until_ms": until_ms}


def _horizons(raw: str) -> Sequence[int]:
    if not raw.strip():
        return HORIZON_MINUTES
    values: List[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        value = int(chunk)
        if value <= 0:
            raise ValueError("horizon minutes must be positive")
        values.append(value)
    if not values:
        raise ValueError("no horizons selected")
    return tuple(values)


def main(argv: Optional[Sequence[str]] = None) -> int:
    namespace = arguments(argv)
    try:
        window = _window(namespace)
        horizons = _horizons(namespace.horizons)
        if namespace.min_samples < 1:
            raise ValueError("min-samples must be positive")
        connection = open_readonly(namespace.db_path)
    except Exception as error:  # noqa: BLE001 - CLI boundary
        payload: Dict[str, Any] = {
            "event": "OUTCOME_REPORT_FATAL",
            "error_type": type(error).__name__,
            "error": str(error),
        }
        sys.stderr.write(json.dumps(payload, sort_keys=True) + "\n")
        return 2
    try:
        report = build_report(
            connection,
            since_ms=window["since_ms"],
            until_ms=window["until_ms"],
            cohort_version=namespace.cohort_version,
            ruleset=namespace.ruleset.strip() or None,
            horizons=horizons,
            min_samples=namespace.min_samples,
        )
    except Exception as error:  # noqa: BLE001 - CLI boundary
        payload = {
            "event": "OUTCOME_REPORT_FATAL",
            "error_type": type(error).__name__,
            "error": str(error),
        }
        sys.stderr.write(json.dumps(payload, sort_keys=True) + "\n")
        return 2
    finally:
        connection.close()
    if namespace.json:
        sys.stdout.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
    else:
        sys.stdout.write(render_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
