#!/usr/bin/env python3
"""Read-only funnel and gate-selectivity report.

    PYTHONPATH=src python3 tools/funnel_report.py --db-path /var/lib/meme-radar/radar.db --hours 24

The database is opened with ``mode=ro`` and ``PRAGMA query_only``; the tool
never writes, so it is safe to run against the live production file.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from meme_radar.funnel_report import build_report, open_readonly, render_text


def arguments():
    parser = argparse.ArgumentParser(
        description="Read-only Meme Radar funnel and gate selectivity report",
    )
    parser.add_argument("--db-path", type=Path, required=True)
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument(
        "--since-ms",
        type=int,
        default=0,
        help="explicit window start; overrides --hours",
    )
    parser.add_argument(
        "--until-ms",
        type=int,
        default=0,
        help="explicit window end; defaults to now",
    )
    parser.add_argument("--chain", default=None)
    parser.add_argument("--source", default=None)
    parser.add_argument("--ruleset", default=None)
    parser.add_argument("--top", type=int, default=25)
    parser.add_argument("--hourly", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = arguments()
    if not 0 < args.hours <= 24 * 90:
        raise ValueError("hours outside 0..2160")
    if args.top < 0 or args.top > 500:
        raise ValueError("top outside 0..500")
    until_ms = args.until_ms or int(time.time() * 1000)
    since_ms = args.since_ms or until_ms - int(args.hours * 3_600_000)
    connection = open_readonly(args.db_path)
    try:
        report = build_report(
            connection,
            since_ms=since_ms,
            until_ms=until_ms,
            chain=args.chain,
            source=args.source,
            ruleset=args.ruleset,
            top=args.top,
            hourly=args.hourly,
        )
    finally:
        connection.close()
    if args.json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        sys.stdout.write(render_text(report))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        print(
            json.dumps(
                {"event": "FUNNEL_REPORT_FATAL", "error_type": type(exc).__name__,
                 "error": str(exc)[:200]},
                separators=(",", ":"),
            ),
            flush=True,
        )
        sys.exit(1)
