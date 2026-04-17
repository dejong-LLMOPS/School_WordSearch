"""
List US states that have zero aggregate keyword hits in the results CSV, and optionally
re-run the scraper for each state via main.py.

Default is dry-run (print table + suggested commands). Use --run to execute main.py
for each listed state sequentially.
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from utils.logging_config import setup_logging

from dashboard_data import (
    STATE_NAMES,
    build_district_records,
    load_all_states_data,
    states_with_zero_keyword_hits,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="States with zero keyword hits in results CSV; optional re-scrape via main.py"
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Path to results CSV (default: same as dashboard / load_all_states_data)",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Actually run: python main.py --state <CODE> for each state (sequential)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most this many states (after sorting by code)",
    )
    parser.add_argument(
        "--pass-to-main",
        type=str,
        default="",
        help='Extra arguments for main.py as one shell string, e.g. \'--workers 8 --html\'',
    )
    args = parser.parse_args()
    main_extra = shlex.split(args.pass_to_main.strip()) if args.pass_to_main.strip() else []

    setup_logging()

    rows = load_all_states_data(args.csv) if args.csv else load_all_states_data()
    if not rows:
        print("No CSV rows loaded; check path and OUTPUT_DIR / RESULTS_CSV.", file=sys.stderr)
        return 1

    records = build_district_records(rows)
    zero_states = states_with_zero_keyword_hits(records)
    if args.limit is not None:
        zero_states = zero_states[: args.limit]

    if not zero_states:
        print("No states with zero aggregate keyword hits (all states with data have at least one hit).")
        return 0

    print(f"States with totalKeywordHits == 0 ({len(zero_states)}):\n")
    print(f"{'Code':<6} {'State':<24} {'Districts':>10} {'Scrape success':>14}")
    print("-" * 58)
    for row in zero_states:
        print(
            f"{row['state']:<6} {row['stateName']:<24} "
            f"{row['totalDistricts']:>10,} {row['districtsWithSuccess']:>14,}"
        )

    main_py = _project_root / "main.py"
    if not main_py.is_file():
        print(f"\nmain.py not found at {main_py}", file=sys.stderr)
        return 1

    print("\nSuggested commands (dry-run; add --run to execute):\n")
    for row in zero_states:
        code = row["state"]
        extra = " ".join(main_extra) if main_extra else ""
        line = f'{sys.executable} "{main_py}" --state {code}'
        if extra:
            line += f" {extra}"
        print(line)

    if not args.run:
        print("\n(Dry-run only. Pass --run to execute the lines above.)")
        return 0

    for i, row in enumerate(zero_states):
        code = row["state"]
        name = row.get("stateName") or STATE_NAMES.get(code, code)
        cmd = [sys.executable, str(main_py), "--state", code, *main_extra]
        print(f"\n[{i + 1}/{len(zero_states)}] Running {name} ({code})...")
        r = subprocess.run(cmd, cwd=str(_project_root))
        if r.returncode != 0:
            print(f"Stopped: main.py exited with {r.returncode} for {code}.", file=sys.stderr)
            return r.returncode

    print(f"\nFinished {len(zero_states)} state run(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
