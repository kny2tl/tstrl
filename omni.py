#!/usr/bin/env python3
"""
omni.py

Fetch milestones, build omni-like JSON and HTML, and generate velocity chart using velocity_chart.py.
Chart generation logic was moved to velocity_chart.generate_velocity_chart.
"""

from __future__ import annotations

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

# local modules
from table_milestones import fetch_milestones_map, build_rows_from_map, build_milestones_table

# import chart generator (optional)
try:
    from velocity_chart import generate_velocity_chart
except Exception:
    generate_velocity_chart = None  # chart generation unavailable

def write_omni_json(path: Path, generated_for_date: str, rows: List[Dict[str, str]], milestone_map: Dict[str, Dict[str, str]]) -> bool:
    payload = {
        "generated_for_date": generated_for_date,
        "rows": rows,
        "milestone_map": milestone_map
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✅ Milestone JSON written to: {path}")
        return True
    except Exception as e:
        print(f"❌ Failed to write JSON {path}: {e}", file=sys.stderr)
        return False

def write_chart_png(rows: List[Dict[str, str]], out_path: Path, months: int = 6) -> bool:
    if generate_velocity_chart is None:
        print("⚠️ velocity_chart.generate_velocity_chart not available — skipping chart generation", file=sys.stderr)
        return False
    try:
        png_bytes = generate_velocity_chart(rows, months=months)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(png_bytes)
        print(f"✅ Velocity chart written to: {out_path}")
        return True
    except Exception as e:
        print(f"❌ Failed to generate or write chart: {e}", file=sys.stderr)
        return False

def build_console_preview(rows: List[Dict[str, str]]) -> None:
    if not rows:
        print("⚠️ No milestones could be retrieved.", file=sys.stderr)
        return
    print(f"{'Name':<30} {'Status':<12} {'Start':<12} {'Due':<12}")
    print("-" * 70)
    for r in rows:
        print(f"{r['name'][:30]:<30} {r['status']:<12} {r['start']:<12} {r['due']:<12}")

def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate omni JSON, HTML and velocity chart")
    parser.add_argument("--json-out", help="Write milestone data to JSON file", default="milestone_data.json")
    parser.add_argument("--chart-out", help="Write velocity chart PNG", default="velocity.png")
    parser.add_argument("--months", type=int, help="Months for velocity chart", default=6)
    args = parser.parse_args(argv[1:] if argv else None)

    out_json = Path(args.json_out)
    out_chart = Path(args.chart_out)

    milestone_map = fetch_milestones_map()
    rows = build_rows_from_map(milestone_map)
    generated_for_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    build_console_preview(rows)
    ok = write_omni_json(out_json, generated_for_date, rows, milestone_map)
    if not ok:
        return 3

    # write chart using external module if available (rows are appropriate input)
    write_chart_png(rows, out_chart, months=args.months)

    # optionally produce HTML snippet using build_milestones_table
    html_table = build_milestones_table(milestone_map)
    print("\nHTML table preview (first 500 chars):")
    print(html_table[:500])

    print(f"\nℹ️ Generated {len(rows)} milestone rows; date: {generated_for_date}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())