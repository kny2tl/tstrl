#!/usr/bin/env python3
"""
output.py — orchestrates table_milestones fetch + omni.py then builds final HTML report

Behavior:
- fetch milestones via table_milestones.fetch_milestones_map() and write milestone_data.json
- run omni.py to produce results.json and delta_*.png
- render HTML report into output/report_<timestamp>.html

Styling:
- table header background: #696969
- Completed rows background: #cccccc
- In-progress status cell text stays green
"""

from __future__ import annotations

import sys
import json
import base64
import subprocess
from datetime import datetime, UTC
from pathlib import Path
import argparse
import html as html_mod

from table_testruns import build_testruns_table
from table_milestones import fetch_milestones_map, build_milestones_table, build_rows_from_map

# Try to import velocity_chart (chart generator moved here); if unavailable continue silently
try:
    from velocity_chart import generate_velocity_chart
except Exception:
    generate_velocity_chart = None

# Defaults
REPO_DIR = Path(__file__).resolve().parent
DEFAULT_MILESTONE_JSON = REPO_DIR / "milestone_data.json"
DEFAULT_RESULTS_JSON = REPO_DIR / "results.json"
DEFAULT_OUTPUT_DIR = REPO_DIR / "output"


def run_subprocess(cmd, desc, cwd=None):
    print(f"▶️ {desc}: {' '.join(map(str, cmd))}")
    try:
        subprocess.run(cmd, check=True, cwd=cwd)
        print(f"✅ {desc} finished")
    except subprocess.CalledProcessError as e:
        print(f"❌ {desc} failed: {e}", file=sys.stderr)
        sys.exit(1)


def write_json(path: Path, obj):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"✅ Wrote JSON: {path}")
        return True
    except Exception as e:
        print(f"❌ Failed to write JSON {path}: {e}", file=sys.stderr)
        return False


def load_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to load {path}: {e}", file=sys.stderr)
        return {}


def find_latest_chart_bytes(output_dir: Path):
    try:
        files = sorted([p for p in output_dir.glob("delta_*.png")])
        if not files:
            raise FileNotFoundError("No delta_*.png found in output/")
        latest = files[-1]
        return latest.read_bytes(), latest.name
    except Exception as e:
        print(f"⚠️ Chart PNG not found or failed to load: {e}", file=sys.stderr)
        return None, None


def build_html(title, generated_for_date, chart_data_uri, milestone_html, testruns_html, milestone_intro_text=""):
    css = """
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial; margin:28px; color:#222; background:#fff; }
    header { margin-bottom:18px; }
    h1 { font-size:20px; margin:0 0 6px 0; }
    .meta { color:#555; margin-bottom:12px; }

    /* milestone intro */
    .milestone-intro { background:#f6f7f8; border:1px dashed #ddd; padding:10px 12px; margin-bottom:10px; color:#555; }

    /* table base */
    .milestone-table, .testruns-table {
      border-collapse: collapse;
      margin-bottom:16px;
      box-shadow: 0 1px 0 rgba(0,0,0,0.03);
      width:100%;
    }
    .milestone-table th, .testruns-table th {
      background: #696969;
      color: #ffffff;
      font-weight: 700;
      padding: 8px 10px;
      border: 1px solid #ccc;
      text-align:left;
    }

    /* Cells transparent so any tr background (if used) shows through */
    .milestone-table td, .testruns-table td {
      background: transparent;
      color: #111;
      padding: 8px 10px;
      border: 1px solid #ccc;
      vertical-align: middle;
    }

    /* Row backgrounds (applied to tr via class) - kept as examples but cells remain transparent */
    tr.milestone-completed { background-color: #D3D3D3; }
    tr.milestone-inprogress { background-color: transparent; } /* user requested transparent bg for in-progress rows */

    /* If you still want a subtle tint for in-progress rows while keeping cells visually transparent,
       uncomment the following line and remove the 'transparent' above:
    tr.milestone-inprogress { background-color: #cccccc; }
    */

    /* Ensure other rules don't override the status cell color: use specific selectors */
    table.milestone-table td.milestone-status-inprogress,
    table.testruns-table td.milestone-status-inprogress {
      color: #2e7d32;
      font-weight: 700;
      background: transparent;
    }

    /* If some external CSS still overrides color, this more specific selector will win:
       table.milestone-table tr.milestone-inprogress > td.milestone-status-inprogress { ... }
       Use !important only as last resort. */

    /* chart */
    .chart { text-align:left; margin-top:8px; }
    img.chart-img { max-width:100%; height:auto; border:1px solid #eee; box-shadow:0 1px 2px rgba(0,0,0,0.04); display:block; margin:0; }

    .placeholder { background:#f6f7f8; border:1px dashed #ddd; padding:12px; margin-bottom:12px; color:#666; }
    .extra { margin-top:12px; margin-bottom:12px; color:#333; }
    footer { margin-top:18px; color:#666; font-size:13px; }
    .small { font-size:13px; color:#666; }
    """
    intro_block = f"<div class='milestone-intro'>{html_mod.escape(milestone_intro_text)}</div>" if milestone_intro_text else ""
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html_mod.escape(title)}</title>
<style>{css}</style>
</head>
<body>
<header>
  <h1>{html_mod.escape(title)}</h1>
  <div class="meta">Report generated for date: <strong>{html_mod.escape(generated_for_date)}</strong></div>
</header>

<section class="milestone-section">
  {intro_block}
  {milestone_html}
</section>

<section class="placeholder">
  <div><strong>Testing activities</strong></div>
</section>

<section class="table-placeholder" id="table-placeholder">
  {testruns_html}
</section>

<section class="extra">
  <div>Below is the historical chart of executed tests per day (daily unique executed results and cumulative total).</div>
</section>

<section class="chart">
  <img class="chart-img" alt="Historical executed results chart" src="{chart_data_uri}" />
</section>

<footer>
  <div class="small">Generated: {datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")}</div>
</footer>
</body>
</html>"""
    return html_doc


def resolve_script_path(default: Path, override: str | None):
    if override:
        p = Path(override).expanduser()
        if not p.exists():
            print(f"❌ Script not found: {p}", file=sys.stderr)
            sys.exit(1)
        return p
    if not default.exists():
        print(f"❌ Script not found: {default}. Pass an explicit path via CLI.", file=sys.stderr)
        sys.exit(1)
    return default


def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = argparse.ArgumentParser(description="Generate TestRail HTML report (milestones fetched every run)")
    parser.add_argument("--omni-script", help="Path to omni.py", default=str(REPO_DIR / "omni.py"))
    parser.add_argument("--results-json", help="Path to results JSON", default=str(DEFAULT_RESULTS_JSON))
    parser.add_argument("--milestones-json", help="Path to milestones JSON to write/use", default=str(DEFAULT_MILESTONE_JSON))
    parser.add_argument("--output-dir", help="Directory for HTML and PNG outputs", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--milestone-intro", help="Text shown above milestones table", default="")
    args = parser.parse_args(argv)

    repo_dir = REPO_DIR
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    omni_py = resolve_script_path(repo_dir / "omni.py", args.omni_script)
    results_json = Path(args.results_json)
    milestones_json = Path(args.milestones_json)

    # 0) Fetch milestones (always) and write omni-like JSON used by output
    print("▶️ Fetching milestones via table_milestones...")
    milestone_map = fetch_milestones_map()
    # Build rows and payload same style as omni.py
    rows = build_rows_from_map(milestone_map)
    generated_for_date = datetime.now().strftime("%Y-%m-%d")
    payload = {
        "generated_for_date": generated_for_date,
        "rows": rows,
        "milestone_map": milestone_map
    }
    # Write milestone JSON for output consumption and debugging
    if not write_json(milestones_json, payload):
        print("⚠️ Continuing despite milestone JSON write failure", file=sys.stderr)

    # Log loaded IDs
    ids = list(milestone_map.keys())
    print(f"ℹ️ Milestones fetched: {len(ids)} — IDs: {', '.join(ids) if ids else 'none'}")

    # 1) Run omni.py to produce results.json and chart PNG(s) into output/
    run_subprocess(
        [sys.executable, str(omni_py), "--json-out", str(results_json)],
        "Generate results JSON and chart PNG",
        cwd=repo_dir
    )

    # 2) Load runs
    omni = load_json(results_json)
    run_rows = omni.get("rows", [])
    grand = omni.get("grand", {})
    generated_for_date_runs = omni.get("generated_for_date", generated_for_date)

    # 3) Chart to data URI
    chart_bytes, chart_name = find_latest_chart_bytes(output_dir)
    chart_data_uri = ""
    if chart_bytes:
        chart_data_uri = "data:image/png;base64," + base64.b64encode(chart_bytes).decode("ascii")
        print(f"📈 Using chart: {chart_name}")

    # 4) Build tables and HTML
    milestone_html = build_milestones_table(milestone_map)
    testruns_html = build_testruns_table(run_rows, grand)
    html_doc = build_html(
        title="TestRail Report",
        generated_for_date=generated_for_date_runs,
        chart_data_uri=chart_data_uri,
        milestone_html=milestone_html,
        testruns_html=testruns_html,
        milestone_intro_text=args.milestone_intro
    )

    # 5) Save HTML
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"report_{stamp}.html"
    try:
        out_path.write_text(html_doc, encoding="utf-8")
        print(f"✅ HTML report written to: {out_path}")
    except Exception as e:
        print(f"❌ Failed to write HTML report: {e}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())