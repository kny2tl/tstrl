#!/usr/bin/env python3
"""
output.py

Assemble final HTML report from omni-produced JSONs, milestones, and per-plan velocity charts.

Changes for "no global chart":
- The script no longer expects or embeds a global velocity chart.
- It includes per-plan charts only (results_plan_*.json).
- Still produces milestone table and runs summary from results.json.
- Copies per-plan PNGs into the report output dir and embeds them.
- Output filename: report_<YYYYMMDD>_<HHMMSS>.html (UTC)
- CLI: --omni-script, --results-json, --output-dir, --skip-omni, --verbose, --milestone-json
"""
from __future__ import annotations

import sys
import subprocess
from pathlib import Path
from typing import Optional, List, Tuple
import argparse
import logging
import json
import os
from html import escape
from datetime import datetime, timezone
import shutil

# optional plan config
try:
    from config import CHART_PLAN_SETS  # type: ignore
except Exception:
    CHART_PLAN_SETS = None
try:
    from config import CHART_PLAN_IDS  # type: ignore
except Exception:
    CHART_PLAN_IDS = None

logger = logging.getLogger("output")
_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.propagate = False


def _run_omni_subprocess(omni_script: Path, extra_args: Optional[list] = None) -> int:
    cmd = [sys.executable, str(omni_script)]
    if extra_args:
        cmd += extra_args
    repo = str(Path(__file__).resolve().parent)
    env = os.environ.copy()
    env["PYTHONPATH"] = repo + (os.pathsep + env.get("PYTHONPATH", ""))
    logger.info("Running omni subprocess for output: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("omni subprocess failed: %s", e)
        return e.returncode


def _generate_charts_for_jsons(results_dir: Path, json_files: List[str], verbose: bool = False) -> None:
    """
    Generate charts by invoking chart_generator.py for each per-plan JSON.
    Skip if chart_generator.py is not present.
    """
    chart_script = Path(__file__).resolve().parent / "chart_generator.py"
    if not chart_script.exists():
        logger.warning("chart_generator not available; skipping chart generation")
        return

    repo = str(Path(__file__).resolve().parent)
    env = os.environ.copy()
    env["PYTHONPATH"] = repo + (os.pathsep + env.get("PYTHONPATH", ""))

    for jf in json_files:
        jpath = results_dir / jf
        if not jpath.exists():
            logger.warning("JSON not found for chart generation: %s", jpath)
            continue
        plan_id = None
        name = jpath.name
        if name.startswith("results_plan_") and name.endswith(".json"):
            try:
                plan_id = int(name[len("results_plan_"):-len(".json")])
            except Exception:
                plan_id = None
        cmd = [sys.executable, str(chart_script), "--from-json", str(jpath)]
        if plan_id is not None:
            cmd += ["--plan-id", str(plan_id)]
        if verbose:
            logger.info("Invoking chart_generator: %s", " ".join(cmd))
        try:
            subprocess.run(cmd, check=True, env=env, cwd=repo)
            logger.info("Generated chart for %s", jpath.name)
        except subprocess.CalledProcessError:
            logger.exception("Chart generation failed for %s", jpath.name)


def _load_json_safe(path: Path) -> Optional[dict]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        logger.debug("Failed to load JSON %s", path)
        return None


def _normalize_plan_ids_from_config() -> List[int]:
    """
    Return ordered list of plan IDs based on CHART_PLAN_SETS or CHART_PLAN_IDS in config.
    """
    plan_ids: List[int] = []
    try:
        if CHART_PLAN_SETS:
            for s in CHART_PLAN_SETS:
                if isinstance(s, (list, tuple)):
                    for p in s:
                        try:
                            plan_ids.append(int(p))
                        except Exception:
                            continue
                else:
                    try:
                        plan_ids.append(int(s))
                    except Exception:
                        continue
        elif CHART_PLAN_IDS:
            for p in CHART_PLAN_IDS:
                try:
                    plan_ids.append(int(p))
                except Exception:
                    continue
    except Exception:
        return []
    out = []
    seen = set()
    for p in plan_ids:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def _collect_per_plan_jsons(results_json: Path) -> List[Path]:
    results_dir = results_json.parent if results_json.parent != Path("") else Path(".")
    # Return per-plan JSONs ordered by config then alphabetically for remaining
    files: List[Path] = []
    plan_ids = _normalize_plan_ids_from_config()
    for pid in plan_ids:
        p = results_dir / f"results_plan_{int(pid)}.json"
        if p.exists():
            files.append(p)
    for p in sorted(results_dir.glob("results_plan_*.json")):
        if p not in files:
            files.append(p)
    return files


def _discover_per_plan_charts(repo_output_dir: Path, run_output_dir: Path) -> List[Path]:
    """
    Discover per-plan PNG charts (delta_<planid>_*.png) from repo/output and results_dir/output.
    Return unique Paths.
    """
    found = []
    for d in (repo_output_dir, run_output_dir):
        if not d or not d.exists():
            continue
        found.extend(sorted(d.glob("delta_*_*.png"), key=lambda p: p.name))
    seen = set()
    unique = []
    for p in found:
        key = str(p.resolve())
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique

def _classify_plan_chart(p: Path) -> Optional[int]:
    """
    If chart filename indicates a plan id (delta_<planid>_YYYYMMDD_... or delta_<planid>_...), return planid.
    Treat an 8-digit numeric second token as a date (global candidate) and NOT as a plan id.
    """
    stem = p.stem
    parts = stem.split("_")
    if len(parts) >= 3 and parts[0] == "delta":
        token = parts[1]
        # token that looks like YYYYMMDD -> treat as date (global), not plan id
        if token.isdigit() and len(token) == 8:
            return None
        # otherwise, if token is numeric (likely plan id), return it
        if token.isdigit():
            try:
                return int(token)
            except Exception:
                return None
    return None

def _order_per_plan_charts(chart_paths: List[Path], results_json: Path) -> List[Path]:
    """
    Order per-plan charts by CHART_PLAN_SETS/CHART_PLAN_IDS, then remaining by plan id order.
    """
    plan_ids = _normalize_plan_ids_from_config()
    by_plan = {}
    for p in chart_paths:
        pid = _classify_plan_chart(p)
        if pid is None:
            # skip non-per-plan charts (global) - we intentionally do not include global charts
            continue
        by_plan.setdefault(int(pid), []).append(p)

    ordered = []
    for pid in plan_ids:
        lst = by_plan.pop(pid, [])
        if lst:
            ordered.append(sorted(lst, key=lambda x: x.name)[-1])  # take latest for plan
    # remaining plans sorted by numeric plan id
    for pid in sorted(by_plan.keys()):
        lst = by_plan[pid]
        ordered.append(sorted(lst, key=lambda x: x.name)[-1])
    return ordered


def _copy_charts_to_report_dir(charts: List[Path], report_dir: Path) -> List[Path]:
    """
    Copy chart files into report_dir (overwriting same-named files), returning list of copied Paths in report_dir.
    """
    report_dir.mkdir(parents=True, exist_ok=True)
    copied = []
    for p in charts:
        dest = report_dir / p.name
        try:
            if p.resolve() == dest.resolve():
                copied.append(dest)
                continue
        except Exception:
            pass
        try:
            shutil.copy2(p, dest)
            copied.append(dest)
        except Exception:
            logger.exception("Failed to copy chart %s to %s", p, dest)
            copied.append(p)
    return copied


def _render_milestone_table_html(milestone_json: Optional[Path]) -> str:
    """
    Robust milestone renderer that handles stringified Python lists and various shapes.
    """
    import ast

    if not milestone_json or not milestone_json.exists():
        return "<div class=\"milestone-intro\">No milestone data available.</div>"
    data = _load_json_safe(milestone_json)
    if not data:
        return "<div class=\"milestone-intro\">No milestone data available.</div>"

    entries = None
    if isinstance(data, list):
        entries = data
    elif isinstance(data, dict):
        if "milestones" in data and isinstance(data["milestones"], (list, str)):
            entries = data["milestones"]
        elif "items" in data and isinstance(data["items"], (list, str)):
            entries = data["items"]
        else:
            for v in data.values():
                if isinstance(v, list):
                    entries = v
                    break
            if entries is None:
                for k, v in data.items():
                    if isinstance(v, str) and v.strip().startswith("["):
                        try:
                            parsed = ast.literal_eval(v)
                            if isinstance(parsed, list):
                                entries = parsed
                                break
                        except Exception:
                            continue

    if isinstance(entries, str):
        txt = entries.strip()
        try:
            parsed = ast.literal_eval(txt)
            if isinstance(parsed, list):
                entries = parsed
            else:
                entries = [str(entries)]
        except Exception:
            entries = [entries]

    if entries is None and isinstance(data, str):
        try:
            parsed = ast.literal_eval(data)
            if isinstance(parsed, list):
                entries = parsed
        except Exception:
            entries = [str(data)]

    if not entries:
        return "<div class=\"milestone-intro\">No milestone entries found.</div>"

    if isinstance(entries, dict):
        entries = [entries]

    flat = []
    for e in entries:
        if isinstance(e, list):
            flat.extend(e)
        else:
            flat.append(e)
    entries = flat

    rows = []
    for e in entries:
        if isinstance(e, str) and e.strip().startswith("{"):
            try:
                parsed = ast.literal_eval(e)
                if isinstance(parsed, dict):
                    e = parsed
            except Exception:
                pass

        if isinstance(e, str):
            rows.append(f"<tr class=''><td>{escape(e)}</td><td class=''></td><td></td><td></td></tr>")
            continue

        if not isinstance(e, dict):
            rows.append(f"<tr class=''><td>{escape(str(e))}</td><td class=''></td><td></td><td></td></tr>")
            continue

        name = escape(str(e.get("name") or e.get("title") or e.get("milestone") or ""))
        status = str(e.get("status") or e.get("state") or e.get("is_completed") or e.get("is_completed_raw") or "")
        if isinstance(status, str) and status.lower() in ("true", "false"):
            status = "Completed" if status.lower() == "true" else "Planned"
        start = escape(str(e.get("start_on") or e.get("start") or e.get("date") or e.get("from") or "TBD"))
        due = escape(str(e.get("due_on") or e.get("due") or e.get("to") or "TBD"))

        cls = ""
        if status and status.lower() in ("completed", "done"):
            cls = "milestone-completed"
        elif status and status.lower() in ("in progress", "inprogress", "ongoing"):
            cls = "milestone-inprogress"
        status_cell = f"<td class='milestone-status-inprogress'>{escape(status)}</td>" if cls == "milestone-inprogress" else f"<td class=''>{escape(status)}</td>"
        rows.append(f"<tr class='{cls}'><td>{name}</td>{status_cell}<td>{start}</td><td>{due}</td></tr>")

    if not rows:
        return "<div class=\"milestone-intro\">No milestone entries found.</div>"

    tbl = "<table class='milestone-table'><tr><th>Name</th><th>Status</th><th>Start</th><th>Due</th></tr>"
    tbl += "".join(rows)
    tbl += "</table>"
    return tbl


def _render_runs_table_html(global_json: Optional[dict]) -> str:
    if not global_json:
        return "<p class='placeholder'>No runs summary available.</p>"

    rows = global_json.get("rows") or []
    grand = global_json.get("grand") or {}

    thead = (
        "<thead><tr><th rowspan='3'>Run Name [Configuration]</th><th rowspan='3'>Planned</th><th colspan='2'>Executed</th>"
        "<th colspan='2'>Not Executed</th><th class='gap' rowspan='3'></th><th colspan='4'>Out of executed</th></tr>"
        "<tr><th colspan='2'></th><th colspan='2'></th><th colspan='2'>Passed</th><th colspan='2'>Failed</th></tr>"
        "<tr><th>%</th><th>#</th><th>%</th><th>#</th><th>%</th><th>#</th><th>%</th><th>#</th></tr></thead>"
    )

    body_rows = []
    plan_name = global_json.get("plan_name")
    if plan_name:
        body_rows.append(f"<tr><td class='group-sep' colspan='11'>Plan: {escape(str(plan_name))}</td></tr>")

    for r in rows:
        run_label = escape(str(r.get("run_label") or r.get("run_name") or f"Run {r.get('run_id','')}"))
        planned = int(r.get("planned", 0))
        executed = int(r.get("executed", 0))
        passed = int(r.get("passed", 0))
        failed = int(r.get("failed", 0))
        not_exec = int(r.get("not_executed", max(0, planned - executed)))
        executed_pct = f"{(executed / planned * 100):.1f}%" if planned else "0.0%"
        not_exec_pct = f"{(not_exec / planned * 100):.1f}%" if planned else "0.0%"
        passed_pct = f"{(passed / executed * 100):.1f}%" if executed else "0.0%"
        failed_pct = f"{(failed / executed * 100):.1f}%" if executed else "0.0%"
        body_rows.append(
            "<tr>"
            f"<td class='left'>{run_label}</td>"
            f"<td>{planned}</td>"
            f"<td>{executed_pct}</td><td>{executed}</td>"
            f"<td>{not_exec_pct}</td><td>{not_exec}</td>"
            "<td class='gap'></td>"
            f"<td>{passed_pct}</td><td>{passed}</td><td>{failed_pct}</td><td>{failed}</td>"
            "</tr>"
        )

    grand_planned = int(grand.get("Planned", sum(int(r.get("planned", 0)) for r in rows)))
    grand_executed = int(grand.get("Executed", sum(int(r.get("executed", 0)) for r in rows)))
    grand_not_exec = int(grand.get("Not Executed", max(0, grand_planned - grand_executed)))
    grand_passed = int(grand.get("Passed", sum(int(r.get("passed", 0)) for r in rows)))
    grand_failed = int(grand.get("Failed", sum(int(r.get("failed", 0)) for r in rows)))
    grand_executed_pct = f"{(grand_executed / grand_planned * 100):.1f}%" if grand_planned else "0.0%"
    grand_not_exec_pct = f"{(grand_not_exec / grand_planned * 100):.1f}%" if grand_planned else "0.0%"
    grand_passed_pct = f"{(grand_passed / grand_executed * 100):.1f}%" if grand_executed else "0.0%"
    grand_failed_pct = f"{(grand_failed / grand_executed * 100):.1f}%" if grand_executed else "0.0%"

    totals_row = (
        "<tr class='group-totals'><td class='left'><strong>TOTAL</strong></td>"
        f"<td>{grand_planned}</td><td>{grand_executed_pct}</td><td>{grand_executed}</td>"
        f"<td>{grand_not_exec_pct}</td><td>{grand_not_exec}</td><td class='gap'></td>"
        f"<td>{grand_passed_pct}</td><td>{grand_passed}</td><td>{grand_failed_pct}</td><td>{grand_failed}</td></tr>"
    )
    grand_row = (
        f"<tr class='grand'><td class='left'>GRAND TOTAL</td><td>{grand_planned}</td><td>{grand_executed_pct}</td><td>{grand_executed}</td>"
        f"<td>{grand_not_exec_pct}</td><td>{grand_not_exec}</td><td class='gap'></td><td>{grand_passed_pct}</td><td>{grand_passed}</td><td>{grand_failed_pct}</td><td>{grand_failed}</td></tr>"
    )

    tbody = "<tbody>" + "".join(body_rows) + totals_row + grand_row + "</tbody>"
    table = f"<table class='runs-table' role='table' aria-label='Runs table grouped by plan'>{thead}{tbody}</table>"
    return table

def _render_per_plan_charts_html(report_output_dir: Path, results_json: Path) -> str:
    """
    Find per-plan charts, order them per config and per-plan id, copy into report_output_dir,
    and return concatenated HTML snippets for each chart.

    Titles: prefer plan name from the corresponding results_plan_<id>.json (if present), else
    "Plan <id> velocity".
    """
    repo_output_dir = Path(__file__).resolve().parent / "output"
    run_output_dir = results_json.parent / "output"
    discovered = _discover_per_plan_charts(repo_output_dir, run_output_dir)
    if not discovered:
        return "<section class='chart'><div class='placeholder'>No per-plan charts found</div></section>"

    ordered = _order_per_plan_charts(discovered, results_json)
    copied = _copy_charts_to_report_dir(ordered, report_output_dir)

    parts = []
    # Preload plan name mapping from results_plan_<id>.json files
    plan_name_map = {}
    results_dir = results_json.parent if results_json.parent != Path("") else Path(".")
    for pjson in results_dir.glob("results_plan_*.json"):
        try:
            obj = _load_json_safe(pjson) or {}
            pid = obj.get("plan_id")
            if pid is None:
                # try to extract id from filename
                name = pjson.name
                if name.startswith("results_plan_") and name.endswith(".json"):
                    try:
                        pid = int(name[len("results_plan_"):-len(".json")])
                    except Exception:
                        pid = None
            if pid is not None:
                pname = obj.get("plan_name") or obj.get("plan_title") or obj.get("plan") or None
                if pname:
                    plan_name_map[int(pid)] = str(pname)
        except Exception:
            continue

    for p in copied:
        pid = _classify_plan_chart(p)
        title = None
        if pid is not None:
            # prefer readable plan name from mapping
            pname = plan_name_map.get(int(pid))
            if pname:
                title = f"{pname}, plan #{pid}"
            else:
                title = f"Plan {pid} velocity"
        else:
            # Should not happen for per-plan chart flow, but handle gracefully
            title = "Plan velocity"
        parts.append(f"<div class='chart'><h3>{escape(title)}</h3><img class='chart-img' alt='{escape(title)}' src='{escape(p.name)}' /></div>")
    return "\n".join(parts)

def _assemble_html_file(output_dir: Path, results_json: Path, milestone_json: Optional[Path] = None) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    global_json = _load_json_safe(results_json) or {}
    milestones_html = _render_milestone_table_html(milestone_json) if milestone_json and milestone_json.exists() else "<div class='milestone-intro'>No milestone data</div>"
    runs_table_html = _render_runs_table_html(global_json)
    per_plan_charts_html = _render_per_plan_charts_html(output_dir, results_json)

    generated_for_date = escape(str(global_json.get("generated_for_date", "")))
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    html_name = f"report_{stamp}.html"
    html_path = output_dir / html_name

    with html_path.open("w", encoding="utf-8") as fh:
        fh.write("<!doctype html>\n")
        fh.write("<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n")
        fh.write("<title>TestRail Report</title>\n")
        fh.write("<style>\n")
        fh.write("    body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, \"Helvetica Neue\", Arial; margin:28px; color:#222; background:#fff; }\n")
        fh.write("    header { margin-bottom:18px; }\n")
        fh.write("    h1 { font-size:20px; margin:0 0 6px 0; }\n")
        fh.write("    .meta { color:#555; margin-bottom:12px; }\n")
        fh.write("    .milestone-intro { background:#f6f7f8; border:1px dashed #ddd; padding:10px 12px; margin-bottom:10px; color:#555; }\n")
        fh.write("    .milestone-table, .testruns-table { border-collapse: collapse; margin-bottom:16px; box-shadow: 0 1px 0 rgba(0,0,0,0.03); width:100%; }\n")
        fh.write("    .milestone-table th, .testruns-table th { background: #696969; color: #ffffff; font-weight: 700; padding: 8px 10px; border: 1px solid #ccc; text-align:left; }\n")
        fh.write("    .milestone-table td, .testruns-table td { background: transparent; color: #111; padding: 8px 10px; border: 1px solid #ccc; vertical-align: middle; }\n")
        fh.write("    tr.milestone-completed { background-color: #D3D3D3; }\n")
        fh.write("    tr.milestone-inprogress { background-color: transparent; }\n")
        fh.write("    table.milestone-table td.milestone-status-inprogress, table.testruns-table td.milestone-status-inprogress { color: #2e7d32; font-weight: 700; background: transparent; }\n")
        fh.write("    .chart { text-align:left; margin-top:8px; }\n")
        fh.write("    img.chart-img { max-width:100%; height:auto; border:1px solid #eee; box-shadow:0 1px 2px rgba(0,0,0,0.04); display:block; margin:0; }\n")
        fh.write("    .placeholder { background:#f6f7f8; border:1px dashed #ddd; padding:12px; margin-bottom:12px; color:#666; }\n")
        fh.write("    .extra { margin-top:12px; margin-bottom:12px; color:#333; }\n")
        fh.write("    footer { margin-top:18px; color:#666; font-size:13px; }\n")
        fh.write("    .small { font-size:13px; color:#666; }\n")
        fh.write("</style>\n</head>\n<body>\n")
        fh.write("<header>\n  <h1>TestRail Report</h1>\n")
        fh.write(f"  <div class=\"meta\">Report generated for date: <strong>{generated_for_date}</strong></div>\n</header>\n")
        fh.write("<section class=\"milestone-section\">\n")
        fh.write(milestones_html)
        fh.write("\n</section>\n")
        fh.write("<section class=\"placeholder\">\n  <div><strong>Testing activities</strong></div>\n</section>\n")
        fh.write("<section class=\"table-placeholder\" id=\"table-placeholder\">\n")
        fh.write("<style>\n")
        fh.write("    .runs-table { border-collapse: collapse; width:100%; max-width:1200px; margin-bottom:12px; font-family:Arial,Helvetica,sans-serif; }\n")
        fh.write("    .runs-table th, .runs-table td { border:1px solid #333; padding:6px 8px; text-align:center; vertical-align:middle; }\n")
        fh.write("    .runs-table th { background:#696969; color:#fff; font-weight:700; }\n")
        fh.write("    .runs-table td.left { text-align:left; }\n")
        fh.write("    .runs-table td.gap { width:10px; background:transparent; border-left:1px solid #333; border-right:1px solid #333; border-top:none; border-bottom:none; }\n")
        fh.write("    .runs-table td.gap.gap-top { border-top:1px solid #333; }\n")
        fh.write("    .runs-table td.gap.gap-bottom { border-bottom:1px solid #333; }\n")
        fh.write("    .runs-table .grand { font-weight:700; background:transparent; }\n")
        fh.write("    .runs-table td { background: transparent; color:#111; }\n")
        fh.write("    .runs-table thead th:first-child { text-align:left; }\n")
        fh.write("    .runs-table .group-sep { background: #f4f4f4; text-align:left; font-weight:700; padding:8px 10px; }\n")
        fh.write("    .runs-table .group-totals { font-weight:700; background:transparent; }\n")
        fh.write("</style>\n")
        fh.write(runs_table_html)
        fh.write("\n</section>\n")
        fh.write("<section class=\"extra\">\n  <div>Below are per-plan velocity charts.</div>\n</section>\n")
        fh.write(per_plan_charts_html + "\n")
        fh.write(f"<footer>\n  <div class=\"small\">Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</div>\n</footer>\n")
        fh.write("</body>\n</html>\n")

    logger.info("HTML report written to: %s", html_path)
    return html_path


def parse_cli(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assemble HTML report from omni JSON and per-plan charts")
    parser.add_argument("--omni-script", help="Path to omni.py to run when not skipping omni", default=None)
    parser.add_argument("--results-json", help="Path to results.json", default="results.json")
    parser.add_argument("--output-dir", help="Directory to write report and charts", default="output")
    parser.add_argument("--skip-omni", action="store_true", help="Do not run omni; assume JSONs are present")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose chart generation")
    parser.add_argument("--milestone-json", help="Path to milestone JSON (optional)", default="milestone_data.json")
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = parse_cli(argv)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    results_json = Path(args.results_json)
    output_dir = Path(args.output_dir)
    milestone_json = Path(args.milestone_json) if args.milestone_json else None

    # Optionally refresh data
    if not args.skip_omni and args.omni_script:
        rc = _run_omni_subprocess(Path(args.omni_script), extra_args=[f"--json-out={str(results_json)}"])
        if rc != 0:
            logger.error("omni subprocess failed; aborting output stage")
            return rc
    else:
        if args.skip_omni:
            logger.info("Skipping omni run as requested (--skip-omni)")

    # Generate per-plan charts (or no-op if orchestrator already created them)
    per_plan = _collect_per_plan_jsons(results_json)
    json_names = [p.name for p in per_plan]
    _generate_charts_for_jsons(results_json.parent, json_names, verbose=args.verbose)

    html_path = _assemble_html_file(output_dir, results_json, milestone_json=milestone_json)
    logger.info("Completed output assembly: %s", html_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))