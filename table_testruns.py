#!/usr/bin/env python3
"""
table_testruns.py

Render a single combined HTML runs table grouped by plan.

Behavior:
- Groups by plan key (plan, plan_id, plan_name, plan_title, planned_for).
- If a group is "Unplanned" but contains runs whose run_id appear in a TestRail plan,
  the group will be renamed to the TestRail plan name by querying get_plan/<id>.
- TestRail credentials are read from config.py (optional) or from environment variables:
  TESTRAIL_URL, TESTRAIL_USERNAME, TESTRAIL_API_KEY.
- No debug output included.
"""
from __future__ import annotations

import html
import os
from collections import defaultdict
from typing import Dict, Any, Optional

# Optional config import (safe if missing)
try:
    import config  # type: ignore
except Exception:
    config = None

# Optional mapping fallback: define PLAN_NAME_MAP = {222: "Name"} in config.py to avoid API call
PLAN_NAME_MAP: Dict[int, str] = {}
if config is not None:
    PLAN_NAME_MAP = getattr(config, "PLAN_NAME_MAP", {}) or {}

def _esc(x):
    return html.escape(str(x)) if x is not None else ""

def _get_testrail_credentials():
    try:
        base = getattr(config, "TESTRAIL_URL", None)
        user = getattr(config, "TESTRAIL_USERNAME", None) or getattr(config, "USERNAME", None)
        key = getattr(config, "TESTRAIL_API_KEY", None) or getattr(config, "API_KEY", None)
    except Exception:
        base = None; user = None; key = None
    base = base or os.getenv("TESTRAIL_URL")
    user = user or os.getenv("TESTRAIL_USERNAME") or os.getenv("USERNAME")
    key = key or os.getenv("TESTRAIL_API_KEY") or os.getenv("API_KEY")
    if base and user and key:
        return base.rstrip('/'), user, key
    return None, None, None

def _get_testrail_plan_info(plan_id: int, timeout: int = 8) -> Optional[Dict[str, Any]]:
    """
    Return {'name': str, 'run_ids': set[int]} on success, or None on error/missing credentials.
    """
    base, user, key = _get_testrail_credentials()
    if not (base and user and key):
        return None
    try:
        import requests  # lazy import
        url = f"{base}/index.php?/api/v2/get_plan/{int(plan_id)}"
        resp = requests.get(url, auth=(user, key), timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        run_ids = set()
        for entry in data.get("entries", []):
            for run in entry.get("runs", []):
                rid = run.get("id")
                if rid is not None:
                    try:
                        run_ids.add(int(rid))
                    except Exception:
                        continue
        return {"name": data.get("name") or f"Plan {plan_id}", "run_ids": run_ids}
    except Exception:
        return None

def _remap_unplanned_by_plan_run_ids(groups: Dict[str, list], candidate_plan_ids: list) -> Dict[str, list]:
    """
    If 'Unplanned' exists and its rows include run_id values that match any plan's runs,
    remap 'Unplanned' to the plan name from API or to PLAN_NAME_MAP entry.
    candidate_plan_ids is a list of ints to check (order matters).
    """
    if "Unplanned" not in groups:
        return groups
    unplanned_rows = groups["Unplanned"]

    # first, try config map quickly
    for pid in candidate_plan_ids:
        mapped_name = PLAN_NAME_MAP.get(int(pid))
        if mapped_name:
            pid_s = str(pid)
            for r in unplanned_rows:
                if str(r.get("run_id")) == pid_s:
                    groups[mapped_name] = groups.get(mapped_name, []) + groups.pop("Unplanned")
                    return groups

    # then, try API-based remap per candidate id
    for pid in candidate_plan_ids:
        info = _get_testrail_plan_info(int(pid))
        if not info:
            continue
        run_ids_in_plan = info.get("run_ids", set())
        if not run_ids_in_plan:
            continue
        matched = [r for r in unplanned_rows if r.get("run_id") is not None and int(str(r.get("run_id"))) in run_ids_in_plan]
        if matched:
            plan_name = info.get("name") or f"Plan {pid}"
            groups[plan_name] = groups.get(plan_name, []) + groups.pop("Unplanned")
            return groups
    return groups

def build_testruns_table(run_rows: list, grand: dict) -> str:
    """
    Render all runs in a single HTML table, grouped by plan.
    """
    rows = run_rows or []
    grand = grand or {}

    # plan key extraction helper (avoid using numeric 'planned' count as key)
    def plan_key_value(r: Dict[str, Any]) -> str:
        candidates = ("plan", "plan_id", "plan_name", "plan_title", "planned_for")
        for k in candidates:
            if k in r and r[k] not in (None, ""):
                return str(r[k])
        lk = {kk.lower(): v for kk, v in r.items()}
        for k in candidates:
            if k.lower() in lk and lk[k.lower()] not in (None, ""):
                return str(lk[k.lower()])
        return "Unplanned"

    # group rows
    groups: Dict[str, list] = defaultdict(list)
    for r in rows:
        try:
            key = plan_key_value(r)
        except Exception:
            key = "Unplanned"
        groups[key].append(r)

    # If Unplanned present, attempt to remap using provided candidate plan IDs:
    # - prefer PLAN_NAME_MAP entries
    # - then try API lookup for candidate ids
    # Customize candidate_plan_ids as needed; include 222 as reported.
    candidate_plan_ids = []
    # include keys present as numeric strings (likely plan ids) and include 222 as explicit candidate
    for k in list(groups.keys()):
        if isinstance(k, str) and k.isdigit():
            try:
                candidate_plan_ids.append(int(k))
            except Exception:
                pass
    # explicit fallback: include 222 if not already present
    if 222 not in candidate_plan_ids:
        candidate_plan_ids.append(222)

    groups = _remap_unplanned_by_plan_run_ids(groups, candidate_plan_ids)

    # stable order, Unplanned last
    plan_keys = sorted(k for k in groups.keys() if k != "Unplanned")
    if "Unplanned" in groups:
        plan_keys.append("Unplanned")

    # CSS + header
    css_table = """
    <style>
    .runs-table { border-collapse: collapse; width:100%; max-width:1200px; margin-bottom:12px; font-family:Arial,Helvetica,sans-serif; }
    .runs-table th, .runs-table td { border:1px solid #333; padding:6px 8px; text-align:center; vertical-align:middle; }
    .runs-table th { background:#696969; color:#fff; font-weight:700; }
    .runs-table td.left { text-align:left; }
    .runs-table td.gap { width:10px; background:transparent; border-left:1px solid #333; border-right:1px solid #333; border-top:none; border-bottom:none; }
    .runs-table td.gap.gap-top { border-top:1px solid #333; }
    .runs-table td.gap.gap-bottom { border-bottom:1px solid #333; }
    .runs-table .grand { font-weight:700; background:transparent; }
    .runs-table td { background: transparent; color:#111; }
    .runs-table thead th:first-child { text-align:left; }
    .runs-table .group-sep { background: #f4f4f4; text-align:left; font-weight:700; padding:8px 10px; }
    .runs-table .group-totals { font-weight:700; background:transparent; }
    .muted { color:#666; font-size:0.9em; margin-left:6px; }
    </style>
    """

    header_rows = [
        '<tr>'
        '<th rowspan="3">Run Name [Configuration]</th>'
        '<th rowspan="3">Planned</th>'
        '<th colspan="2">Executed</th>'
        '<th colspan="2">Not Executed</th>'
        '<th class="gap" rowspan="3"></th>'
        '<th colspan="4">Out of executed</th>'
        '</tr>',
        '<tr><th colspan="2"></th><th colspan="2"></th><th colspan="2">Passed</th><th colspan="2">Failed</th></tr>',
        '<tr><th>%</th><th>#</th><th>%</th><th>#</th><th>%</th><th>#</th><th>%</th><th>#</th></tr>'
    ]

    out = []
    out.append(css_table)
    out.append(f'<table class="runs-table" role="table" aria-label="Runs table grouped by plan">')
    out.append('<thead>' + "".join(header_rows) + '</thead>')
    out.append('<tbody>')

    overall_totals = {"planned": 0, "executed": 0, "not_executed": 0, "passed": 0, "failed": 0}
    any_rows = False

    for plan_key in plan_keys:
        group_rows = groups[plan_key]
        if not group_rows:
            continue

        any_rows = True
        out.append(f'<tr><td class="group-sep" colspan="11">Plan: {_esc(plan_key)}</td></tr>')

        g_planned = g_executed = g_not = g_passed = g_failed = 0
        total = len(group_rows)

        for idx, r in enumerate(group_rows):
            run_name = r.get("run_name") or r.get("name") or r.get("run_label") or f"Run {r.get('run_id','')}"
            config = r.get("configuration") or r.get("config") or r.get("suite_name") or r.get("env") or ""
            if config:
                run_label_cell = f"{html.escape(str(run_name))} [{html.escape(str(config))}]"
            else:
                run_label_cell = html.escape(str(run_name))

            try:
                planned = int(r.get("planned", r.get("Planned", 0)))
            except Exception:
                planned = 0
            try:
                executed = int(r.get("executed", r.get("Executed", 0)))
            except Exception:
                executed = 0
            try:
                blocked = int(r.get("blocked", 0))
            except Exception:
                blocked = 0
            try:
                untested = int(r.get("untested", 0))
            except Exception:
                untested = 0

            if (blocked or untested):
                not_executed = blocked + untested
            else:
                try:
                    not_executed = int(r.get("not_executed", max(0, planned - executed)))
                except Exception:
                    not_executed = max(0, planned - executed)

            try:
                passed = int(r.get("passed", 0))
            except Exception:
                passed = 0
            try:
                failed = int(r.get("failed", 0))
            except Exception:
                failed = 0

            executed_pct = f"{(executed / planned * 100):.1f}%" if planned else "0.0%"
            not_executed_pct = f"{(not_executed / planned * 100):.1f}%" if planned else "0.0%"
            passed_pct = f"{(passed / executed * 100):.1f}%" if executed else "0.0%"
            failed_pct = f"{(failed / executed * 100):.1f}%" if executed else "0.0%"

            gap_classes = "gap"
            if idx == 0:
                gap_classes += " gap-top"
            if idx == total - 1:
                gap_classes += " gap-bottom"

            out.append(
                '<tr>'
                f'<td class="left">{run_label_cell}</td>'
                f'<td>{planned}</td>'
                f'<td>{executed_pct}</td>'
                f'<td>{executed}</td>'
                f'<td>{not_executed_pct}</td>'
                f'<td>{not_executed}</td>'
                f'<td class="{gap_classes}"></td>'
                f'<td>{passed_pct}</td>'
                f'<td>{passed}</td>'
                f'<td>{failed_pct}</td>'
                f'<td>{failed}</td>'
                '</tr>'
            )

            g_planned += planned
            g_executed += executed
            g_not += not_executed
            g_passed += passed
            g_failed += failed

            overall_totals["planned"] += planned
            overall_totals["executed"] += executed
            overall_totals["not_executed"] += not_executed
            overall_totals["passed"] += passed
            overall_totals["failed"] += failed

        g_executed_pct = f"{(g_executed / g_planned * 100):.1f}%" if g_planned else "0.0%"
        g_not_pct = f"{(g_not / g_planned * 100):.1f}%" if g_planned else "0.0%"
        g_passed_pct = f"{(g_passed / g_executed * 100):.1f}%" if g_executed else "0.0%"
        g_failed_pct = f"{(g_failed / g_executed * 100):.1f}%" if g_executed else "0.0%"

        out.append(
            '<tr class="group-totals">'
            '<td class="left"><strong>TOTAL</strong></td>'
            f'<td>{g_planned}</td>'
            f'<td>{g_executed_pct}</td>'
            f'<td>{g_executed}</td>'
            f'<td>{g_not_pct}</td>'
            f'<td>{g_not}</td>'
            f'<td class="gap gap-bottom"></td>'
            f'<td>{g_passed_pct}</td>'
            f'<td>{g_passed}</td>'
            f'<td>{g_failed_pct}</td>'
            f'<td>{g_failed}</td>'
            '</tr>'
        )

    if not any_rows:
        out.append('<tr><td class="left" colspan="11" style="border:1px solid #eee; color:#666;">No runs to display</td></tr>')

    try:
        gp = int(grand.get("Planned", grand.get("planned", overall_totals["planned"])))
    except Exception:
        gp = overall_totals["planned"]
    try:
        ge = int(grand.get("Executed", grand.get("executed", overall_totals["executed"])))
    except Exception:
        ge = overall_totals["executed"]
    try:
        gn = int(grand.get("Not Executed", grand.get("not_executed", overall_totals["not_executed"])))
    except Exception:
        gn = overall_totals["not_executed"]
    try:
        gpass = int(grand.get("Passed", grand.get("passed", overall_totals["passed"])))
    except Exception:
        gpass = overall_totals["passed"]
    try:
        gfail = int(grand.get("Failed", grand.get("failed", overall_totals["failed"])))
    except Exception:
        gfail = overall_totals["failed"]

    overall_pct_exec = f"{(ge / gp * 100):.1f}%" if gp else "0.0%"
    overall_pct_not = f"{(gn / gp * 100):.1f}%" if gp else "0.0%"
    overall_pct_pass = f"{(gpass / ge * 100):.1f}%" if ge else "0.0%"
    overall_pct_fail = f"{(gfail / ge * 100):.1f}%" if ge else "0.0%"

    out.append(
        '<tr class="grand">'
        '<td class="left">GRAND TOTAL</td>'
        f'<td>{gp}</td>'
        f'<td>{overall_pct_exec}</td>'
        f'<td>{ge}</td>'
        f'<td>{overall_pct_not}</td>'
        f'<td>{gn}</td>'
        f'<td class="gap gap-bottom"></td>'
        f'<td>{overall_pct_pass}</td>'
        f'<td>{gpass}</td>'
        f'<td>{overall_pct_fail}</td>'
        f'<td>{gfail}</td>'
        '</tr>'
    )

    out.append('</tbody></table>')
    return "\n".join(out)