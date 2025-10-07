#!/usr/bin/env python3
"""
table_testruns.py

Render a single combined HTML runs table grouped by plan.

Improvements applied:
- Uses logging instead of silent failures
- Narrow exception handling for optional imports and network errors
- Normalizes input rows once to canonical keys and types
- Avoids repeated try/except in hot loops
- Respect optional PLAN_NAME_MAP from config; avoid leaking credentials in logs
- Clearer function boundaries and docstrings
- Defensive handling of malformed or missing values
"""

from __future__ import annotations

import html
import logging
import os
from collections import defaultdict
from typing import Dict, Any, Optional, Iterable, List, Tuple, Set

# Optional config import (safe if missing)
try:
    import config  # type: ignore
except (ImportError, ModuleNotFoundError):
    config = None

# Optional mapping fallback: define PLAN_NAME_MAP = {222: "Name"} in config.py to avoid API call
PLAN_NAME_MAP: Dict[int, str] = {}
if config is not None:
    PLAN_NAME_MAP = getattr(config, "PLAN_NAME_MAP", {}) or {}

# Configure logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = "%(asctime)s %(levelname)s %(message)s"
    handler.setFormatter(logging.Formatter(fmt, "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
logger.propagate = False
logger.setLevel(logging.INFO)


def _esc(x: Optional[object]) -> str:
    return html.escape(str(x)) if x is not None else ""


def _get_testrail_credentials() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return (base, user, key) or (None, None, None) if incomplete.
    Credentials are read from config attributes or environment variables.
    """
    try:
        base = getattr(config, "TESTRAIL_URL", None)
        user = getattr(config, "TESTRAIL_USERNAME", None) or getattr(config, "USERNAME", None)
        key = getattr(config, "TESTRAIL_API_KEY", None) or getattr(config, "API_KEY", None)
    except Exception:
        base = None
        user = None
        key = None

    base = base or os.getenv("TESTRAIL_URL")
    user = user or os.getenv("TESTRAIL_USERNAME") or os.getenv("USERNAME")
    key = key or os.getenv("TESTRAIL_API_KEY") or os.getenv("API_KEY")

    if base and user and key:
        return base.rstrip("/"), user, key
    return None, None, None


def _get_testrail_plan_info(plan_id: int, timeout: int = 8) -> Optional[Dict[str, Any]]:
    """
    Return {"name": str, "run_ids": set[int]} on success, or None on error/missing credentials.
    Logs errors at debug level to avoid leaking credentials in normal logs.
    """
    base, user, key = _get_testrail_credentials()
    if not (base and user and key):
        logger.debug("TestRail credentials not available; skipping plan info fetch for %s", plan_id)
        return None

    try:
        import requests  # lazy import
        url = f"{base}/index.php?/api/v2/get_plan/{int(plan_id)}"
        resp = requests.get(url, auth=(user, key), timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        run_ids: Set[int] = set()
        for entry in data.get("entries", []):
            for run in entry.get("runs", []):
                rid = run.get("id")
                if rid is not None:
                    try:
                        run_ids.add(int(rid))
                    except (ValueError, TypeError):
                        continue
        plan_name = data.get("name") or f"Plan {plan_id}"
        return {"name": plan_name, "run_ids": run_ids}
    except Exception as exc:
        # Log at debug to avoid exposing details unless verbose debug is enabled
        logger.debug("Failed to fetch plan %s: %s", plan_id, exc)
        return None


def _normalize_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a single input row to canonical keys and types used by rendering logic.

    Canonical keys:
      - run_id: Optional[int]
      - run_name: str
      - configuration: str
      - planned: int
      - executed: int
      - blocked: int
      - untested: int
      - not_executed: int (derived if absent)
      - passed: int
      - failed: int
      - plan_key: str (string used for grouping; may be numeric string)
      - other original keys preserved
    """
    r = dict(raw or {})
    def safe_int(v, default=0):
        try:
            if v is None:
                return default
            if isinstance(v, bool):
                return int(v)
            return int(str(v))
        except (ValueError, TypeError):
            return default

    # determine run id
    run_id = None
    for cand in ("run_id", "id"):
        if cand in r and r[cand] not in (None, ""):
            try:
                run_id = safe_int(r[cand], None)
                break
            except Exception:
                run_id = None

    # determine run name / label
    run_name = r.get("run_name") or r.get("name") or r.get("run_label") or ""
    configuration = r.get("configuration") or r.get("config") or r.get("suite_name") or r.get("env") or ""

    # numeric fields
    planned = safe_int(r.get("planned", r.get("Planned", 0)))
    executed = safe_int(r.get("executed", r.get("Executed", 0)))
    blocked = safe_int(r.get("blocked", 0))
    untested = safe_int(r.get("untested", 0))
    not_executed = safe_int(r.get("not_executed", None), None)
    if not_executed is None:
        # prefer explicit blocked/untested else derive
        if blocked or untested:
            not_executed = blocked + untested
        else:
            not_executed = max(0, planned - executed)

    passed = safe_int(r.get("passed", 0))
    failed = safe_int(r.get("failed", 0))

    # plan grouping key - prefer explicit plan related keys
    candidates = ("plan", "plan_id", "plan_name", "plan_title", "planned_for")
    plan_key = "Unplanned"
    for k in candidates:
        if k in r and r[k] not in (None, ""):
            plan_key = str(r[k])
            break
    if plan_key == "Unplanned":
        # also check lowercase variants
        lk = {kk.lower(): vv for kk, vv in r.items()}
        for k in candidates:
            if k.lower() in lk and lk[k.lower()] not in (None, ""):
                plan_key = str(lk[k.lower()])
                break

    normalized = {
        **r,
        "run_id": run_id,
        "run_name": str(run_name),
        "configuration": str(configuration),
        "planned": planned,
        "executed": executed,
        "blocked": blocked,
        "untested": untested,
        "not_executed": not_executed,
        "passed": passed,
        "failed": failed,
        "plan_key": plan_key,
    }
    return normalized


def _remap_unplanned_by_plan_run_ids(groups: Dict[str, List[Dict[str, Any]]], candidate_plan_ids: Iterable[int]) -> Dict[str, List[Dict[str, Any]]]:
    """
    If 'Unplanned' exists and its rows include run_id values that match any plan's runs,
    remap 'Unplanned' to the plan name from API or to PLAN_NAME_MAP entry.
    candidate_plan_ids is a list of ints to check (order matters).
    """
    if "Unplanned" not in groups:
        return groups
    unplanned_rows = groups["Unplanned"]

    # quick map lookup from config
    for pid in candidate_plan_ids:
        mapped_name = PLAN_NAME_MAP.get(int(pid))
        if mapped_name:
            pid_s = str(pid)
            # match rows where run_id equals pid (string compare kept for safety)
            if any(str(r.get("run_id")) == pid_s for r in unplanned_rows):
                groups[mapped_name] = groups.get(mapped_name, []) + groups.pop("Unplanned")
                logger.info("Remapped Unplanned group to PLAN_NAME_MAP[%s] -> %s", pid, mapped_name)
                return groups

    # then try API-based remap per candidate id
    for pid in candidate_plan_ids:
        info = _get_testrail_plan_info(int(pid))
        if not info:
            continue
        run_ids_in_plan = info.get("run_ids", set())
        if not run_ids_in_plan:
            continue
        matched = [r for r in unplanned_rows if r.get("run_id") is not None and int(r.get("run_id")) in run_ids_in_plan]
        if matched:
            plan_name = info.get("name") or f"Plan {pid}"
            groups[plan_name] = groups.get(plan_name, []) + groups.pop("Unplanned")
            logger.info("Remapped Unplanned group to TestRail plan %s (%s)", pid, plan_name)
            return groups
    return groups


def build_testruns_table(run_rows: Iterable[Dict[str, Any]], grand: Optional[Dict[str, Any]] = None) -> str:
    """
    Render all runs in a single HTML table, grouped by plan.

    Input:
      - run_rows: iterable of raw dicts (will be normalized)
      - grand: optional dict with summary totals (keys case-insensitive)

    Returns rendered HTML string.
    """
    rows = [ _normalize_row(r) for r in (run_rows or []) ]
    grand = dict(grand or {})

    # group rows by plan_key
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = r.get("plan_key", "Unplanned") or "Unplanned"
        groups[key].append(r)

    # Build candidate plan ids: keys that look numeric
    candidate_plan_ids: List[int] = []
    for k in list(groups.keys()):
        if isinstance(k, str) and k.isdigit():
            try:
                candidate_plan_ids.append(int(k))
            except ValueError:
                continue
    # explicit fallback: include 222 if not already present
    if 222 not in candidate_plan_ids:
        candidate_plan_ids.append(222)

    groups = _remap_unplanned_by_plan_run_ids(groups, candidate_plan_ids)

    # stable ordering: all non-Unplanned sorted, Unplanned last
    plan_keys = sorted(k for k in groups.keys() if k != "Unplanned")
    if "Unplanned" in groups:
        plan_keys.append("Unplanned")

    # CSS + header (kept as a single template block)
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

    out: List[str] = []
    out.append(css_table)
    out.append('<table class="runs-table" role="table" aria-label="Runs table grouped by plan">')
    out.append('<thead>' + "".join(header_rows) + '</thead>')
    out.append('<tbody>')

    overall_totals = {"planned": 0, "executed": 0, "not_executed": 0, "passed": 0, "failed": 0}
    any_rows = False

    for plan_key in plan_keys:
        group_rows = groups.get(plan_key, [])
        if not group_rows:
            continue

        any_rows = True
        out.append(f'<tr><td class="group-sep" colspan="11">Plan: {_esc(plan_key)}</td></tr>')

        g_planned = g_executed = g_not = g_passed = g_failed = 0
        total = len(group_rows)

        for idx, r in enumerate(group_rows):
            run_name = r.get("run_name") or f"Run {r.get('run_id','')}"
            config_val = r.get("configuration") or ""
            if config_val:
                run_label_cell = f"{html.escape(str(run_name))} [{html.escape(str(config_val))}]"
            else:
                run_label_cell = html.escape(str(run_name))

            planned = int(r.get("planned", 0))
            executed = int(r.get("executed", 0))
            blocked = int(r.get("blocked", 0))
            untested = int(r.get("untested", 0))
            not_executed = int(r.get("not_executed", max(0, planned - executed)))
            passed = int(r.get("passed", 0))
            failed = int(r.get("failed", 0))

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

    # Grand totals: allow grand override but fall back to computed totals
    def _safe_grand(key_candidates: Tuple[str, ...], fallback: int) -> int:
        for kc in key_candidates:
            if kc in grand and grand[kc] not in (None, ""):
                try:
                    return int(grand[kc])
                except (ValueError, TypeError):
                    continue
        return int(fallback)

    gp = _safe_grand(("Planned", "planned"), overall_totals["planned"])
    ge = _safe_grand(("Executed", "executed"), overall_totals["executed"])
    gn = _safe_grand(("Not Executed", "not_executed"), overall_totals["not_executed"])
    gpass = _safe_grand(("Passed", "passed"), overall_totals["passed"])
    gfail = _safe_grand(("Failed", "failed"), overall_totals["failed"])

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