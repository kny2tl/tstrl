#!/usr/bin/env python3
"""
testrail_plans.py

Helper module and CLI to list TestRail Test Plans.

Provides:
- fetch_all_plans(session: requests.Session) -> List[Dict[str, Any]]
  Paginate through TestRail API (get_plans) and return a list of plan summary dicts.

- get_plan_details(session: requests.Session, plan_id: int) -> Dict[str, Any]
  Fetch a single plan detail via get_plan/{plan_id} and return parsed dict.

CLI:
- When run as a script it prints JSON array of plans to stdout or writes to --out file.
- Optional --verbose enables debug logging.

Requires a local config.py exposing TESTRAIL_URL, USERNAME, API_KEY.
"""

from __future__ import annotations

import sys
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests

# Import TestRail config (must define TESTRAIL_URL, USERNAME, API_KEY)
from config import TESTRAIL_URL, USERNAME, API_KEY  # type: ignore

logger = logging.getLogger("testrail_plans")
_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.propagate = False


def _session() -> requests.Session:
    s = requests.Session()
    s.auth = (USERNAME, API_KEY)
    s.headers.update({"Accept": "application/json"})
    return s


def _safe_json(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except Exception:
        logger.debug("Non-JSON response (truncated): %s", getattr(resp, "text", "")[:400])
        return None


def fetch_all_plans(session: requests.Session, project_id: Optional[int] = None, per_page: int = 250) -> List[Dict[str, Any]]:
    """
    Fetch test plans across the TestRail instance.

    - If project_id is provided, uses get_plans/{project_id} endpoint (paginated).
    - If project_id is None, attempts to iterate all projects and aggregate plans.
      Note: get_plans API typically needs a project id; calling without a project_id
      is not supported on all TestRail instances. If project_id is omitted this function
      will log a warning and return an empty list.

    Returns a list of plan summary dicts as returned by the API.
    """
    plans: List[Dict[str, Any]] = []

    if project_id is None:
        logger.warning("project_id not provided. To list plans pass --project-id or set project_id argument.")
        return plans

    url_template = f"{TESTRAIL_URL}/index.php?/api/v2/get_plans/{project_id}"
    offset = 0

    while True:
        url = f"{url_template}&limit={per_page}&offset={offset}"
        try:
            resp = session.get(url, timeout=30)
        except Exception as e:
            logger.warning("HTTP error while fetching plans for project %s: %s", project_id, e)
            break
        if resp.status_code != 200:
            logger.warning("get_plans returned %s for project %s", resp.status_code, project_id)
            break
        data = _safe_json(resp)
        if not data:
            break
        if isinstance(data, dict) and "plans" in data:
            batch = data.get("plans") or []
        elif isinstance(data, list):
            # some TestRail instances return a flat list
            batch = data
        else:
            logger.warning("Unexpected JSON shape from get_plans for project %s", project_id)
            break

        if not batch:
            break

        plans.extend(batch)
        if len(batch) < per_page:
            break
        offset += len(batch)

    logger.info("Fetched %d plans for project %s", len(plans), project_id)
    return plans


def get_plan_details(session: requests.Session, plan_id: int) -> Dict[str, Any]:
    """
    Fetch detailed information for a single plan using get_plan/{plan_id}.

    Returns the JSON-decoded dict on success, or an empty dict on failure.
    """
    url = f"{TESTRAIL_URL}/index.php?/api/v2/get_plan/{plan_id}"
    try:
        resp = session.get(url, timeout=30)
    except Exception as e:
        logger.warning("HTTP error while fetching plan %s: %s", plan_id, e)
        return {}
    if resp.status_code != 200:
        logger.warning("get_plan returned %s for plan %s", resp.status_code, plan_id)
        return {}
    data = _safe_json(resp)
    if isinstance(data, dict):
        return data
    logger.warning("Unexpected JSON shape for plan %s", plan_id)
    return {}


def summarize_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a compact summary for a plan dict:
    - id, name, assignedto_id, passed_count, failed_count, entry_count, run_ids, created_by, created_on
    """
    run_ids: List[int] = []
    entries = plan.get("entries") or []
    for e in entries:
        for r in (e.get("runs") or []):
            rid = r.get("id") or r.get("run_id")
            if rid is not None:
                try:
                    run_ids.append(int(rid))
                except Exception:
                    continue
    summary = {
        "id": plan.get("id"),
        "name": plan.get("name"),
        "milestone_id": plan.get("milestone_id"),
        "assignedto_id": plan.get("assignedto_id"),
        "run_ids": sorted(set(run_ids)),
        "entries_count": len(entries),
        "created_by": plan.get("created_by"),
        "created_on": plan.get("created_on"),
    }
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Query TestRail for test plans and print JSON.")
    parser.add_argument("--project-id", type=int, help="TestRail project id to list plans for", required=True)
    parser.add_argument("--out", help="Write JSON to this file (otherwise stdout)", default=None)
    parser.add_argument("--details", action="store_true", help="Include full plan details (calls get_plan for each plan)")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)

    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    session = _session()

    plans = fetch_all_plans(session, project_id=args.project_id)
    if args.details and plans:
        detailed = []
        for p in plans:
            pid = p.get("id")
            if pid is None:
                continue
            detail = get_plan_details(session, int(pid))
            detailed.append(detail)
        out_obj = detailed
    else:
        out_obj = [summarize_plan(p) for p in plans]

    out_data = json.dumps(out_obj, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(out_data, encoding="utf-8")
        logger.info("Wrote %d plans to %s", len(out_obj), args.out)
    else:
        print(out_data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))