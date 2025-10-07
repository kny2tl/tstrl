#!/usr/bin/env python3
"""
omni.py — data fetcher and JSON producer (global + per-plan)

Responsibilities:
- Fetch TestRail results for configured RUN_IDS
- Produce a global results.json containing dates, daily_results, cumulative, rows, grand
- Produce per-plan JSON files results_plan_<PLANID>.json containing plan_id and plan_name
- Use resilient HTTP session, atomic writes, and clear logging
- CLI: --json-out, --days, --dry-run, --verbose, --workers
"""
from __future__ import annotations

import sys
import json
import argparse
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta, time as dtime
from typing import List, Dict, Any, Tuple, Optional, Iterable, Set
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Local config: must define TESTRAIL_URL, USERNAME, API_KEY, RUN_IDS
from config import TESTRAIL_URL, USERNAME, API_KEY, RUN_IDS  # type: ignore

# Optional plan config (either form accepted)
try:
    from config import CHART_PLAN_SETS  # type: ignore
except Exception:
    CHART_PLAN_SETS = None
try:
    from config import CHART_PLAN_IDS  # type: ignore
except Exception:
    CHART_PLAN_IDS = None

# Optional local tz config
try:
    from config import LOCAL_TZ_NAME  # type: ignore
except Exception:
    LOCAL_TZ_NAME = None
try:
    from config import LOCAL_TZ_OFFSET_HOURS  # type: ignore
except Exception:
    LOCAL_TZ_OFFSET_HOURS = 0

# Logging
logger = logging.getLogger("omni")
_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.propagate = False

# Constants
EXECUTED_FOR_COUNTS = {1, 5}  # status ids considered "executed" (passed=1, failed=5)
DEFAULT_DAYS = 7
MAX_NETWORK_WORKERS = 8
REQUEST_RETRIES = 3
REQUEST_BACKOFF_FACTOR = 0.5


# -------------------------
# Utilities
# -------------------------
def ensure_int(v: object) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def _get_local_tz():
    try:
        from zoneinfo import ZoneInfo  # type: ignore
    except Exception:
        ZoneInfo = None
    if LOCAL_TZ_NAME and ZoneInfo:
        try:
            return ZoneInfo(LOCAL_TZ_NAME)
        except Exception:
            logger.warning("Failed to load ZoneInfo(%s); falling back to fixed offset", LOCAL_TZ_NAME)
    try:
        offset_hours = int(LOCAL_TZ_OFFSET_HOURS)
    except Exception:
        offset_hours = 0
    return timezone(timedelta(hours=offset_hours))


LOCAL_TZ = _get_local_tz()


# -------------------------
# Session with retries/backoff
# -------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    s.auth = (USERNAME, API_KEY)
    s.headers.update({"Accept": "application/json"})
    retries = Retry(
        total=REQUEST_RETRIES,
        backoff_factor=REQUEST_BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PUT", "DELETE", "HEAD"])
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


# -------------------------
# HTTP helpers
# -------------------------
def _safe_json_resp(resp: requests.Response) -> Optional[Any]:
    try:
        return resp.json()
    except Exception:
        logger.debug("Response not JSON (truncated): %s", getattr(resp, "text", "")[:200])
        return None


def fetch_results_for_run(session: requests.Session, run_id: int, timeout: int = 30) -> List[Dict[str, Any]]:
    url = f"{TESTRAIL_URL}/index.php?/api/v2/get_results_for_run/{run_id}"
    try:
        resp = session.get(url, timeout=timeout)
    except Exception as e:
        logger.warning("HTTP error fetching results for run %s: %s", run_id, e)
        return []
    if resp.status_code != 200:
        logger.warning("get_results_for_run returned %s for run %s", resp.status_code, run_id)
        return []
    data = _safe_json_resp(resp)
    if data is None:
        return []
    if isinstance(data, dict):
        return data.get("results", []) or []
    if isinstance(data, list):
        return data
    logger.warning("Unexpected JSON shape for results of run %s", run_id)
    return []


def fetch_run_meta(session: requests.Session, run_id: int, timeout: int = 15) -> Dict[str, Any]:
    url = f"{TESTRAIL_URL}/index.php?/api/v2/get_run/{run_id}"
    try:
        resp = session.get(url, timeout=timeout)
    except Exception as e:
        logger.warning("HTTP error fetching run meta for %s: %s", run_id, e)
        return {}
    if resp.status_code != 200:
        logger.warning("get_run returned %s for run %s", resp.status_code, run_id)
        return {}
    data = _safe_json_resp(resp)
    if isinstance(data, dict):
        return data
    logger.warning("Unexpected JSON shape for run meta %s", run_id)
    return {}


def fetch_plan_meta(session: requests.Session, plan_id: int, timeout: int = 20) -> Dict[str, Any]:
    url = f"{TESTRAIL_URL}/index.php?/api/v2/get_plan/{plan_id}"
    try:
        resp = session.get(url, timeout=timeout)
    except Exception as e:
        logger.warning("HTTP error fetching plan %s: %s", plan_id, e)
        return {}
    if resp.status_code != 200:
        logger.warning("get_plan returned %s for plan %s", resp.status_code, plan_id)
        return {}
    data = _safe_json_resp(resp)
    if isinstance(data, dict):
        return data
    logger.warning("Unexpected JSON shape for plan %s", plan_id)
    return {}


def fetch_plan_runs(session: requests.Session, plan_id: int, timeout: int = 20) -> List[int]:
    """
    Fetch a TestRail plan and return its run IDs.
    Uses get_plan/{plan_id} and extracts entry runs.
    """
    plan = fetch_plan_meta(session, plan_id, timeout=timeout)
    if not plan:
        return []
    runs: List[int] = []
    entries = plan.get("entries") or []
    for entry in entries:
        entry_runs = entry.get("runs") or []
        for r in entry_runs:
            rid = r.get("id") or r.get("run_id")
            if rid is not None:
                try:
                    runs.append(int(rid))
                except Exception:
                    continue
    runs = sorted(set(runs))
    logger.debug("Plan %s contains runs: %s", plan_id, runs)
    return runs


# -------------------------
# Fetch orchestration (global)
# -------------------------
def fetch_all_results(run_ids: Iterable[int], max_workers: int = MAX_NETWORK_WORKERS) -> Tuple[Dict[Tuple[int, int], List[Tuple[int, int]]], int]:
    session = make_session()
    tests_map: Dict[Tuple[int, int], List[Tuple[int, int]]] = defaultdict(list)
    total = 0
    run_ids_list = list(run_ids)
    if not run_ids_list:
        logger.warning("RUN_IDS is empty in config")
        return tests_map, 0

    logger.info("Fetching results for %d runs using up to %d workers", len(run_ids_list), max_workers)
    with ThreadPoolExecutor(max_workers=min(max_workers, max(1, len(run_ids_list)))) as ex:
        future_to_run = {ex.submit(fetch_results_for_run, session, rid): rid for rid in run_ids_list}
        for fut in as_completed(future_to_run):
            rid = future_to_run[fut]
            try:
                results = fut.result()
            except Exception as e:
                logger.warning("Exception fetching results for run %s: %s", rid, e)
                results = []
            total += len(results)
            for r in results:
                test_id = r.get("test_id")
                ts = r.get("created_on")
                sid = r.get("status_id")
                if test_id is None or not isinstance(ts, (int, float)) or ts <= 0:
                    continue
                key = (int(rid), int(test_id))
                tests_map[key].append((int(ts), ensure_int(sid)))

    logger.info("Total raw results fetched across runs: %d", total)
    logger.info("Unique test keys found: %d", len(tests_map))
    for k in tests_map:
        tests_map[k].sort(key=lambda x: x[0])
    return tests_map, total


# -------------------------
# Plan-scoped fetch + per-plan JSON generation
# -------------------------
def fetch_results_for_runs_batch(session: requests.Session,
                                 run_ids: Iterable[int],
                                 max_workers: int = MAX_NETWORK_WORKERS) -> Tuple[Dict[Tuple[int, int], List[Tuple[int, int]]], int]:
    """
    Fetch get_results_for_run for the provided run_ids in parallel and return a tests_map
    keyed by (run_id, test_id) -> list[(ts, status_id)] and total results count.
    """
    tests_map: Dict[Tuple[int, int], List[Tuple[int, int]]] = defaultdict(list)
    total = 0
    run_ids_list = [int(r) for r in run_ids] if run_ids else []
    if not run_ids_list:
        return tests_map, 0

    logger.info("Fetching %d run(s) results for plan with up to %d workers", len(run_ids_list), min(max_workers, len(run_ids_list)))
    with ThreadPoolExecutor(max_workers=min(max_workers, max(1, len(run_ids_list)))) as ex:
        future_to_run = {ex.submit(fetch_results_for_run, session, rid): rid for rid in run_ids_list}
        for fut in as_completed(future_to_run):
            rid = future_to_run[fut]
            try:
                results = fut.result()
            except Exception as e:
                logger.warning("Exception fetching results for run %s: %s", rid, e)
                results = []
            total += len(results)
            for r in results:
                test_id = r.get("test_id")
                ts = r.get("created_on")
                sid = r.get("status_id")
                if test_id is None or not isinstance(ts, (int, float)) or ts <= 0:
                    continue
                key = (int(rid), int(test_id))
                tests_map[key].append((int(ts), ensure_int(sid)))
    for k in tests_map:
        tests_map[k].sort(key=lambda x: x[0])
    return tests_map, total


# -------------------------
# Series computation
# -------------------------
def compute_executed_series(tests_map: Dict[Tuple[int, int], List[Tuple[int, int]]],
                            days: int = DEFAULT_DAYS,
                            local_tz: Optional[timezone] = None) -> Tuple[List[Any], List[int], List[int]]:
    tz = local_tz or LOCAL_TZ
    now_local = datetime.now(timezone.utc).astimezone(tz)
    ordered_days = [now_local.date() - timedelta(days=(days - 1 - i)) for i in range(days)]

    day_start_utc: List[int] = []
    day_end_utc: List[int] = []
    for d in ordered_days:
        local_start = datetime.combine(d, dtime.min).replace(tzinfo=tz)
        local_end = datetime.combine(d, dtime.max).replace(tzinfo=tz)
        day_start_utc.append(int(local_start.astimezone(timezone.utc).timestamp()))
        day_end_utc.append(int(local_end.astimezone(timezone.utc).timestamp()))

    first_executed_day_idx: Dict[Tuple[int, int], Optional[int]] = {}
    for (run_id, test_id), timeline in tests_map.items():
        first_idx = None
        for ts, st in timeline:
            if st in EXECUTED_FOR_COUNTS:
                for day_idx, (ds, de) in enumerate(zip(day_start_utc, day_end_utc)):
                    if ds <= ts <= de:
                        first_idx = day_idx
                        break
                if first_idx is None:
                    if ts < day_start_utc[0]:
                        first_idx = -1
                    else:
                        first_idx = len(ordered_days) - 1
                break
        first_executed_day_idx[(run_id, test_id)] = first_idx

    cumulative_per_day = [0] * len(ordered_days)
    for first_idx in first_executed_day_idx.values():
        if first_idx is None:
            continue
        start = 0 if first_idx == -1 else first_idx
        for di in range(start, len(ordered_days)):
            cumulative_per_day[di] += 1

    daily_presence = [0] * len(ordered_days)
    for first_idx in first_executed_day_idx.values():
        if first_idx is None or first_idx == -1:
            continue
        daily_presence[first_idx] += 1

    for i in range(1, len(cumulative_per_day)):
        if cumulative_per_day[i] < cumulative_per_day[i - 1]:
            cumulative_per_day[i] = cumulative_per_day[i - 1]

    return ordered_days, daily_presence, cumulative_per_day


# -------------------------
# Summaries
# -------------------------
def build_rows_and_grand_from_tests_map_with_metadata(tests_map: Dict[Tuple[int, int], List[Tuple[int, int]]],
                                                      session: Optional[requests.Session] = None) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    if session is None:
        session = make_session()

    per_run_testids = defaultdict(set)
    last_status = {}
    for (run_id, test_id), timeline in tests_map.items():
        per_run_testids[int(run_id)].add(int(test_id))
        if timeline:
            last_status[(int(run_id), int(test_id))] = timeline[-1][1]

    rows: List[Dict[str, Any]] = []
    grand = {"Planned": 0, "Executed": 0, "Not Executed": 0, "Passed": 0, "Failed": 0}

    run_ids_all = sorted(set(list(per_run_testids.keys()) + [int(r) for r in RUN_IDS]))
    with ThreadPoolExecutor(max_workers=min(MAX_NETWORK_WORKERS, max(1, len(run_ids_all)))) as ex:
        future_map = {ex.submit(fetch_run_meta, session, rid): rid for rid in run_ids_all}
        meta_store: Dict[int, Dict[str, Any]] = {}
        for fut in as_completed(future_map):
            rid = future_map[fut]
            try:
                meta_store[rid] = fut.result()
            except Exception:
                meta_store[rid] = {}
                logger.debug("get_run failed for %s", rid)

    for run_id in run_ids_all:
        meta = meta_store.get(run_id, {}) or {}
        run_name = None
        configuration = None
        meta_passed = meta_blocked = meta_untested = meta_retest = meta_failed = None

        if meta:
            run_name = meta.get("name") or meta.get("description") or None
            configuration = meta.get("configuration") or meta.get("config") or None
            try:
                meta_passed = int(meta.get("passed_count", 0))
                meta_blocked = int(meta.get("blocked_count", 0))
                meta_untested = int(meta.get("untested_count", 0))
                meta_retest = int(meta.get("retest_count", 0))
                meta_failed = int(meta.get("failed_count", 0))
            except Exception:
                meta_passed = meta_blocked = meta_untested = meta_retest = meta_failed = None

        if not configuration and isinstance(run_name, str) and "(" in run_name and ")" in run_name:
            try:
                name_part, par = run_name.split("(", 1)
                par = par.rsplit(")", 1)[0].strip()
                configuration = par or None
                run_name = name_part.strip() or run_name
            except Exception:
                pass

        run_label = run_name or f"Run {run_id}"

        if meta_passed is not None:
            planned = (meta_passed + (meta_blocked or 0) + (meta_untested or 0) + (meta_retest or 0) + (meta_failed or 0))
            passed = meta_passed
            failed = meta_failed or 0
            executed = passed + failed
            not_executed = (meta_blocked or 0) + (meta_untested or 0)
        else:
            testids = sorted(per_run_testids.get(run_id, []))
            planned = len(testids)
            executed = passed = failed = 0
            for tid in testids:
                st = last_status.get((run_id, tid))
                if st == 1:
                    passed += 1
                    executed += 1
                elif st == 5:
                    failed += 1
                    executed += 1
            not_executed = planned - executed

        rows.append({
            "run_id": run_id,
            "run_label": run_label,
            "run_name": run_name,
            "configuration": configuration,
            "planned": planned,
            "executed": executed,
            "not_executed": not_executed,
            "passed": passed,
            "failed": failed
        })

        grand["Planned"] += planned
        grand["Executed"] += executed
        grand["Not Executed"] += not_executed
        grand["Passed"] += passed
        grand["Failed"] += failed

    rows.sort(key=lambda r: r["run_id"])
    return rows, grand


# -------------------------
# JSON writer (atomic)
# -------------------------
def _atomic_write_json(path: Path, obj: Any) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(obj, fh, ensure_ascii=False, indent=2)
            fh.flush()
        tmp.replace(path)
        return True
    except Exception as e:
        logger.exception("Failed atomic write to %s: %s", path, e)
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


def save_json_output(path: Path,
                     generated_for_date: str,
                     dates: List[datetime.date],
                     daily_results: List[int],
                     cumulative: List[int],
                     rows: Optional[List[Dict[str, Any]]] = None,
                     grand: Optional[Dict[str, int]] = None,
                     source_discrepancy: int = 0,
                     extras: Optional[Dict[str, Any]] = None) -> bool:
    try:
        payload = {
            "generated_for_date": generated_for_date,
            "dates": [d.strftime("%Y-%m-%d") for d in dates],
            "daily_results": daily_results,
            "cumulative": cumulative,
            "source_discrepancy": int(source_discrepancy)
        }
        if rows is not None:
            payload["rows"] = rows
        if grand is not None:
            payload["grand"] = grand
        if extras:
            payload.update(extras)
        return _atomic_write_json(path, payload)
    except Exception as e:
        logger.exception("Failed to prepare JSON output %s: %s", path, e)
        return False


# -------------------------
# Normalize plan ids
# -------------------------
def _normalize_plan_ids() -> List[int]:
    plan_ids: List[int] = []
    try:
        if CHART_PLAN_SETS:
            for s in CHART_PLAN_SETS:
                try:
                    for p in s:
                        plan_ids.append(int(p))
                except Exception:
                    continue
        elif CHART_PLAN_IDS:
            plan_ids = [int(p) for p in CHART_PLAN_IDS]
    except Exception:
        return []
    seen: Set[int] = set()
    out: List[int] = []
    for p in plan_ids:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


# -------------------------
# Per-plan JSON generation (fetching plan runs then results)
# -------------------------
def write_per_plan_jsons(base_out_dir: Path,
                         global_tests_map: Dict[Tuple[int, int], List[Tuple[int, int]]],
                         generated_for_date: str,
                         days: int,
                         session: requests.Session) -> List[Path]:
    """
    For each plan id configured:
      - fetch plan runs
      - fetch results for those runs (fresh)
      - compute per-plan series using the same `days` window
      - build plan rows/grand and write results_plan_<PLANID>.json (includes plan_id and plan_name)
    Returns list of written Paths.
    """
    written: List[Path] = []
    plan_ids = _normalize_plan_ids()
    if not plan_ids:
        logger.debug("No CHART_PLAN_IDS or CHART_PLAN_SETS configured; skipping per-plan JSON generation")
        return written

    logger.info("Generating per-plan JSON for plan IDs: %s", plan_ids)
    for plan_id in plan_ids:
        try:
            plan_runs = fetch_plan_runs(session, plan_id)
            if not plan_runs:
                logger.warning("Plan %s contains no runs (or failed to fetch); skipping", plan_id)
                continue

            # Fetch plan-run results explicitly (do not trust global_tests_map to include all runs)
            plan_tests_map, plan_total = fetch_results_for_runs_batch(session, plan_runs, max_workers=min(MAX_NETWORK_WORKERS, len(plan_runs)))
            if not plan_tests_map:
                logger.warning("No results returned for plan %s runs %s; skipping", plan_id, plan_runs)
                continue

            # compute series for this plan using the same 'days' window
            ordered_days, daily_presence, cumulative_series = compute_executed_series(plan_tests_map, days=days, local_tz=LOCAL_TZ)

            # build rows/grand for this plan
            rows_plan, grand_plan = build_rows_and_grand_from_tests_map_with_metadata(plan_tests_map, session=session)

            # reconcile metadata-only executed counts for plan
            meta_executed = grand_plan.get("Executed", 0)
            current_last = cumulative_series[-1] if cumulative_series else 0
            missing_from_meta = max(0, meta_executed - current_last)
            if missing_from_meta > 0:
                for i in range(len(cumulative_series)):
                    cumulative_series[i] += missing_from_meta
            for i in range(1, len(cumulative_series)):
                if cumulative_series[i] < cumulative_series[i - 1]:
                    cumulative_series[i] = cumulative_series[i - 1]

            # fetch plan metadata to get readable plan name
            plan_meta = fetch_plan_meta(session, plan_id)
            plan_name = None
            if isinstance(plan_meta, dict):
                plan_name = plan_meta.get("name") or plan_meta.get("description") or None

            out_name = base_out_dir / f"results_plan_{plan_id}.json"
            extras = {"plan_id": int(plan_id)}
            if plan_name:
                extras["plan_name"] = plan_name

            ok = save_json_output(out_name,
                                  generated_for_date,
                                  ordered_days,
                                  daily_presence,
                                  cumulative_series,
                                  rows=rows_plan,
                                  grand=grand_plan,
                                  source_discrepancy=missing_from_meta,
                                  extras=extras)
            if ok:
                logger.info("Wrote per-plan results JSON: %s", out_name)
                written.append(out_name)
            else:
                logger.warning("Failed to write per-plan JSON for plan %s", plan_id)
        except Exception as e:
            logger.exception("Exception while processing plan %s: %s", plan_id, e)
            continue
    return written


# -------------------------
# Console helpers
# -------------------------
def print_run_summaries(rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        run_name = r.get("run_name") or r.get("run_label")
        config_val = r.get("configuration")
        display = f"{run_name} [{config_val}]" if config_val else f"{run_name}"
        planned = int(r.get("planned", 0))
        executed = int(r.get("executed", 0))
        passed = int(r.get("passed", 0))
        failed = int(r.get("failed", 0))
        unexecuted = int(r.get("not_executed", max(0, planned - executed)))
        print(f"{display}: Planned {planned} Executed {executed} Passed {passed} Failed {failed} Unexecuted {unexecuted}")


def print_daily_summary(ordered_days: List[datetime.date], executed_series: List[int], cumulative_series: List[int]) -> None:
    print("\nDate        DailyExecuted  CumulativeAsOfDay  UniqueExecutedResultsThatDay")
    for d, de, ca in zip(ordered_days, executed_series, cumulative_series):
        print(f"{d}   {de:<13} {ca:<17} {de}")
    print("====================================")
    print(f"TOTAL cumulative as of last day: {cumulative_series[-1] if cumulative_series else 0}")


# -------------------------
# CLI parsing & main flow
# -------------------------
def parse_cli(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch TestRail results and produce omni-style JSON (optionally per-plan).")
    parser.add_argument("--json-out", help="Write results JSON to this path", default="results.json")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Window in days for daily/cumulative series")
    parser.add_argument("--dry-run", action="store_true", help="Do everything except write files")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging (DEBUG)")
    parser.add_argument("--workers", type=int, default=MAX_NETWORK_WORKERS, help="Max network worker threads")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_cli(argv)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    logger.debug("Args: %s", args)

    session = make_session()

    try:
        tests_map, total_fetched = fetch_all_results(RUN_IDS, max_workers=args.workers)
    except Exception as e:
        logger.exception("Failed to fetch TestRail results: %s", e)
        return 2

    ordered_days, executed_series, cumulative_series = compute_executed_series(tests_map, days=args.days)
    rows, grand = build_rows_and_grand_from_tests_map_with_metadata(tests_map, session=session)

    # Reconcile metadata-only executed counts (global)
    meta_executed = grand.get("Executed", 0)
    current_last = cumulative_series[-1] if cumulative_series else 0
    missing_from_meta = max(0, meta_executed - current_last)
    if missing_from_meta > 0:
        for i in range(len(cumulative_series)):
            cumulative_series[i] += missing_from_meta
    for i in range(1, len(cumulative_series)):
        if cumulative_series[i] < cumulative_series[i - 1]:
            cumulative_series[i] = cumulative_series[i - 1]

    generated_for_date = ordered_days[-1].strftime("%Y-%m-%d")
    out_json = Path(args.json_out)

    produced_files: Dict[str, List[str]] = {"global": [], "per_plan": []}

    if args.dry_run:
        logger.info("Dry-run enabled; skipping JSON writes.")
    else:
        ok = save_json_output(out_json, generated_for_date, ordered_days, executed_series, cumulative_series,
                              rows=rows, grand=grand, source_discrepancy=missing_from_meta)
        if not ok:
            logger.error("Failed to persist global results JSON; aborting.")
            return 3
        produced_files["global"].append(str(out_json))

        # per-plan JSON generation (fetch per-plan runs/results)
        try:
            written = write_per_plan_jsons(out_json.parent, tests_map, generated_for_date, args.days, session)
            produced_files["per_plan"].extend([str(p) for p in written])
        except Exception:
            logger.exception("Per-plan JSON generation raised an unexpected exception; continuing.")

        # write a simple manifest of produced files
        try:
            manifest = out_json.parent / "produced_files.json"
            _atomic_write_json(manifest, produced_files)
            logger.info("Wrote manifest: %s", manifest)
        except Exception:
            logger.exception("Failed to write produced_files.json manifest")

    # Console summaries
    print_run_summaries(rows)
    print_daily_summary(ordered_days, executed_series, cumulative_series)

    logger.info("Generated %d milestone rows; date: %s", len(rows), generated_for_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))