#!/usr/bin/env python3
"""
table_milestones.py — fetch/export TestRail milestones (omni-like JSON) + HTML table builder

Improvements applied:
- Uses logging instead of prints; respects --verbose flag
- Safe, lazy config loading from config.py or environment variables
- Narrow exception handling around network I/O and JSON parsing
- Streams JSON to disk using json.dump to avoid large in-memory strings
- Normalizes timestamp and ID types before comparisons
- Safer CLI parsing (parse_args(argv) and main(sys.argv[1:]))
- Optional parallel fetching using ThreadPoolExecutor with configurable workers
- Defensive handling of malformed milestone entries
"""

from __future__ import annotations

import sys
import json
import argparse
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

# lazy import requests only when needed
try:
    import requests  # type: ignore
    from requests import RequestException  # type: ignore
except Exception:
    requests = None
    RequestException = Exception  # fallback for typing only

# Constants
DEFAULT_TIMEOUT = 15
JSON_INDENT = 2
DATE_FMT = "%Y-%m-%d"
DEFAULT_WORKERS = 6

# Configure module logger (main can adjust level via --verbose)
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(handler)
logger.propagate = False
logger.setLevel(logging.INFO)


def _load_config() -> Tuple[Optional[str], Optional[str], Optional[str], Optional[List[int]]]:
    """
    Load TestRail configuration lazily:
      - Prefer attributes from config.py if importable
      - Fall back to environment variables
    Returns (TESTRAIL_URL, USERNAME, API_KEY, MILESTONE_IDS)
    """
    TESTRAIL_URL = None
    USERNAME = None
    API_KEY = None
    MILESTONE_IDS = None

    try:
        import config  # type: ignore
    except (ImportError, ModuleNotFoundError):
        config = None

    if config is not None:
        TESTRAIL_URL = getattr(config, "TESTRAIL_URL", None)
        USERNAME = getattr(config, "USERNAME", None)
        API_KEY = getattr(config, "API_KEY", None)
        MILESTONE_IDS = getattr(config, "MILESTONE_IDS", None)

    # environment fallback
    TESTRAIL_URL = TESTRAIL_URL or os.getenv("TESTRAIL_URL")
    USERNAME = USERNAME or os.getenv("USERNAME") or os.getenv("TESTRAIL_USERNAME")
    API_KEY = API_KEY or os.getenv("API_KEY") or os.getenv("TESTRAIL_API_KEY")

    # Normalize MILESTONE_IDS into a list[int] if possible
    if MILESTONE_IDS is None:
        env_ids = os.getenv("MILESTONE_IDS")
        if env_ids:
            try:
                # accept comma-separated values
                MILESTONE_IDS = [int(x.strip()) for x in env_ids.split(",") if x.strip()]
            except Exception:
                MILESTONE_IDS = None

    if isinstance(MILESTONE_IDS, (list, tuple)):
        try:
            MILESTONE_IDS = [int(x) for x in MILESTONE_IDS]
        except Exception:
            MILESTONE_IDS = None

    return TESTRAIL_URL, USERNAME, API_KEY, MILESTONE_IDS


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    if v is None:
        return default
    try:
        if isinstance(v, bool):
            return int(v)
        return int(str(v))
    except (ValueError, TypeError):
        return default


def _format_ts(ts: Optional[int]) -> str:
    """
    Format epoch seconds to YYYY-MM-DD in UTC. Treat None as TBD.
    Note: 0 is treated as 1970-01-01 unless treated as invalid upstream; preserve 0.
    """
    if ts is None:
        return "TBD"
    try:
        dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        return dt.strftime(DATE_FMT)
    except Exception:
        return "TBD"


def _classify_status_from_api(m: Dict[str, Any]) -> str:
    """
    Determine a display status using API fields:
      - 'Completed' if is_completed is truthy
      - If start_on is missing -> 'Planned'
      - If start_on > now -> 'Planned'
      - If start_on <= now -> 'In progress'
    Handles string/int values for start_on safely.
    """
    try:
        if m.get("is_completed"):
            return "Completed"
    except Exception:
        # defensive: if is_completed exists but malformed, ignore and continue
        pass

    start_raw = m.get("start_on")
    start = _safe_int(start_raw, None)
    if start is None:
        return "Planned"

    now_ts = int(datetime.now(timezone.utc).timestamp())
    try:
        if start > now_ts:
            return "Planned"
        return "In progress"
    except Exception:
        return "Planned"


def _get_milestone_once(url: str, auth: Tuple[str, str], mid: int, timeout: int = DEFAULT_TIMEOUT) -> Optional[Dict[str, Any]]:
    """
    Fetch a single milestone via TestRail API. Returns parsed JSON or None.
    Exceptions are logged at debug level; user-facing errors use logger.error sparingly.
    """
    if requests is None:
        logger.error("requests module not available; cannot fetch milestones")
        return None

    endpoint = f"{url}/index.php?/api/v2/get_milestone/{mid}"
    try:
        resp = requests.get(endpoint, auth=auth, timeout=timeout)
        resp.raise_for_status()
        try:
            data = resp.json()
            return data
        except ValueError as ve:
            logger.debug("Invalid JSON for milestone %s: %s", mid, ve)
            return None
    except RequestException as e:
        logger.debug("HTTP error fetching milestone %s: %s", mid, e)
        return None


def fetch_milestones_map(concurrent: bool = True, max_workers: int = DEFAULT_WORKERS) -> Dict[str, Dict[str, str]]:
    """
    Fetch milestones for IDs provided via config or env.
    Returns mapping str(mid) -> { name, status, start, due, is_completed_raw }
    Uses optional concurrency to speed up network IO.
    """
    TESTRAIL_URL, USERNAME, API_KEY, MILESTONE_IDS = _load_config()

    if not MILESTONE_IDS:
        logger.warning("MILESTONE_IDS is not set or invalid; nothing to fetch")
        return {}

    if not (TESTRAIL_URL and USERNAME and API_KEY):
        logger.warning("TestRail credentials (URL/USERNAME/API_KEY) missing; cannot fetch milestones")
        return {}

    auth = (USERNAME, API_KEY)
    result: Dict[str, Dict[str, str]] = {}

    if concurrent and len(MILESTONE_IDS) > 1:
        # parallel fetches
        futures = []
        with ThreadPoolExecutor(max_workers=min(max_workers, len(MILESTONE_IDS))) as ex:
            for mid in MILESTONE_IDS:
                futures.append(ex.submit(_get_milestone_once, TESTRAIL_URL, auth, int(mid)))
            for fut, mid in zip(futures, MILESTONE_IDS):
                try:
                    data = fut.result()
                except Exception as e:
                    logger.debug("Exception while fetching milestone %s: %s", mid, e)
                    data = None
                if not data:
                    continue
                try:
                    is_completed_raw = bool(data.get("is_completed"))
                    status = _classify_status_from_api(data)
                    name = data.get("name") or "Unnamed"
                    start = _format_ts(_safe_int(data.get("start_on"), None))
                    due = _format_ts(_safe_int(data.get("due_on"), None))
                    result[str(mid)] = {
                        "name": str(name),
                        "status": status,
                        "start": start,
                        "due": due,
                        "is_completed_raw": "true" if is_completed_raw else "false",
                    }
                except Exception as e:
                    logger.debug("Malformed milestone data for %s: %s", mid, e)
    else:
        # sequential fetch
        for mid in MILESTONE_IDS:
            data = _get_milestone_once(TESTRAIL_URL, auth, int(mid))
            if not data:
                continue
            try:
                is_completed_raw = bool(data.get("is_completed"))
                status = _classify_status_from_api(data)
                name = data.get("name") or "Unnamed"
                start = _format_ts(_safe_int(data.get("start_on"), None))
                due = _format_ts(_safe_int(data.get("due_on"), None))
                result[str(mid)] = {
                    "name": str(name),
                    "status": status,
                    "start": start,
                    "due": due,
                    "is_completed_raw": "true" if is_completed_raw else "false",
                }
            except Exception as e:
                logger.debug("Malformed milestone data for %s: %s", mid, e)

    return result


def _parse_start_date(s: str) -> Optional[datetime]:
    if not s or s == "TBD":
        return None
    try:
        return datetime.strptime(s, DATE_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def build_rows_from_map(milestone_map: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Build sorted rows list from milestone_map.
    Sort by start ascending; missing start placed last; numeric id tie-breaker.
    """
    def _key(item):
        mid, data = item
        start_dt = _parse_start_date(data.get("start", "TBD"))
        has_start_flag = 0 if start_dt is not None else 1
        try:
            nid = int(mid)
        except Exception:
            nid = mid
        return (has_start_flag, start_dt or datetime.max.replace(tzinfo=timezone.utc), nid)

    items = sorted(milestone_map.items(), key=_key)
    rows: List[Dict[str, str]] = []
    for mid, m in items:
        rows.append({
            "id": str(mid),
            "name": m.get("name", "") or "",
            "status": m.get("status", "") or "",
            "start": m.get("start", "TBD") or "TBD",
            "due": m.get("due", "TBD") or "TBD",
            "is_completed_raw": m.get("is_completed_raw", "false")
        })
    return rows


def write_omni_json(path: Path, generated_for_date: str, rows: List[Dict[str, str]], milestone_map: Dict[str, Dict[str, str]]) -> bool:
    """
    Write the omni-like JSON file to disk using streaming json.dump.
    Returns True on success, False on failure.
    """
    payload = {
        "generated_for_date": generated_for_date,
        "rows": rows,
        "milestone_map": milestone_map
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=JSON_INDENT)
        logger.info("Milestone JSON written to: %s", path)
        # tighten permissions if needed (best effort)
        try:
            os.chmod(path, 0o644)
        except Exception:
            # not fatal; skip if not supported on platform
            logger.debug("Could not change file permissions for %s", path)
        return True
    except OSError as e:
        logger.error("Failed to write JSON %s: %s", path, e)
        return False


def build_console_preview(rows: List[Dict[str, str]]) -> None:
    if not rows:
        logger.warning("No milestones could be retrieved.")
        return
    print(f"{'Name':<30} {'Status':<12} {'Start':<12} {'Due':<12}")
    print("-" * 70)
    for r in rows:
        name = (r.get("name") or "")[:30]
        status = r.get("status") or ""
        start = r.get("start") or "TBD"
        due = r.get("due") or "TBD"
        print(f"{name:<30} {status:<12} {start:<12} {due:<12}")


def build_milestones_table(milestone_map: Dict[str, Dict[str, str]]) -> str:
    """
    Build an HTML table for the milestone_map.
    """
    if not isinstance(milestone_map, dict):
        milestone_map = {}

    rows = build_rows_from_map(milestone_map)

    out: List[str] = []
    out.append("<table class='milestone-table'>")
    out.append("<tr><th>Name</th><th>Status</th><th>Start</th><th>Due</th></tr>")

    if not rows:
        out.append("<tr><td colspan='4' style='color:#666'>No milestone data available</td></tr>")
        out.append("</table>")
        return "".join(out)

    for r in rows:
        name = html_escape_str(r.get("name", ""))
        status = r.get("status", "") or ""
        start = html_escape_str(r.get("start", "TBD"))
        due = html_escape_str(r.get("due", "TBD"))

        if status == "Completed":
            row_class = "milestone-completed"
            status_class = ""
        elif status == "In progress":
            row_class = "milestone-inprogress"
            status_class = "milestone-status-inprogress"
        else:
            row_class = ""
            status_class = ""

        out.append(
            f"<tr class='{row_class}'>"
            f"<td>{name}</td>"
            f"<td class='{status_class}'>{html_escape_str(status)}</td>"
            f"<td>{start}</td>"
            f"<td>{due}</td>"
            f"</tr>"
        )

    out.append("</table>")
    return "".join(out)


def html_escape_str(s: Any) -> str:
    try:
        import html as _html
        return _html.escape(str(s))
    except Exception:
        return str(s)


def parse_cli(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch milestones and export omni-like JSON")
    parser.add_argument("--json-out", help="Write milestone data to JSON file", default="milestone_data.json")
    parser.add_argument("--no-concurrency", action="store_true", help="Disable concurrent HTTP fetches")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max workers for concurrent fetch")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_cli(argv)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    logger.debug("Arguments: %s", args)

    out_path = Path(args.json_out)

    milestone_map = fetch_milestones_map(concurrent=not args.no_concurrency, max_workers=args.workers)
    rows = build_rows_from_map(milestone_map)
    generated_for_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    build_console_preview(rows)
    ok = write_omni_json(out_path, generated_for_date, rows, milestone_map)
    if not ok:
        return 3

    logger.info("Generated %d milestone rows; date: %s", len(rows), generated_for_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))