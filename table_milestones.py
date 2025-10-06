#!/usr/bin/env python3
"""
table_milestones.py — fetch/export TestRail milestones (omni-like JSON) + HTML table builder

- Stav Completed se bere z API pole is_completed.
- Exportuje: fetch_milestones_map(), build_rows_from_map(), build_milestones_table().
- CLI: py table_milestones.py (zapíše milestone_data.json).
"""

from __future__ import annotations

import sys
import json
import argparse
import requests
from datetime import datetime, timezone
import html
from pathlib import Path
from typing import Dict, List, Any, Optional

from config import TESTRAIL_URL, USERNAME, API_KEY, MILESTONE_IDS


def get_milestone(mid: int | str) -> Optional[Dict[str, Any]]:
    url = f"{TESTRAIL_URL}/index.php?/api/v2/get_milestone/{mid}"
    try:
        resp = requests.get(url, auth=(USERNAME, API_KEY), timeout=15)
        if resp.status_code != 200:
            print(f"❌ Failed to fetch milestone {mid} (status {resp.status_code})", file=sys.stderr)
            return None
        return resp.json()
    except Exception as e:
        print(f"❌ Error fetching milestone {mid}: {e}", file=sys.stderr)
        return None


def classify_status_from_api(m: Dict[str, Any]) -> str:
    """
    Determine display status using API fields.
    Completed is taken from m.get('is_completed') explicitly.
    If start_on is missing, treat as Planned.
    Otherwise use start_on relative to now to determine Planned or In progress.
    """
    if m.get("is_completed"):
        return "Completed"
    now_ts = int(datetime.now(timezone.utc).timestamp())
    start = m.get("start_on")
    if start is None:
        return "Planned"
    if start and start > now_ts:
        return "Planned"
    if start and start <= now_ts:
        return "In progress"
    return "Planned"


def format_ts(ts: Optional[int]) -> str:
    if not ts:
        return "TBD"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def fetch_milestones_map() -> Dict[str, Dict[str, str]]:
    """
    Fetch milestones for IDs in MILESTONE_IDS.
    Returns dict[id] -> { name, status, start, due, is_completed_raw }
    """
    if not MILESTONE_IDS or not isinstance(MILESTONE_IDS, list):
        print("⚠️ MILESTONE_IDS is not set or invalid in config.py", file=sys.stderr)
        return {}

    milestone_map: Dict[str, Dict[str, str]] = {}
    for mid in MILESTONE_IDS:
        m = get_milestone(mid)
        if not m:
            continue
        # Use is_completed from API directly
        is_completed_raw = bool(m.get("is_completed"))
        status = classify_status_from_api(m)
        name = m.get("name", "Unnamed")
        start = format_ts(m.get("start_on"))
        due = format_ts(m.get("due_on"))
        milestone_map[str(mid)] = {
            "name": name,
            "status": status,
            "start": start,
            "due": due,
            # keep raw indicator for debugging or future logic
            "is_completed_raw": "true" if is_completed_raw else "false"
        }
    return milestone_map


def _parse_start_date(s: str) -> Optional[datetime]:
    if not s or s == "TBD":
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def build_rows_from_map(milestone_map: Dict[str, Dict[str, str]]) -> List[Dict[str, str]]:
    """
    Build rows sorted by start ascending (missing start last).
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
            "name": m.get("name", ""),
            "status": m.get("status", ""),
            "start": m.get("start", "TBD"),
            "due": m.get("due", "TBD"),
            "is_completed_raw": m.get("is_completed_raw", "false")
        })
    return rows


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


def build_console_preview(rows: List[Dict[str, str]]) -> None:
    if not rows:
        print("⚠️ No milestones could be retrieved.", file=sys.stderr)
        return
    print(f"{'Name':<30} {'Status':<12} {'Start':<12} {'Due':<12}")
    print("-" * 70)
    for r in rows:
        print(f"{r['name'][:30]:<30} {r['status']:<12} {r['start']:<12} {r['due']:<12}")


def build_milestones_table(milestone_map: Dict[str, Dict[str, str]]) -> str:
    """
    Build HTML table and apply row classes:
      - Completed -> tr class='milestone-completed' (background #D3D3D3 via CSS)
      - In progress -> tr class='milestone-inprogress' (background #cccccc via CSS)
      - Status cell for In progress -> class='milestone-status-inprogress' (green bold)
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
        name = html.escape(str(r.get("name", "")))
        status = str(r.get("status", ""))
        start = html.escape(str(r.get("start", "TBD")))
        due = html.escape(str(r.get("due", "TBD")))

        # Decide classes strictly from status (which itself was derived from is_completed)
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
            f"<td class='{status_class}'>{html.escape(status)}</td>"
            f"<td>{start}</td>"
            f"<td>{due}</td>"
            f"</tr>"
        )

    out.append("</table>")
    return "".join(out)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Fetch milestones and export omni-like JSON")
    parser.add_argument("--json-out", help="Write milestone data to JSON file", default="milestone_data.json")
    args = parser.parse_args(argv[1:] if argv else None)

    out_path = Path(args.json_out)

    milestone_map = fetch_milestones_map()
    rows = build_rows_from_map(milestone_map)
    generated_for_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    build_console_preview(rows)
    ok = write_omni_json(out_path, generated_for_date, rows, milestone_map)
    if not ok:
        return 3

    print(f"\nℹ️ Generated {len(rows)} milestone rows; date: {generated_for_date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())