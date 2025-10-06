#!/usr/bin/env python3
"""
velocity_chart.py

Small, reusable chart generator for milestone velocity.

Public functions:
- generate_velocity_chart(rows, months=6, figsize=(8,4), dpi=150) -> bytes
  Returns PNG image bytes for the velocity chart computed from `rows`.

- save_velocity_chart(rows, path, months=6, figsize=(8,4), dpi=150) -> None
  Convenience wrapper that writes PNG to disk.

Expectations for `rows`:
- Iterable of dict-like objects with keys:
    - "is_completed_raw" -> "true" or "false" (or boolean)
    - "due" -> "YYYY-MM-DD" or "TBD" or empty
    - "start" -> "YYYY-MM-DD" or "TBD" or empty
  The generator treats a milestone as completed when is_completed_raw == "true" or True.
  The completion month is taken from the `due` field when completed, otherwise from `start`.
  If date parsing fails, the row is ignored for chart counts.
"""

from __future__ import annotations

import io
import math
from datetime import datetime, timezone
from typing import Iterable, Dict, Any, List, Tuple, Optional

# Matplotlib is required for rendering the chart; raise a clear error if missing.
try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
except Exception as exc:
    raise ImportError("matplotlib is required for velocity_chart.py (install python-matplotlib).") from exc


_DATE_FMT = "%Y-%m-%d"


def _parse_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    if not s or s.upper() == "TBD" or s == "—":
        return None
    try:
        return datetime.strptime(s, _DATE_FMT).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _month_key(dt: datetime) -> datetime:
    """Return a datetime anchored to first day of month in UTC for grouping."""
    return datetime(dt.year, dt.month, 1, tzinfo=timezone.utc)


def _collect_completed_counts(rows: Iterable[Dict[str, Any]], months: int) -> Tuple[List[datetime], List[int]]:
    """
    Build X (month start datetimes ascending) and Y (completed count) lists for the last `months` months.
    Completed milestone detection: is_completed_raw == "true" or True.
    Completion date prioritized from 'due', then 'start'.
    """
    now = datetime.now(timezone.utc)
    # build month starts from oldest to newest
    months_list: List[datetime] = []
    # compute first day of current month
    cur_month_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    for i in range(months - 1, -1, -1):
        # month offset backwards
        year = cur_month_start.year
        month = cur_month_start.month - i
        # normalize month/year
        while month <= 0:
            month += 12
            year -= 1
        months_list.append(datetime(year, month, 1, tzinfo=timezone.utc))

    # initialize counts
    counts = {m: 0 for m in months_list}

    for r in rows:
        try:
            is_completed = r.get("is_completed_raw", False)
            if isinstance(is_completed, str):
                is_completed_flag = is_completed.lower() == "true"
            else:
                is_completed_flag = bool(is_completed)
            if not is_completed_flag:
                continue
            # prefer due date for completion month, then start
            dt = _parse_date(r.get("due")) or _parse_date(r.get("start"))
            if not dt:
                continue
            mkey = _month_key(dt)
            # if mkey falls into our months_list range, increment
            if mkey in counts:
                counts[mkey] += 1
        except Exception:
            # ignore malformed rows
            continue

    x = sorted(counts.keys())
    y = [counts[k] for k in x]
    return x, y


def generate_velocity_chart(rows: Iterable[Dict[str, Any]],
                            months: int = 6,
                            figsize: Tuple[float, float] = (8.0, 4.0),
                            dpi: int = 150) -> bytes:
    """
    Generate a PNG bytes object with a simple velocity bar chart for the given rows.

    Parameters:
    - rows: iterable of milestone row dicts
    - months: how many months to include (default 6)
    - figsize: matplotlib figure size
    - dpi: output image DPI

    Returns:
    - PNG image bytes (bytes)
    """
    if months <= 0:
        raise ValueError("months must be > 0")

    x, y = _collect_completed_counts(rows, months)

    # Create figure
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    ax.bar(x, y, width=20, align="center", color="#4c72b0", edgecolor="#2a4a7a")

    # Format x-axis as Month Year
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")

    # Labels and grid
    ax.set_ylabel("Completed milestones")
    ax.set_title(f"Velocity — last {months} months")
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # Y-axis tidy limits
    max_y = max(y) if y else 0
    top = max(3, max_y + math.ceil(max_y * 0.25)) if max_y > 0 else 3
    ax.set_ylim(0, top)

    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", transparent=False)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def save_velocity_chart(rows: Iterable[Dict[str, Any]], path: str, months: int = 6,
                        figsize: Tuple[float, float] = (8.0, 4.0), dpi: int = 150) -> None:
    """
    Generate and save velocity chart PNG to `path`.
    """
    png = generate_velocity_chart(rows, months=months, figsize=figsize, dpi=dpi)
    with open(path, "wb") as fh:
        fh.write(png)