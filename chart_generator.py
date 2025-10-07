#!/usr/bin/env python3
"""
chart_generator.py

Generate PNG charts from omni-style JSON.

Features:
- Programmatic API: make_chart_png_bytes(dates, daily_values, cumulative_values, width=10, height=5, dpi=140, title=None) -> bytes
- CLI:
  --from-json PATH    read JSON and validate required schema, then write PNG
  --out PATH          explicit output path (overrides default)
  --plan-id INT       optional plan id used in filename when provided
  --title TEXT        optional explicit title (overrides JSON metadata)
- Validation of input JSON schema with clear errors
- Atomic PNG writes (write to temp then rename)
- Filenames:
  - If --plan-id provided: output/delta_<PLANID>_<YYYYMMDD>_<HHMMSS>.png
  - Else if --out provided: use that
  - Else: output/delta_<YYYYMMDD>_<HHMMSS>.png
"""
from __future__ import annotations

import os
import json
import tempfile
from io import BytesIO
from datetime import datetime
from pathlib import Path
from typing import Sequence, List, Optional, Tuple, Any
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def make_chart_png_bytes(dates: Sequence[str],
                         daily_values: Sequence[int],
                         cumulative_values: Sequence[int],
                         width: float = 10,
                         height: float = 5,
                         dpi: int = 140,
                         title: Optional[str] = None) -> bytes:
    if not (len(dates) == len(daily_values) == len(cumulative_values)):
        raise ValueError("dates, daily_values and cumulative_values must have the same length")
    x = list(range(len(dates)))
    fig, ax = plt.subplots(figsize=(width, height))
    if title:
        ax.set_title(title, fontsize=12, weight="bold")
    bars = ax.bar(x, daily_values, color="#4C78A8", label="Daily executed (first-executed)")
    ax.plot(x, cumulative_values, color="#F58518", marker="o", linewidth=2, label="Cumulative (as of day)")
    ax.set_yticks([])
    for rect, val, xi, cumv in zip(bars, daily_values, x, cumulative_values):
        height_rect = rect.get_height()
        ax.annotate(f"{val}", xy=(rect.get_x() + rect.get_width() / 2, height_rect),
                    xytext=(0, 6), textcoords="offset points", ha="center", va="bottom",
                    fontsize=9, bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none"))
        ax.annotate(f"{cumv}", xy=(xi, cumv), xytext=(6, 2), textcoords="offset points",
                    ha="left", va="bottom", fontsize=9, bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none"))
    ax.set_xticks(x)
    ax.set_xticklabels(dates, rotation=45, ha="right")
    ax.legend(loc="upper left")
    ax.grid(axis="y", linestyle=":", alpha=0.25)
    plt.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=dpi)
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _make_output_path_png(plan_id: Optional[int] = None, out_override: Optional[Path] = None) -> Path:
    if out_override:
        return out_override
    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if plan_id is not None:
        filename = f"delta_{int(plan_id)}_{stamp}.png"
    else:
        filename = f"delta_{stamp}.png"
    return out_dir / filename


class ValidationError(Exception):
    pass


def validate_chart_json(obj: Any) -> Tuple[List[str], List[int], List[int]]:
    if not isinstance(obj, dict):
        raise ValidationError("JSON root must be an object")
    dates = obj.get("dates")
    daily = obj.get("daily_results") or obj.get("daily")
    cumulative = obj.get("cumulative") or obj.get("cumulative_results")
    if dates is None:
        raise ValidationError("Missing required key: dates")
    if daily is None:
        raise ValidationError("Missing required key: daily_results (or daily)")
    if cumulative is None:
        raise ValidationError("Missing required key: cumulative (or cumulative_results)")
    if not isinstance(dates, list):
        raise ValidationError("dates must be a list of YYYY-MM-DD strings")
    if not isinstance(daily, list) or not isinstance(cumulative, list):
        raise ValidationError("daily_results and cumulative must be lists of integers")
    if not (len(dates) == len(daily) == len(cumulative)):
        raise ValidationError("dates, daily_results and cumulative must be the same length")
    for i, d in enumerate(dates):
        if not isinstance(d, str):
            raise ValidationError(f"dates[{i}] is not a string")
    try:
        daily_ints = [int(x) for x in daily]
        cumulative_ints = [int(x) for x in cumulative]
    except Exception:
        raise ValidationError("daily_results and cumulative must contain integer-convertible values")
    for i in range(1, len(cumulative_ints)):
        if cumulative_ints[i] < cumulative_ints[i - 1]:
            raise ValidationError("cumulative series must be non-decreasing")
    return dates, daily_ints, cumulative_ints


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        Path(tmp).replace(path)
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _build_title_from_json_and_args(obj: dict, explicit_title: Optional[str], plan_id_arg: Optional[int]) -> Optional[str]:
    if explicit_title:
        return explicit_title
    plan_name = obj.get("plan_name")
    plan_id = obj.get("plan_id")
    if plan_id is None and plan_id_arg is not None:
        plan_id = int(plan_id_arg)
    title = None
    if plan_name and plan_id is not None:
        title = f"{plan_name}, plan #{int(plan_id)}"
    elif plan_name:
        title = f"{plan_name}"
    elif plan_id is not None:
        title = f"Plan #{int(plan_id)}"
    return title


def _cli_run_from_json(json_path: Path, out_path: Optional[Path], plan_id_arg: Optional[int], explicit_title: Optional[str]) -> int:
    try:
        obj = _read_json(json_path)
    except Exception as e:
        print(f"❌ Failed to read JSON {json_path}: {e}", file=sys.stderr)
        return 2
    try:
        dates, daily, cumulative = validate_chart_json(obj)
    except ValidationError as e:
        print(f"❌ Validation error for {json_path}: {e}", file=sys.stderr)
        return 3

    title = _build_title_from_json_and_args(obj if isinstance(obj, dict) else {}, explicit_title, plan_id_arg)
    try:
        png = make_chart_png_bytes(dates, daily, cumulative, title=title)
    except Exception as e:
        print(f"❌ Chart generation failed for {json_path}: {e}", file=sys.stderr)
        return 4

    target = _make_output_path_png(plan_id=plan_id_arg, out_override=out_path)
    try:
        _atomic_write_bytes(target, png)
    except Exception as e:
        print(f"❌ Failed to write PNG {target}: {e}", file=sys.stderr)
        return 5
    print(f"Saved chart to: {target}")
    return 0


def _cli_demo(out_path: Optional[Path], plan_id_arg: Optional[int], explicit_title: Optional[str]) -> int:
    dates = ["2025-10-01", "2025-10-02", "2025-10-03", "2025-10-04", "2025-10-05", "2025-10-06", "2025-10-07"]
    daily = [12, 9, 0, 3, 5, 2, 4]
    cumulative = [120, 129, 129, 132, 137, 139, 143]
    title = explicit_title or (f"Plan #{plan_id_arg}" if plan_id_arg is not None else None)
    png = make_chart_png_bytes(dates, daily, cumulative, title=title)
    target = _make_output_path_png(plan_id=plan_id_arg, out_override=out_path)
    try:
        _atomic_write_bytes(target, png)
    except Exception as e:
        print(f"❌ Failed to write demo PNG {target}: {e}", file=sys.stderr)
        return 6
    print(f"Saved chart to: {target}")
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Generate PNG chart from omni-style JSON")
    parser.add_argument("--from-json", help="JSON file with keys dates,daily_results,cumulative", default=None)
    parser.add_argument("--out", help="Explicit output path (overrides default timestamped filename)", default=None)
    parser.add_argument("--plan-id", type=int, help="Optional Test Plan ID to include in filename", default=None)
    parser.add_argument("--title", help="Optional explicit title to render on chart (overrides JSON metadata)", default=None)
    args = parser.parse_args(argv)

    out_path = Path(args.out) if args.out else None
    if args.from_json:
        return _cli_run_from_json(Path(args.from_json), out_path, args.plan_id, args.title)
    return _cli_demo(out_path, args.plan_id, args.title)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))