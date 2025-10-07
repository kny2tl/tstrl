#!/usr/bin/env python3
"""
chart_generator.py

Provides:
- make_chart_png_bytes(dates, daily_values, cumulative_values, width=10, height=5, dpi=140) -> bytes
- make_output_path_png(suffix: str | None = None) -> str

When run as a script it creates one or more output PNG files:
- If --from-json PATH is provided it reads JSON with keys:
    {"dates": ["YYYY-MM-DD", ...], "daily_results": [...], "cumulative": [...]}
  and writes a single PNG (or multiple PNGs if config.CHART_RUN_SETS is present).
- Otherwise it writes a demo chart to output/delta_<YYYYMMDD>_<HHMMSS>.png (and additional charts if config requests them).

Enhancement:
- Supports optional configuration in config.py to request charts for specific TestRail run groups.
  Suggested config variable names:
    - CHART_RUN_SETS: Optional[List[Sequence[int]]]  # list of run-id sequences; each set produces one chart
    - CHART_RUN_IDS: Optional[Sequence[int]]         # single sequence, backward-compatible shorthand

Behavior:
- If config.CHART_RUN_SETS exists, one chart is generated per set.
- If config.CHART_RUN_IDS exists (and CHART_RUN_SETS not present), a single chart is generated for that list.
- If no config is present, fallback to single chart behavior as before.
"""
from __future__ import annotations

from io import BytesIO
from datetime import datetime
from typing import Sequence, List, Optional, Iterable, Tuple
import os
import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def make_output_path_png(suffix: Optional[str] = None) -> str:
    os.makedirs("output", exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if suffix:
        # sanitize suffix to filesystem-friendly short token
        safe = "".join(c for c in suffix if c.isalnum() or c in "-_")
        return os.path.join("output", f"delta_{stamp}_{safe}.png")
    return os.path.join("output", f"delta_{stamp}.png")


def make_chart_png_bytes(dates: Sequence[str],
                         daily_values: Sequence[int],
                         cumulative_values: Sequence[int],
                         width: float = 10,
                         height: float = 5,
                         dpi: int = 140) -> bytes:
    x = list(range(len(dates)))
    fig, ax = plt.subplots(figsize=(width, height))
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


def _read_json(path: str) -> Tuple[List[str], List[int], List[int]]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    dates = data.get("dates", [])
    daily = data.get("daily_results", []) or data.get("daily", []) or []
    cumulative = data.get("cumulative", []) or data.get("cumulative_results", []) or []
    return dates, daily, cumulative


def _load_chart_run_sets_from_config() -> Optional[Iterable[Sequence[int]]]:
    """
    Try to import config.py and return an iterable of run-id sequences to produce charts for.
    Priority:
      1) CHART_RUN_SETS -> treated as an iterable of sequences (list of lists)
      2) CHART_RUN_IDS -> treated as a single sequence (wrapped into one-item list)
    Returns None if neither is present or on import error.
    """
    try:
        import config  # type: ignore
    except Exception:
        return None

    if hasattr(config, "CHART_RUN_SETS"):
        try:
            sets = getattr(config, "CHART_RUN_SETS")
            # coerce into list-of-seqs
            return [list(s) for s in sets]
        except Exception:
            return None
    if hasattr(config, "CHART_RUN_IDS"):
        try:
            ids = list(getattr(config, "CHART_RUN_IDS"))
            return [ids]
        except Exception:
            return None
    return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(prog="chart_generator.py", description="Generate chart PNG (writes file).")
    parser.add_argument("--from-json", help="JSON file with keys dates,daily_results,cumulative", default=None)
    parser.add_argument("--out", help="Explicit output path (overrides default timestamped filename)", default=None)
    parser.add_argument("--suffix", help="Optional suffix token for filename (used when creating multiple charts)", default=None)
    args = parser.parse_args()

    if args.from_json:
        try:
            dates, daily, cumulative = _read_json(args.from_json)
        except Exception as e:
            raise SystemExit(f"Failed to read JSON input {args.from_json}: {e}")
    else:
        # demo data
        dates = ["2023-09-25", "2023-09-26", "2023-09-27", "2023-09-28", "2023-09-29", "2023-09-30", "2023-10-01"]
        daily = [57, 35, 0, 0, 142, 0, 0]
        cumulative = [1031, 1066, 1066, 1066, 1208, 1208, 1208]

    # Base single PNG (existing behavior)
    png = make_chart_png_bytes(dates, daily, cumulative)
    out_path = args.out or make_output_path_png(suffix=args.suffix)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as fh:
        fh.write(png)
    print(f"Saved chart to: {out_path}")

    # Additional charts controlled via config.py (optional)
    run_sets = _load_chart_run_sets_from_config()
    if run_sets:
        # For each set, create an additional chart file. We reuse the same series by default.
        # Caller may instead arrange to pass different JSON files per run-set; this enhancement is
        # a simple entry point to produce multiple files with clear suffixes.
        for idx, run_set in enumerate(run_sets):
            # build a concise suffix, e.g. runs-12-34 or runs-A if too long/special
            try:
                if not run_set:
                    suffix = f"set{idx+1}"
                else:
                    # join numeric ids with dash, truncate if too long
                    token = "-".join(str(int(r)) for r in run_set)
                    if len(token) > 40:
                        token = token[:40]
                    suffix = f"runs-{token}"
            except Exception:
                suffix = f"set{idx+1}"

            png_bytes = make_chart_png_bytes(dates, daily, cumulative)
            out_path = make_output_path_png(suffix=suffix)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as fh:
                fh.write(png_bytes)
            print(f"Saved extra chart for {run_set} to: {out_path}")