#!/usr/bin/env python3
"""
orchestrator.py

Run pipeline:
- Run omni to write results.json and results_plan_*.json
- Generate charts only for per-plan JSONs (no global chart)
- Run output.py with --skip-omni so output.py assembles the report (renderer only)
"""
from __future__ import annotations

import sys
import subprocess
import logging
from pathlib import Path
from typing import List, Optional
import importlib
import os

logger = logging.getLogger("orchestrator")
_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.propagate = False


def _run_omni(args: List[str]) -> int:
    try:
        omni = importlib.import_module("omni")
        if hasattr(omni, "main"):
            logger.info("Calling omni.main() directly")
            return omni.main(args)
    except Exception:
        logger.debug("Import/inline omni failed")

    omni_script = Path(__file__).resolve().parent / "omni.py"
    if not omni_script.exists():
        logger.error("omni.py not found at %s", omni_script)
        return 2
    cmd = [sys.executable, str(omni_script)] + args
    logger.info("Running omni.py via subprocess: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("omni.py subprocess failed: %s", e)
        return e.returncode


def _discover_per_plan_jsons(out_dir: Path) -> List[Path]:
    return sorted(out_dir.glob("results_plan_*.json"))


def _invoke_chart_generator_for_plan_json(json_path: Path) -> int:
    chart_script = Path(__file__).resolve().parent / "chart_generator.py"
    if not chart_script.exists():
        logger.error("chart_generator.py not found at %s", chart_script)
        return 2
    # extract plan id
    plan_id = None
    name = json_path.name
    if name.startswith("results_plan_") and name.endswith(".json"):
        try:
            plan_id = int(name[len("results_plan_"):-len(".json")])
        except Exception:
            plan_id = None
    cmd = [sys.executable, str(chart_script), "--from-json", str(json_path)]
    if plan_id is not None:
        cmd += ["--plan-id", str(plan_id)]
    logger.info("Running chart_generator for %s (plan_id=%s)", json_path.name, plan_id)
    try:
        subprocess.run(cmd, check=True)
        logger.info("chart_generator succeeded for %s", json_path.name)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("chart_generator failed for %s: %s", json_path.name, e)
        return e.returncode


def _run_output(args: List[str]) -> int:
    try:
        output = importlib.import_module("output")
        if hasattr(output, "main"):
            logger.info("Calling output.main() directly")
            return output.main(args)
    except Exception:
        logger.debug("Import/inline output failed")

    output_script = Path(__file__).resolve().parent / "output.py"
    if not output_script.exists():
        logger.error("output.py not found at %s", output_script)
        return 2

    cmd = [sys.executable, str(output_script)] + args + ["--skip-omni"]
    repo_dir = str(Path(__file__).