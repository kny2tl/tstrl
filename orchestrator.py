#!/usr/bin/env python3
"""
orchestrator.py

Run the full pipeline end-to-end without requiring CLI to specify JSON paths.
Behavior:
- Runs omni to produce results.json in the repo root
- Runs table_milestones.py (import/main preferred) to create milestone_data.json next to results.json
- Generates per-plan charts (chart_generator.py) for results_plan_*.json (no global chart)
- Runs output.py with --skip-omni and passes the discovered results.json, milestone_data.json, and output dir
- All subprocesses are invoked with PYTHONPATH set to the repo so local imports work
"""
from __future__ import annotations

import sys
import subprocess
import logging
from pathlib import Path
from typing import List
import importlib
import os

logger = logging.getLogger("orchestrator")
_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.propagate = False


def _run_omni() -> int:
    """
    Run omni to produce results.json in the repository root (default).
    Try importing omni and calling main(); fall back to subprocess if import fails.
    """
    repo_dir = Path(__file__).resolve().parent
    args = ["--json-out", str(repo_dir / "results.json")]
    try:
        omni = importlib.import_module("omni")
        if hasattr(omni, "main"):
            logger.info("Calling omni.main() directly")
            return omni.main(args)
    except Exception:
        logger.debug("Import/inline omni failed; invoking as subprocess")

    omni_script = repo_dir / "omni.py"
    if not omni_script.exists():
        logger.error("omni.py not found at %s", omni_script)
        return 2
    cmd = [sys.executable, str(omni_script), "--json-out", str(repo_dir / "results.json")]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_dir) + (os.pathsep + env.get("PYTHONPATH", ""))
    logger.info("Running omni.py via subprocess: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("omni.py subprocess failed: %s", e)
        return e.returncode


def _discover_per_plan_jsons(results_dir: Path) -> List[Path]:
    return sorted(results_dir.glob("results_plan_*.json"))


def _invoke_chart_generator_for_plan_json(json_path: Path) -> int:
    """
    Invoke chart_generator.py for a per-plan JSON.
    """
    repo_dir = Path(__file__).resolve().parent
    chart_script = repo_dir / "chart_generator.py"
    if not chart_script.exists():
        logger.error("chart_generator.py not found at %s", chart_script)
        return 2
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
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_dir) + (os.pathsep + env.get("PYTHONPATH", ""))
    logger.info("Running chart_generator for %s (plan_id=%s)", json_path.name, plan_id)
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
        logger.info("chart_generator succeeded for %s", json_path.name)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("chart_generator failed for %s: %s", json_path.name, e)
        return e.returncode


def _run_table_milestone(results_dir: Path) -> Path:
    """
    Run table_milestones (prefer import & main()) and return Path to milestone JSON.

    Attempts:
      1) import table_milestones or table_milestone and call main(["--json-out", path])
      2) fallback to subprocess on whichever script file exists using --json-out
    Returns the expected milestone path (may not exist if the script failed).
    """
    repo_dir = Path(__file__).resolve().parent
    milestone_out = results_dir / "milestone_data.json"

    # 1) Try import-based invocation for both module name candidates
    for mod_name in ("table_milestones", "table_milestone"):
        try:
            mod = importlib.import_module(mod_name)
            if hasattr(mod, "main"):
                logger.info("Invoking %s.main() directly", mod_name)
                try:
                    # call with explicit json-out flag
                    mod.main(["--json-out", str(milestone_out)])
                except TypeError:
                    # some mains may not accept args
                    mod.main()
                logger.info("%s.main() completed; expected output at %s", mod_name, milestone_out)
                return milestone_out
        except Exception:
            logger.debug("Import/call of %s failed; will try subprocess", mod_name)

    # 2) Fallback to subprocess for candidate filenames
    candidates = [repo_dir / "table_milestones.py", repo_dir / "table_milestone.py"]
    for script in candidates:
        if not script.exists():
            continue
        cmd = [sys.executable, str(script), "--json-out", str(milestone_out)]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(repo_dir) + (os.pathsep + env.get("PYTHONPATH", ""))
        logger.info("Running table_milestone subprocess: %s", " ".join(cmd))
        try:
            subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
            if milestone_out.exists():
                logger.info("table_milestone produced %s", milestone_out)
            else:
                logger.warning("table_milestone finished but %s not found", milestone_out)
            return milestone_out
        except subprocess.CalledProcessError as e:
            logger.warning("table_milestone subprocess failed for %s: %s", script.name, e)

    logger.debug("No table_milestone script succeeded; returning expected path %s", milestone_out)
    return milestone_out


def _run_output(results_json: Path, output_dir: Path, milestone_json: Path) -> int:
    """
    Run output.py to assemble final HTML. Prefer importing output and calling main, else subprocess.
    Always pass --skip-omni to avoid double-running omni.
    """
    repo_dir = Path(__file__).resolve().parent
    # Try direct import
    try:
        output_mod = importlib.import_module("output")
        if hasattr(output_mod, "main"):
            logger.info("Calling output.main() directly")
            args = [
                "--results-json", str(results_json),
                "--output-dir", str(output_dir),
                "--milestone-json", str(milestone_json),
                "--skip-omni",
            ]
            return output_mod.main(args)
    except Exception:
        logger.debug("Import/inline output failed; invoking as subprocess")

    output_script = repo_dir / "output.py"
    if not output_script.exists():
        logger.error("output.py not found at %s", output_script)
        return 2

    cmd = [
        sys.executable, str(output_script),
        "--results-json", str(results_json),
        "--output-dir", str(output_dir),
        "--milestone-json", str(milestone_json),
        "--skip-omni",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_dir) + (os.pathsep + env.get("PYTHONPATH", ""))
    logger.info("Running output.py via subprocess: %s", " ".join(cmd))
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("output.py subprocess failed: %s", e)
        return e.returncode


def main(argv: List[str] | None = None) -> int:
    """
    Orchestrator entrypoint. Runs everything with default paths in the repository root.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Run full pipeline (omni -> table_milestones -> per-plan charts -> output)")
    parser.add_argument("--no-charts", action="store_true", help="Skip generating per-plan charts")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)

    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    repo_dir = Path(__file__).resolve().parent
    results_json = repo_dir / "results.json"
    output_subdir = repo_dir / "output"
    output_subdir.mkdir(parents=True, exist_ok=True)

    # 1) Run omni (writes results.json into repo root)
    logger.info("Starting omni stage")
    rc = _run_omni()
    if rc != 0:
        logger.error("omni stage failed with code %s; aborting", rc)
        return rc
    logger.info("omni stage completed")

    # 2) Run table_milestones to produce milestone_data.json (next to results.json)
    milestone_path = _run_table_milestone(repo_dir)

    # 3) Discover per-plan JSONs (results_plan_*.json) in repo root
    per_plan_jsons = _discover_per_plan_jsons(repo_dir)
    if not per_plan_jsons:
        logger.warning("No per-plan result JSONs found in %s; skipping chart generation", repo_dir)
    else:
        logger.info("Discovered %d per-plan JSON file(s): %s", len(per_plan_jsons), ", ".join(p.name for p in per_plan_jsons))

    # 4) Generate per-plan charts unless skipped
    if not args.no_charts and per_plan_jsons:
        any_failed = False
        for j in per_plan_jsons:
            rc = _invoke_chart_generator_for_plan_json(j)
            if rc != 0:
                any_failed = True
        if any_failed:
            logger.warning("Some per-plan chart generation invocations failed; charts may be incomplete")
    else:
        logger.info("Per-plan chart generation skipped")

    # 5) Run output to assemble final HTML (pass explicit results.json and milestone_data.json)
    logger.info("Starting output stage")
    rc2 = _run_output(results_json, output_subdir, milestone_path)
    if rc2 != 0:
        logger.error("output stage failed with code %s", rc2)
        return rc2

    logger.info("Pipeline completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))