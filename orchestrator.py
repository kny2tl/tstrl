#!/usr/bin/env python3
"""
orchestrator.py

Run pipeline and append Jira tables to the final HTML report.

Behavior changes:
- Removes lines beginning with "Generated:" (case-insensitive) from the final HTML before injection
- Appends two Jira tables and injects a scoped CSS block that limits milestone/test-run/all tables
  to max-width: 800px while keeping them responsive.
"""
from __future__ import annotations

import sys
import subprocess
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
import importlib
import os
import json
import html
import re

logger = logging.getLogger("orchestrator")
_handler = logging.StreamHandler()
_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.propagate = False

# -------------------------
# Pipeline runners (unchanged)
# -------------------------
def _run_omni() -> int:
    repo_dir = Path(__file__).resolve().parent
    out_path = repo_dir / "results.json"
    args = ["--json-out", str(out_path)]
    try:
        omni = importlib.import_module("omni")
        if hasattr(omni, "main"):
            logger.info("Calling omni.main() directly")
            return omni.main(args)
    except Exception:
        logger.debug("Import/inline omni failed; will use subprocess")
    omni_script = repo_dir / "omni.py"
    if not omni_script.exists():
        logger.error("omni.py not found at %s", omni_script)
        return 2
    cmd = [sys.executable, str(omni_script), "--json-out", str(out_path)]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_dir) + (os.pathsep + env.get("PYTHONPATH", ""))
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("omni.py subprocess failed: %s", e)
        return e.returncode

def _discover_per_plan_jsons(results_dir: Path) -> List[Path]:
    return sorted(results_dir.glob("results_plan_*.json"))

def _invoke_chart_generator_for_plan_json(json_path: Path) -> int:
    repo_dir = Path(__file__).resolve().parent
    chart_script = repo_dir / "chart_generator.py"
    if not chart_script.exists():
        logger.error("chart_generator.py not found at %s", chart_script)
        return 2
    plan_id: Optional[int] = None
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
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("chart_generator failed for %s: %s", json_path.name, e)
        return e.returncode

def _run_table_milestone(results_dir: Path) -> Path:
    repo_dir = Path(__file__).resolve().parent
    milestone_out = results_dir / "milestone_data.json"
    for mod_name in ("table_milestones", "table_milestone"):
        try:
            mod = importlib.import_module(mod_name)
            if hasattr(mod, "main"):
                try:
                    mod.main(["--json-out", str(milestone_out)])
                except TypeError:
                    mod.main()
                return milestone_out
        except Exception:
            logger.debug("Import/call of %s failed; will try subprocess", mod_name)
    candidates = [repo_dir / "table_milestones.py", repo_dir / "table_milestone.py"]
    for script in candidates:
        if not script.exists():
            continue
        cmd = [sys.executable, str(script), "--json-out", str(milestone_out)]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(repo_dir) + (os.pathsep + env.get("PYTHONPATH", ""))
        try:
            subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
            return milestone_out
        except subprocess.CalledProcessError as e:
            logger.warning("table_milestone subprocess failed for %s: %s", script.name, e)
    return milestone_out

def _run_table_jira(results_dir: Path) -> Path:
    repo_dir = Path(__file__).resolve().parent
    jira_out = results_dir / "jira_counts.json"
    try:
        mod = importlib.import_module("table_jira")
        if hasattr(mod, "main"):
            try:
                return_code = mod.main(["--json-out", str(jira_out)])
                if return_code not in (None, 0):
                    logger.warning("table_jira.main() returned exit code %s", return_code)
                return jira_out
            except TypeError:
                mod.main()
                return jira_out
    except Exception:
        logger.debug("Import/inline table_jira failed; will try subprocess")
    script = repo_dir / "table_jira.py"
    if not script.exists():
        logger.error("table_jira.py not found at %s", script)
        return jira_out
    cmd = [sys.executable, str(script), "--json-out", str(jira_out)]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_dir) + (os.pathsep + env.get("PYTHONPATH", ""))
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
        return jira_out
    except subprocess.CalledProcessError as e:
        logger.error("table_jira subprocess failed: %s", e)
        return jira_out

def _run_output(results_json: Path, output_dir: Path, milestone_json: Path) -> int:
    repo_dir = Path(__file__).resolve().parent
    try:
        output_mod = importlib.import_module("output")
        if hasattr(output_mod, "main"):
            args = [
                "--results-json", str(results_json),
                "--output-dir", str(output_dir),
                "--milestone-json", str(milestone_json),
                "--skip-omni",
            ]
            return output_mod.main(args)
    except Exception:
        logger.debug("Import/inline output failed; will use subprocess")
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
    try:
        subprocess.run(cmd, check=True, env=env, cwd=repo_dir)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error("output.py subprocess failed: %s", e)
        return e.returncode

# -------------------------
# Injection helpers (updated)
# -------------------------
def _find_latest_html(output_dir: Path) -> Optional[Path]:
    html_files = list(output_dir.glob("*.html"))
    if not html_files:
        return None
    latest = max(html_files, key=lambda p: p.stat().st_mtime)
    return latest

def _safe_int_str(v) -> str:
    if v is None:
        return ""
    try:
        return str(int(v))
    except Exception:
        return html.escape(str(v))

def _ci_lookup(jira_data: Dict[str, Any], key: str):
    if not isinstance(jira_data, dict):
        return None
    lower_key = key.lower()
    for k, v in jira_data.items():
        if isinstance(k, str) and k.lower() == lower_key:
            return v
    return None

# Scoped CSS that aims to constrain milestone/test-run/all tables to 800px responsively.
# It is intentionally specific and uses !important to override page rules that set table width to 100%.
_SCOPED_CSS = (
    "<style>\n"
    "/* Orchestrator injected styles: constrain tables to 800px max and keep them responsive */\n"
    ".orchestrator-jira-wrap, .orchestrator-jira-wrap table, .orchestrator-jira-wrap .milestone-table, .orchestrator-jira-wrap .test-run-table { max-width:800px !important; width:100% !important; box-sizing:border-box !important; }\n"
    ".orchestrator-jira-wrap table{ table-layout:fixed !important; border-collapse:collapse !important; }\n"
    ".orchestrator-jira-wrap th, .orchestrator-jira-wrap td { padding:8px !important; border:1px solid #ddd !important; overflow-wrap:break-word !important; word-break:break-word !important; }\n"
    ".orchestrator-jira-wrap thead th { background:#f2f2f2 !important; }\n"
    ".orchestrator-jira-wrap thead tr + tr th { background:#f9f9f9 !important; }\n"
    "</style>\n"
)

def _build_defects_count_fragment(jira_data: Dict[str, Any]) -> str:
    opened_text = _safe_int_str(_ci_lookup(jira_data, "Highest+high Opened"))
    resolved_text = _safe_int_str(_ci_lookup(jira_data, "Highest+high Resolved"))
    frag = (
        '\n<!-- BEGIN: Jira Defects table injected by orchestrator.py -->\n'
        f'{_SCOPED_CSS}'
        '<div class="orchestrator-jira-wrap">\n'
        '  <section class="jira-defects">\n'
        '    <table>\n'
        '      <thead>\n'
        '        <tr>\n'
        '          <th colspan="2">Defects Count (Highest &amp; High)</th>\n'
        '        </tr>\n'
        '        <tr>\n'
        '          <th>Opened</th>\n'
        '          <th>Resolved</th>\n'
        '        </tr>\n'
        '      </thead>\n'
        '      <tbody>\n'
        f'        <tr><td>{opened_text}</td><td>{resolved_text}</td></tr>\n'
        '      </tbody>\n'
        '    </table>\n'
        '  </section>\n'
        '</div>\n'
        '<!-- END: Jira Defects table injected by orchestrator.py -->\n'
    )
    return frag

def _build_status_severity_fragment(jira_data: Dict[str, Any]) -> str:
    mapping = [
        ("To Do", "To Do Highest", "To Do High"),
        ("In progress", "In Progress Highest", "In Progress High"),
        ("In review", "In Review Highest", "In Review High"),
        ("In testing", "In Testing Highest", "In Testing High"),
        ("Blocked", "Blocked Highest", "Blocked High"),
        ("All Resolved", "All Resolved Highest", "All Resolved High"),
    ]
    frag = (
        '\n<!-- BEGIN: Jira Status x Severity table injected by orchestrator.py -->\n'
        f'{_SCOPED_CSS}'
        '<div class="orchestrator-jira-wrap">\n'
        '  <section class="jira-status-severity">\n'
        '    <table>\n'
        '      <thead>\n'
        '        <tr>\n'
        '          <th rowspan="2">Defect status</th>\n'
        '          <th colspan="2">Severity</th>\n'
        '        </tr>\n'
        '        <tr>\n'
        '          <th>Highest</th>\n'
        '          <th>High</th>\n'
        '        </tr>\n'
        '      </thead>\n'
        '      <tbody>\n'
    )
    for status_label, key_highest, key_high in mapping:
        v_h = _safe_int_str(_ci_lookup(jira_data, key_highest))
        v_high = _safe_int_str(_ci_lookup(jira_data, key_high))
        frag += f'        <tr><td>{html.escape(status_label)}</td><td>{v_h}</td><td>{v_high}</td></tr>\n'
    frag += (
        '      </tbody>\n'
        '    </table>\n'
        '  </section>\n'
        '</div>\n'
        '<!-- END: Jira Status x Severity table injected by orchestrator.py -->\n'
    )
    return frag

def _remove_generated_lines(text: str) -> str:
    # Remove any line starting with "Generated:" (case-insensitive), including trailing whitespace and newline
    return re.sub(r"(?mi)^[ \t]*generated:.*(?:\r?\n|$)", "", text)

def _append_fragments_to_html(target_html: Path, fragments: List[str]) -> bool:
    try:
        txt = target_html.read_text(encoding="utf-8")
    except Exception as e:
        logger.warning("Failed to read %s: %s", target_html, e)
        return False

    # Remove any "Generated: ..." lines anywhere in the document
    txt = _remove_generated_lines(txt)

    lower = txt.lower()
    if "</body>" in lower:
        orig_idx = lower.rfind("</body>")
        new_txt = txt[:orig_idx] + ("\n".join(fragments) + "\n") + txt[orig_idx:]
    else:
        new_txt = txt + ("\n".join(fragments) + "\n")
    try:
        target_html.write_text(new_txt, encoding="utf-8")
        logger.info("Appended Jira fragments into %s", target_html)
        return True
    except Exception as e:
        logger.warning("Failed to write modified HTML to %s: %s", target_html, e)
        return False

# -------------------------
# Main pipeline
# -------------------------
def main(argv: List[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Run pipeline and append Jira tables")
    parser.add_argument("--no-charts", action="store_true")
    parser.add_argument("--skip-jira", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    repo_dir = Path(__file__).resolve().parent
    output_dir = repo_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1) omni
    rc = _run_omni()
    if rc != 0:
        logger.error("omni failed: %s", rc)
        return rc

    # 2) table_milestone
    milestone_path = _run_table_milestone(repo_dir)

    # 3) charts
    per_plan_jsons = _discover_per_plan_jsons(repo_dir)
    if not args.no_charts and per_plan_jsons:
        for j in per_plan_jsons:
            _invoke_chart_generator_for_plan_json(j)

    # 4) table_jira
    jira_path = repo_dir / "jira_counts.json"
    if not args.skip_jira:
        _run_table_jira(repo_dir)

    # 5) output
    rc_out = _run_output(repo_dir / "results.json", output_dir, milestone_path)
    if rc_out != 0:
        logger.error("output failed: %s", rc_out)
        return rc_out

    # 6) injection: append to the most recent HTML
    latest = _find_latest_html(output_dir)
    if not latest:
        logger.warning("No HTML to inject into; pipeline completed without injection")
        return 0

    try:
        jira_data = json.loads(jira_path.read_text(encoding="utf-8")) if jira_path.exists() else {}
    except Exception:
        jira_data = {}

    frag1 = _build_defects_count_fragment(jira_data)
    frag2 = _build_status_severity_fragment(jira_data)
    success = _append_fragments_to_html(latest, [frag1, frag2])
    if success:
        logger.info("Jira tables appended to %s", latest.name)
    else:
        logger.warning("Failed to append Jira tables to %s", latest.name)
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))