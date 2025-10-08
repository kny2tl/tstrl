#!/usr/bin/env python3
"""
table_jira.py

Fetch counts for an embedded set of JIRA filters and emit a single JSON file:
  mapping filter name -> count (null if unavailable)

Behavior:
- Primary POST /rest/api/3/search/jql with {"jql":..., "maxResults":1, "fields":[]}
- If API returns an explicit total field use it
- Otherwise page using GET /rest/api/3/search/jql and accumulate until isLast or max pages
- Only writes counts JSON (no summary)
- Safe defaults and small CLI to control cache / paging
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests  # type: ignore
    from requests import RequestException  # type: ignore
except Exception:
    requests = None
    RequestException = Exception  # type: ignore

# Defaults
DEFAULT_TIMEOUT = 20
DEFAULT_WORKERS = 4
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 2000
DEFAULT_CACHE_TTL = 300
DEFAULT_CACHE_PATH = Path(".jira_cache.json")

# Config
@dataclass(frozen=True)
class Config:
    jira_base: str
    jira_user: Optional[str]
    jira_token: Optional[str]
    timeout: int = DEFAULT_TIMEOUT
    workers: int = DEFAULT_WORKERS
    page_size: int = DEFAULT_PAGE_SIZE
    max_pages: int = DEFAULT_MAX_PAGES
    cache_ttl: int = DEFAULT_CACHE_TTL
    cache_path: Path = DEFAULT_CACHE_PATH
    session: Optional[Any] = None

# Logging
logger = logging.getLogger("table_jira")
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(ch)
logger.propagate = False
logger.setLevel(logging.INFO)

# Filters to run (edit)
FILTERS: List[Tuple[str, str]] = [
    ("To Do Highest", 'project = MFS AND issuetype = Bug AND priority = Highest AND status = "To Do" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("In Progress Highest", 'project = MFS AND issuetype = Bug AND priority = Highest AND status = "In Progress" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("In Review Highest", 'project = MFS AND issuetype = Bug AND priority = Highest AND status = "In Review" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("In testing Highest", 'project = MFS AND issuetype = Bug AND priority = Highest AND status = "In Testing" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("Blocked Highest", 'project = MFS AND issuetype = Bug AND priority = Highest AND status = Blocked AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("All Resolved Highest", 'project = MFS AND issuetype = Bug AND priority = Highest AND statusCategory = Done AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("Highest+high Opened", 'project = MFS AND issuetype = Bug AND statusCategory != Done AND priority in (Highest, High) AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("Highest+high Resolved", 'project = MFS AND issuetype = Bug AND statusCategory = Done AND priority in (Highest, High) AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),


    ("To Do High", 'project = MFS AND issuetype = Bug AND priority = High AND status = "To Do" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("In Progress High", 'project = MFS AND issuetype = Bug AND priority = High AND status = "In Progress" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("In Review High", 'project = MFS AND issuetype = Bug AND priority = High AND status = "In Review" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("In testing High", 'project = MFS AND issuetype = Bug AND priority = High AND status = "In Testing" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("Blocked High", 'project = MFS AND issuetype = Bug AND priority = High AND status = "Blocked" AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
    ("All Resolved High", 'project = MFS AND issuetype = Bug AND priority = High AND statusCategory=Done AND "Region (migrated)[Select List (multiple choices)]" = Tunisia'),
]

# Utilities
_INVISIBLE_RE = re.compile(r"[\u200B\u200C\u200D\uFEFF]")

def _sha12(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]

def _short_exc(e: Exception) -> str:
    try:
        return str(e).splitlines()[0][:200]
    except Exception:
        return "error"

def _redact(s: str, n: int = 120) -> str:
    if not s:
        return ""
    s = s.replace("\n", " ")
    return s if len(s) <= n else s[:n] + "..."

# JQL sanitize (conservative)
_SMART_QUOTES = {
    "\u201C": '"', "\u201D": '"', "\u2018": "'", "\u2019": "'",
    "\u201E": '"', "\u201F": '"'
}

def sanitize_jql(jql: str) -> Optional[str]:
    if not isinstance(jql, str):
        return None
    s = jql
    for k, v in _SMART_QUOTES.items():
        s = s.replace(k, v)
    s = unicodedata.normalize("NFKC", s)
    s = _INVISIBLE_RE.sub("", s)
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    # quick parentheses check
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return None
    if depth != 0:
        return None
    return s

def _cache_key(jql: str) -> str:
    return f"{_sha12(jql)}::{sanitize_jql(jql) or jql}"

# Simple disk cache (only successful counts cached)
def _load_cache(path: Path, ttl: int) -> Dict[str, Dict[str, Any]]:
    try:
        if ttl <= 0 or not path.exists():
            return {}
        raw = path.read_text(encoding="utf-8")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        logger.debug("Failed to load cache (ignored)")
        return {}

def _save_cache(path: Path, ttl: int, cache: Dict[str, Dict[str, Any]]) -> None:
    try:
        if ttl <= 0:
            return
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        logger.debug("Failed to write cache (ignored)")

def _cache_get(cache: Dict[str, Dict[str, Any]], key: str, ttl: int) -> Optional[int]:
    ent = cache.get(key)
    if not ent:
        return None
    ts = ent.get("ts", 0)
    if time.time() - ts > ttl:
        return None
    return ent.get("count")

def _cache_set(cache: Dict[str, Dict[str, Any]], key: str, count: int) -> None:
    cache[key] = {"ts": int(time.time()), "count": int(count)}

# Network session
def _session(cfg: Config):
    if cfg.session:
        return cfg.session
    if requests is None:
        raise RuntimeError("requests not available")
    return requests.Session()

# Parse explicit total only (do not trust len(issues) from maxResults=1)
def _extract_total(data: Any) -> Optional[int]:
    if not isinstance(data, dict):
        return None
    for k in ("total", "totalResults", "total_count", "count", "size"):
        v = data.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.isdigit():
            return int(v)
    md = data.get("metadata")
    if isinstance(md, dict) and isinstance(md.get("total"), int):
        return md.get("total")
    return None

def _page_count_get(cfg: Config, auth: Tuple[str, str], safe_jql: str) -> Optional[int]:
    sess = _session(cfg)
    endpoint = f"{cfg.jira_base}/rest/api/3/search/jql"
    start = 0
    page_size = max(1, min(cfg.page_size, 1000))
    total = 0
    pages = 0
    backoff = 0.8

    while True:
        if pages >= cfg.max_pages:
            logger.error("Exceeded max pages (%d) while paging; aborting", cfg.max_pages)
            return None
        params = {"jql": safe_jql, "startAt": start, "maxResults": page_size}
        try:
            resp = sess.get(endpoint, params=params, auth=auth, headers={"Accept": "application/json"}, timeout=cfg.timeout)
        except RequestException as e:
            logger.debug("Paging GET exception: %s", _short_exc(e))
            time.sleep(backoff + backoff * 0.2 * random.random())
            backoff *= 2
            continue

        logger.debug("Paging GET startAt=%d -> %s", start, resp.status_code)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else backoff
            except Exception:
                wait = backoff
            time.sleep(wait + wait * 0.2 * random.random())
            backoff *= 2
            continue
        if resp.status_code >= 400:
            logger.debug("Paging GET response preview: %s", (resp.text or "")[:400])
            return None

        try:
            data = resp.json()
        except Exception:
            logger.debug("Failed to parse paging response JSON")
            return None

        issues = data.get("issues")
        if not isinstance(issues, list):
            return None
        total += len(issues)
        pages += 1
        if data.get("isLast") is True:
            return total
        if len(issues) == 0:
            return total
        start += len(issues)
        time.sleep(0.12 + 0.08 * random.random())

def _fetch_count(cfg: Config, auth: Tuple[str, str], jql: str) -> Optional[int]:
    sess = _session(cfg)
    safe = sanitize_jql(jql)
    if safe is None:
        logger.warning("Sanitization failed for JQL: %s", _redact(jql))
        return None

    endpoint = f"{cfg.jira_base}/rest/api/3/search/jql"
    payload = {"jql": safe, "maxResults": 1, "fields": []}
    backoff = 0.8

    for attempt in range(1, 4):
        try:
            resp = sess.post(endpoint, json=payload, auth=auth, headers={"Accept": "application/json"}, timeout=cfg.timeout)
        except RequestException as e:
            logger.debug("Primary POST exception: %s", _short_exc(e))
            time.sleep(backoff + backoff * 0.2 * random.random())
            backoff *= 2
            continue

        logger.debug("POST %s -> %s", endpoint, resp.status_code)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                wait = float(ra) if ra else backoff
            except Exception:
                wait = backoff
            time.sleep(wait + wait * 0.2 * random.random())
            backoff *= 2
            continue
        if resp.status_code >= 400:
            logger.debug("Primary POST response preview: %s", (resp.text or "")[:400])
            if 400 <= resp.status_code < 500:
                logger.info("Client error %s for JQL [%s]; aborting", resp.status_code, _redact(safe))
                return None
            time.sleep(backoff + backoff * 0.2 * random.random())
            backoff *= 2
            continue

        try:
            data = resp.json()
        except Exception:
            logger.debug("Failed to parse primary POST JSON")
            return None

        # if API provides explicit total use it
        t = _extract_total(data)
        if isinstance(t, int):
            return t

        # no explicit total -> compute via GET paging (always)
        return _page_count_get(cfg, auth, safe)

    logger.error("Failed to fetch count for JQL [%s] after attempts", _redact(jql))
    return None

def fetch_counts(cfg: Config, filters: List[Tuple[str, str]], no_cache: bool = False, force: bool = False) -> Dict[str, Optional[int]]:
    if not cfg.jira_base or not cfg.jira_user or not cfg.jira_token:
        logger.error("JIRA base or credentials missing")
        return {name: None for name, _ in filters}

    auth = (cfg.jira_user, cfg.jira_token)
    cache = _load_cache(cfg.cache_path, cfg.cache_ttl) if not no_cache else {}
    results: Dict[str, Optional[int]] = {}

    # normalize and dedupe names
    seen = set()
    norm: List[Tuple[str, str]] = []
    for item in filters:
        name, jql = item
        name = str(name).strip()
        jql = str(jql).strip()
        if not name or not jql:
            continue
        if name in seen:
            logger.warning("Duplicate filter name skipped: %s", name)
            continue
        seen.add(name)
        norm.append((name, jql))

    for name, jql in norm:
        key = _cache_key(jql)
        cached = _cache_get(cache, key, cfg.cache_ttl)
        if cached is not None and not force:
            results[name] = cached
            continue
        val = _fetch_count(cfg, auth, jql)
        if isinstance(val, int):
            _cache_set(cache, key, val)
        results[name] = val

    try:
        _save_cache(cfg.cache_path, cfg.cache_ttl, cache)
    except Exception:
        logger.debug("Cache save failed (ignored)")

    return results

def parse_cli(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch JIRA filter counts and write JSON mapping name->count")
    p.add_argument("--json-out", default="jira_counts.json")
    p.add_argument("--no-cache", action="store_true")
    p.add_argument("--force", action="store_true", help="Ignore cache and refresh")
    p.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    p.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    p.add_argument("--cache-ttl", type=int, default=DEFAULT_CACHE_TTL)
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    return p.parse_args(argv)

def load_config_from_env(page_size: int, max_pages: int, cache_ttl: int, workers: int) -> Config:
    try:
        import config  # type: ignore
    except Exception:
        config = None
    base = (getattr(config, "JIRA_BASE_URL", None) if config else None) or os.getenv("JIRA_BASE_URL", "")
    user = (getattr(config, "JIRA_USERNAME", None) if config else None) or os.getenv("JIRA_USERNAME", "")
    token = (getattr(config, "JIRA_API_TOKEN", None) if config else None) or os.getenv("JIRA_API_TOKEN", None)
    return Config(
        jira_base=base.rstrip("/") if base else "",
        jira_user=user,
        jira_token=token,
        timeout=DEFAULT_TIMEOUT,
        workers=max(1, workers),
        page_size=max(1, min(page_size, 1000)),
        max_pages=max(1, max_pages),
        cache_ttl=max(0, cache_ttl),
        cache_path=DEFAULT_CACHE_PATH,
        session=None,
    )

def main(argv: Optional[List[str]] = None) -> int:
    args = parse_cli(argv)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    cfg = load_config_from_env(args.page_size, args.max_pages, args.cache_ttl, args.workers)
    logger.debug("Config jira_base=%s user=%s token_set=%s", cfg.jira_base, _redact(cfg.jira_user or ""), "yes" if cfg.jira_token else "no")
    counts = fetch_counts(cfg, FILTERS, no_cache=args.no_cache, force=args.force)
    try:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(counts, fh, ensure_ascii=False, indent=2)
    except Exception:
        logger.exception("Failed to write JSON output")
        return 2
    logger.info("Wrote %s", args.json_out)
    return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))