# =============================================================================
# UFC Stats scraper — completed events  ->  one row per fight (CSV or JSON)
# =============================================================================
#
# One request per EVENT (~700 total), each yielding several fight rows. Every
# field (matchups, result, method, round, time, fighter links, per-fighter
# KD/STR/TD/SUB) lives in the event page's fight table, so we never hit the
# ~8000 individual fight-detail pages here (that's the 02 scraper's job).
#
# Backends (set FETCH_BACKEND):
#   playwright_async  ONE browser, many concurrent tabs. Fast AND clears the
#                     Cloudflare JS challenge. Recommended default.
#   playwright        ONE browser, one tab, sequential. Reliable but slow.
#   cloudscraper      pure-HTTP, thread-poolable — but does NOT clear ufcstats'
#   curl_cffi         JS challenge (no JS engine). Kept only for non-CF targets.
#
# Dependencies:
#   Core (always):     pip install beautifulsoup4 lxml
#   playwright path:   pip install playwright && playwright install chromium
#   cloudscraper path: pip install cloudscraper
#   curl_cffi path:    pip install curl_cffi
#
# RESUME: the always-on JSONL backup (ufc_events_raw.jsonl) is the crash-safe
# source of truth. On restart, already-scraped events (keyed on event_url) are
# reloaded and skipped, then the final CSV/JSON is rebuilt from all rows.
# =============================================================================

# ----------------------------- CONFIG (edit here) ----------------------------
MAX_EVENTS      = 500           # None = all completed events; or an int for testing
OUTPUT_DIR      = r"ufc_data/raw"    # created if missing
OUTPUT_FORMAT   = "csv"         # "csv" or "json"  (final consolidated snapshot)
WRITE_JSONL_BACKUP = True       # always stream a crash-safe .jsonl (required for RESUME)
DELAY_SECONDS   = 1.0           # polite pause between requests, PER WORKER/TAB
FETCH_BACKEND   = "playwright_async"  # see backend list above

# --- speed knobs ---
WORKERS         = 8             # parallel fetchers for the HTTP backends only.
                                #   (Ignored by playwright_async — use PW_CONCURRENCY.)
PW_CONCURRENCY  = 6             # playwright_async: tabs fetching at once in ONE browser.
                                #   4-8 is sane; higher = more RAM/CPU and a burstier,
                                #   more bot-like footprint. Start at 6.
RESUME          = True          # skip events already saved in the JSONL from a prior run
PW_BLOCK_MEDIA  = True          # playwright: skip image/css/font bytes (src stays in DOM)

# --- anti-throttle knobs (help the HTTP backends survive concurrency) ---
MAX_RPS         = 4.0           # GLOBAL cap on total requests/sec across ALL workers.
                                #   Tames connection resets under load. 0 = uncapped.
WARMUP          = True          # HTTP backends: solve Cloudflare once on the main thread,
                                #   then share those cookies with every worker.
RETRY_JITTER    = 1.5           # max random seconds added to each backoff (de-syncs herd)

RETRIES         = 4             # attempts per URL before giving up
RETRY_BACKOFF   = 2.0           # base seconds for exponential backoff
TIMEOUT         = 30            # per-request timeout (seconds)
CHALLENGE_WAIT  = 15            # max seconds to wait for the JS challenge to clear

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

BASE_URL  = "http://ufcstats.com/"
INDEX_URL = "http://ufcstats.com/statistics/events/completed?page=all"
# -----------------------------------------------------------------------------

import os
import sys
import json
import time
import csv
import random
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

# Output schema, defined upfront. One dict per fight matches these keys.
FIELDNAMES = [
    "event_name", "event_date", "event_location", "event_url",
    "fight_url",
    "fighter1", "fighter1_url",
    "fighter2", "fighter2_url",
    "result_raw", "winner",
    "kd_1", "kd_2",
    "str_1", "str_2",
    "td_1", "td_2",
    "sub_1", "sub_2",
    "weight_class", "method", "method_detail",
    "round", "time",
]

# =============================================================================
# Fetch layer — one entry point, swappable backends behind it.
# =============================================================================
_STATE = {}                 # sync playwright state: {playwright, browser, context, page}
_TLS = threading.local()    # per-thread HTTP sessions (sessions aren't thread-safe)
_BLOCK_TYPES = {"image", "media", "font", "stylesheet"}


def _looks_like_challenge(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    return ("checking your browser" in low
            or "this site requires javascript" in low
            or "<title>loading" in low)


# --- global rate limiter: caps TOTAL request rate regardless of worker count ---
_RATE_LOCK = threading.Lock()
_RATE_NEXT = {"t": 0.0}


def _rate_limit():
    if not MAX_RPS or MAX_RPS <= 0:
        return
    interval = 1.0 / MAX_RPS
    with _RATE_LOCK:
        now = time.monotonic()
        slot = _RATE_NEXT["t"] if _RATE_NEXT["t"] > now else now
        _RATE_NEXT["t"] = slot + interval
    delay = slot - time.monotonic()
    if delay > 0:
        time.sleep(delay)


# --- cookie warmup: prime Cloudflare ONCE on the main thread, share with workers -
_WARM = {}  # {"cookies": {...}} once primed


def _warm_cookies():
    """Best-effort HTTP-backend warmup. Any failure just skips it and workers fall
    back to solving the challenge themselves."""
    if not WARMUP or FETCH_BACKEND not in ("cloudscraper", "curl_cffi"):
        return
    try:
        _rate_limit()
        if FETCH_BACKEND == "cloudscraper":
            import cloudscraper
            s = cloudscraper.create_scraper(
                browser={"browser": "chrome", "platform": "windows", "mobile": False})
            s.get(BASE_URL, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
            _WARM["cookies"] = s.cookies.get_dict()
        elif FETCH_BACKEND == "curl_cffi":
            from curl_cffi import requests as creq
            s = creq.Session(impersonate="chrome")
            s.get(BASE_URL, timeout=TIMEOUT)
            try:
                _WARM["cookies"] = dict(s.cookies.items())
            except Exception:
                _WARM["cookies"] = {}
        n = len(_WARM.get("cookies") or {})
        print("  [warmup] primed %d cookie(s) for %s workers" % (n, FETCH_BACKEND)
              if n else "  [warmup] no cookies captured (edge may still challenge each worker)")
    except Exception as e:  # noqa: BLE001
        print("  [warmup] skipped (%s) — each worker will solve the challenge itself" % e)


def _seed_cookies(session):
    cookies = _WARM.get("cookies")
    if cookies:
        try:
            session.cookies.update(cookies)
        except Exception:
            pass


def _fetch_cloudscraper(url: str) -> str:
    import cloudscraper
    s = getattr(_TLS, "cloudscraper", None)
    if s is None:
        s = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False})
        _seed_cookies(s)
        _TLS.cloudscraper = s
    r = s.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text


def _fetch_curl_cffi(url: str) -> str:
    from curl_cffi import requests as creq
    s = getattr(_TLS, "curl", None)
    if s is None:
        s = creq.Session(impersonate="chrome")
        _seed_cookies(s)
        _TLS.curl = s
    r = s.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def _fetch_playwright(url: str) -> str:
    """Sync single-tab playwright (FETCH_BACKEND='playwright')."""
    if "page" not in _STATE:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        if PW_BLOCK_MEDIA:
            context.route("**/*", lambda r: (r.abort() if r.request.resource_type in _BLOCK_TYPES
                                             else r.continue_()))
        _STATE.update(playwright=pw, browser=browser, context=context,
                      page=context.new_page())
    page = _STATE["page"]
    page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT * 1000)
    deadline = time.time() + CHALLENGE_WAIT
    html = page.content()
    while _looks_like_challenge(html) and time.time() < deadline:
        page.wait_for_timeout(1000)
        html = page.content()
    return html


def _raw_fetch(url: str) -> str:
    if FETCH_BACKEND == "cloudscraper":
        return _fetch_cloudscraper(url)
    if FETCH_BACKEND == "curl_cffi":
        return _fetch_curl_cffi(url)
    if FETCH_BACKEND == "playwright":
        return _fetch_playwright(url)
    raise ValueError(f"Unknown sync FETCH_BACKEND: {FETCH_BACKEND!r}")


def fetch_html(url: str) -> str:
    """Sync fetch with global rate limit + jittered exponential backoff.
    Used by the HTTP backends, the sequential playwright backend, and the one-off
    index fetch. The async backend has its own loader."""
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            _rate_limit()
            html = _raw_fetch(url)
            if _looks_like_challenge(html):
                raise RuntimeError("JS challenge not cleared")
            return html
        except Exception as e:  # noqa: BLE001
            last_err = e
            wait = RETRY_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, RETRY_JITTER)
            print(f"    [retry {attempt}/{RETRIES}] {url} -> {e} (waiting {wait:.1f}s)")
            time.sleep(wait)
    raise RuntimeError(f"failed after {RETRIES} attempts: {last_err}")


def cleanup_fetch():
    if "page" in _STATE:
        try:
            _STATE["browser"].close()
            _STATE["playwright"].stop()
        except Exception:
            pass


# =============================================================================
# playwright_async — ONE browser + a pool of concurrent tabs.
# The shared context clears Cloudflare ONCE; every tab reuses that clearance.
# No thread-safety problem (async = one event loop) and no "N simultaneous
# challenges" problem (one shared context).
# =============================================================================
async def _async_block_route(route):
    try:
        if route.request.resource_type in _BLOCK_TYPES:
            await route.abort()
        else:
            await route.continue_()
    except Exception:
        try:
            await route.continue_()
        except Exception:
            pass


async def _pw_load_html(page, url):
    await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT * 1000)
    deadline = time.time() + CHALLENGE_WAIT
    html = await page.content()
    while _looks_like_challenge(html) and time.time() < deadline:
        await page.wait_for_timeout(1000)
        html = await page.content()
    return html


async def _fetch_index_async() -> str:
    """One-off async fetch of the completed-events index page."""
    from playwright.async_api import async_playwright
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=USER_AGENT)
        if PW_BLOCK_MEDIA:
            await ctx.route("**/*", _async_block_route)
        page = await ctx.new_page()
        try:
            html = await _pw_load_html(page, INDEX_URL)
        finally:
            await browser.close()
    if _looks_like_challenge(html):
        raise RuntimeError("index: JS challenge not cleared")
    return html


async def _run_playwright_async(todo, handle):
    """Crawl `todo` events concurrently in one browser; call handle(event, rows, err)."""
    from playwright.async_api import async_playwright

    sem = asyncio.Semaphore(max(1, PW_CONCURRENCY))

    async def _load_retry(ctx, url):
        last = None
        for attempt in range(1, RETRIES + 1):
            page = await ctx.new_page()
            try:
                html = await _pw_load_html(page, url)
                if _looks_like_challenge(html):
                    raise RuntimeError("JS challenge not cleared")
                return html
            except Exception as e:  # noqa: BLE001
                last = e
                wait = RETRY_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, RETRY_JITTER)
                print(f"    [retry {attempt}/{RETRIES}] {url} -> {e} (waiting {wait:.1f}s)")
                await asyncio.sleep(wait)
            finally:
                try:
                    await page.close()
                except Exception:
                    pass
        raise RuntimeError(f"failed after {RETRIES} attempts: {last}")

    async def _worker(ctx, event):
        async with sem:
            if DELAY_SECONDS:
                await asyncio.sleep(DELAY_SECONDS * random.uniform(0.5, 1.0))
            try:
                html = await _load_retry(ctx, event["event_url"])
                rows = parse_event(html, event)
                handle(event, rows, None)
            except Exception as e:  # noqa: BLE001
                handle(event, None, e)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=USER_AGENT)
        if PW_BLOCK_MEDIA:
            await ctx.route("**/*", _async_block_route)
        try:
            warm = await ctx.new_page()
            await _pw_load_html(warm, BASE_URL)
            await warm.close()
            print("  [warmup] shared browser context primed (one clearance for all tabs)")
        except Exception as e:  # noqa: BLE001
            print("  [warmup] context prime skipped (%s) — tabs will clear individually" % e)
        try:
            await asyncio.gather(*(_worker(ctx, e) for e in todo))
        finally:
            await browser.close()


# =============================================================================
# Parsing
# =============================================================================
def _texts(node, selector):
    return [el.get_text(strip=True) for el in node.select(selector)]


def parse_index(html: str):
    """Return list of dicts: event_name, event_date, event_location, event_url."""
    soup = BeautifulSoup(html, "lxml")
    events = []
    for row in soup.select("tr.b-statistics__table-row"):
        link = row.select_one("a.b-link.b-link_style_black[href*='/event-details/']")
        if not link:  # spacer / header rows have no event link
            continue
        name = link.get_text(strip=True)
        url = link.get("href", "").strip()
        date_el = row.select_one("span.b-statistics__date")
        date = date_el.get_text(strip=True) if date_el else ""
        cols = row.select("td.b-statistics__table-col")
        location = cols[-1].get_text(strip=True) if cols else ""
        events.append({
            "event_name": name,
            "event_date": date,
            "event_location": location,
            "event_url": url,
        })
    return events


def parse_event(html: str, event: dict):
    """Return list of fight-row dicts for a single event page."""
    soup = BeautifulSoup(html, "lxml")
    fights = []
    for row in soup.select("tr.b-fight-details__table-row"):
        try:
            fight_url = row.get("data-link", "").strip()
            cols = row.select("td.b-fight-details__table-col")
            if len(cols) < 10:
                continue

            result_flags = _texts(cols[0], "i.b-flag__text")
            result_raw = "/".join(result_flags)

            fighter_links = cols[1].select("a.b-link")
            f_names = [a.get_text(strip=True) for a in fighter_links]
            f_urls = [a.get("href", "").strip() for a in fighter_links]
            while len(f_names) < 2:
                f_names.append("")
                f_urls.append("")

            kd = _texts(cols[2], "p.b-fight-details__table-text")
            st = _texts(cols[3], "p.b-fight-details__table-text")
            td = _texts(cols[4], "p.b-fight-details__table-text")
            sub = _texts(cols[5], "p.b-fight-details__table-text")

            def pair(lst, i):
                return lst[i] if i < len(lst) else ""

            weight = pair(_texts(cols[6], "p.b-fight-details__table-text"), 0)
            method_parts = _texts(cols[7], "p.b-fight-details__table-text")
            method = pair(method_parts, 0)
            method_detail = pair(method_parts, 1)
            rnd = pair(_texts(cols[8], "p.b-fight-details__table-text"), 0)
            tme = pair(_texts(cols[9], "p.b-fight-details__table-text"), 0)

            winner = f_names[0] if result_flags[:1] == ["win"] else ""

            fights.append({
                "event_name": event["event_name"],
                "event_date": event["event_date"],
                "event_location": event["event_location"],
                "event_url": event["event_url"],
                "fight_url": fight_url,
                "fighter1": f_names[0], "fighter1_url": f_urls[0],
                "fighter2": f_names[1], "fighter2_url": f_urls[1],
                "result_raw": result_raw, "winner": winner,
                "kd_1": pair(kd, 0), "kd_2": pair(kd, 1),
                "str_1": pair(st, 0), "str_2": pair(st, 1),
                "td_1": pair(td, 0), "td_2": pair(td, 1),
                "sub_1": pair(sub, 0), "sub_2": pair(sub, 1),
                "weight_class": weight,
                "method": method, "method_detail": method_detail,
                "round": rnd, "time": tme,
            })
        except Exception as e:  # one bad row shouldn't kill the event
            print(f"    [warn] skipped a fight row: {e}")
            continue
    return fights


# =============================================================================
# Output
# =============================================================================
def write_final(all_rows, out_path):
    if OUTPUT_FORMAT == "json":
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, ensure_ascii=False, indent=2)
    else:  # csv
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
            w.writeheader()
            w.writerows(all_rows)


# =============================================================================
# Main
# =============================================================================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"ufc_events.{OUTPUT_FORMAT}")
    jsonl_path = os.path.join(OUTPUT_DIR, "ufc_events_raw.jsonl")
    failed_path = os.path.join(OUTPUT_DIR, "failed_events.txt")

    print(f"Backend={FETCH_BACKEND}  output={out_path}  format={OUTPUT_FORMAT}")
    print("Step 1: fetching completed-events index ...")

    try:
        if FETCH_BACKEND == "playwright_async":
            index_html = asyncio.run(_fetch_index_async())
        else:
            index_html = fetch_html(INDEX_URL)
    except Exception as e:
        print(f"FATAL: could not fetch index page: {e}")
        if FETCH_BACKEND in ("cloudscraper", "curl_cffi"):
            print("       The HTTP backends can't clear ufcstats' JS challenge.")
            print("       Set FETCH_BACKEND = 'playwright_async' and re-run.")
        cleanup_fetch()
        sys.exit(1)

    events = parse_index(index_html)
    if MAX_EVENTS is not None:
        events = events[:MAX_EVENTS]
    total = len(events)

    # --- RESUME: reload prior rows from the JSONL, skip events already scraped ---
    # Each JSONL line is one fight row carrying event_url, so a present event_url
    # means that whole event was completed (rows are flushed per-event).
    all_rows = []
    done_events = set()
    jsonl_mode = "w"
    if RESUME and WRITE_JSONL_BACKUP and os.path.exists(jsonl_path):
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                    all_rows.append(row)
                    done_events.add(row.get("event_url", ""))
                except Exception:
                    continue
        jsonl_mode = "a"

    todo = [e for e in events if e["event_url"] not in done_events]
    conc = (max(1, PW_CONCURRENCY) if FETCH_BACKEND == "playwright_async"
            else 1 if FETCH_BACKEND == "playwright" else max(1, WORKERS))
    print(f"Found {total} events  done={len(done_events)}  to_fetch={len(todo)}  "
          f"concurrency={conc}\n")

    failed = []
    jsonl_file = open(jsonl_path, jsonl_mode, encoding="utf-8") if WRITE_JSONL_BACKUP else None
    lock = threading.Lock()
    counter = {"n": 0}

    def handle(event, rows, err):
        with lock:
            counter["n"] += 1
            i = counter["n"]
            if err is None:
                all_rows.extend(rows)
                if jsonl_file:
                    for row in rows:
                        jsonl_file.write(json.dumps(row, ensure_ascii=False) + "\n")
                    jsonl_file.flush()
                print(f"  [{i}/{len(todo)}] {event['event_name']} -> {len(rows)} fights")
            else:
                failed.append(f"{event['event_url']}\t{err}")
                print(f"  [{i}/{len(todo)}] [FAILED] {event['event_url']} -> {err}")

    def work(event):
        time.sleep(DELAY_SECONDS)          # per-worker politeness spacing
        return parse_event(fetch_html(event["event_url"]), event)

    _warm_cookies()   # HTTP backends only; self-guarded

    try:
        if FETCH_BACKEND == "playwright_async":
            asyncio.run(_run_playwright_async(todo, handle))
        elif conc <= 1:
            for event in todo:             # sequential (single-tab playwright path)
                try:
                    handle(event, work(event), None)
                except Exception as e:
                    handle(event, None, e)
        else:
            with ThreadPoolExecutor(max_workers=conc) as ex:
                futs = {ex.submit(work, e): e for e in todo}
                for fut in as_completed(futs):
                    ev = futs[fut]
                    try:
                        handle(ev, fut.result(), None)
                    except Exception as e:
                        handle(ev, None, e)
    finally:
        if jsonl_file:
            jsonl_file.close()
        cleanup_fetch()

    # Rebuild the consolidated snapshot from ALL rows (reloaded + newly scraped).
    write_final(all_rows, out_path)

    if failed:
        with open(failed_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))

    print("\n" + "=" * 60)
    print(f"Done. {len(all_rows)} fight rows total "
          f"({len(todo) - len(failed)}/{len(todo)} new events scraped).")
    print(f"Output : {out_path}")
    if jsonl_file is not None:
        print(f"JSONL  : {jsonl_path}  (raw per-fight backup / resume source)")
    if failed:
        print(f"Failed : {len(failed)} events logged to {failed_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# SCRAPE_FIGHT_DETAILS (extension): each fight row's `fight_url` feeds the 02
# scraper for round-by-round significant strikes, control time, etc.
# -----------------------------------------------------------------------------