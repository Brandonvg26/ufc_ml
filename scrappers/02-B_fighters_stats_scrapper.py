# =============================================================================
# crawl_fighter_stats.py — read fighters.csv, crawl each ufcstats fighter page,
#                          one row per fighter (career stat block). Resumable.
# =============================================================================
#
# Dependencies:
#   Core (always):     pip install beautifulsoup4 lxml
#   playwright path:   pip install playwright && playwright install chromium
#   curl_cffi path:    pip install curl_cffi
#   cloudscraper path: pip install cloudscraper
#
# Backends (set FETCH_BACKEND):
#   playwright_async  ONE browser, many concurrent tabs. Fast AND clears the
#                     Cloudflare JS challenge. Recommended default for a 1500+ crawl.
#   playwright        ONE browser, one tab, sequential. Reliable but slow.
#   curl_cffi         pure-HTTP, thread-poolable, ~10x faster — but does NOT clear
#   cloudscraper      ufcstats' JS challenge (no JS engine). Kept for non-CF targets.
#
# RESUME (unchanged): fighter_stats.csv is the crash-safe resumable store. Each
#   fighter is written + flushed immediately; on restart, any fighter_url already
#   in the CSV is skipped. Interrupt any time and just re-run. RESUME=False = fresh.
#
# SCOPE: career-stat block (record, nickname, physicals, SLpM/Str.Acc/SApM/Str.Def/
#   TD Avg/TD Acc/TD Def/Sub.Avg). raw_stats_json holds the full dump so nothing is
#   silently dropped. Per-bout fight-history table is NOT scraped here.
# =============================================================================

# ----------------------------- CONFIG (edit here) ----------------------------
INPUT_CSV      = r"ufc_data\lists\list_fighters.csv"      # columns: ,fighter,fighter_url
FIGHTER_COL    = "fighter"
URL_COL        = "fighter_url"

OUTPUT_DIR     = r"ufc_data\raw"          # created if missing
OUTPUT_CSV     = "fighter_stats.csv" # streamed to disk; also the resume store
OUTPUT_FORMAT  = "csv"               # "csv" | "json" | "both"  (json built from final csv)

MAX_FIGHTERS   = None                # None = all; or an int for a quick test run
RESUME         = True                # skip fighter_urls already in OUTPUT_CSV
DELAY_SECONDS  = 1.5                 # polite pause between requests, PER WORKER/TAB

FETCH_BACKEND  = "playwright_async"  # see backend list above

# --- speed knobs ---
WORKERS        = 8                   # parallel fetchers for the HTTP backends only.
                                     #   (Ignored by playwright_async — use PW_CONCURRENCY.)
PW_CONCURRENCY = 6                   # playwright_async: tabs fetching at once in ONE browser.
                                     #   4-8 sane; higher = more RAM/CPU, burstier footprint.
PW_BLOCK_MEDIA = True                # skip image/css/font bytes (fighter pages need none)

# --- anti-throttle knobs (help the HTTP backends survive concurrency) ---
MAX_RPS        = 4.0                 # GLOBAL cap on total requests/sec across ALL workers.
                                     #   Tames connection resets under load. 0 = uncapped.
                                     #   (Applies to the HTTP + sync backends, not async.)
WARMUP         = True                # HTTP backends: solve Cloudflare once on the main
                                     #   thread, then share those cookies with every worker.
RETRY_JITTER   = 1.5                 # max random seconds added to each backoff (de-syncs herd)

RETRIES        = 4
RETRY_BACKOFF  = 2.0                 # base seconds for exponential backoff
TIMEOUT        = 30                  # per-request timeout (seconds)
CHALLENGE_WAIT = 15                  # max seconds to wait for the JS challenge to clear

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

BASE_URL = "http://ufcstats.com/"    # used to warm a Cloudflare clearance cookie
# -----------------------------------------------------------------------------

import os
import sys
import csv
import json
import time
import random
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

# Output schema, defined upfront. One dict per fighter.
FIELDNAMES = [
    "fighter", "fighter_url", "record", "nickname",
    "height", "weight", "reach", "stance", "dob",
    "slpm", "str_acc", "sapm", "str_def",
    "td_avg", "td_acc", "td_def", "sub_avg",
    "raw_stats_json",
]

# Map ufcstats stat titles (lowercased, trimmed) -> our column names.
KEY_MAP = {
    "height": "height", "weight": "weight", "reach": "reach",
    "stance": "stance", "dob": "dob",
    "slpm": "slpm", "str. acc.": "str_acc", "sapm": "sapm", "str. def.": "str_def",
    "td avg.": "td_avg", "td acc.": "td_acc", "td def.": "td_def", "sub. avg.": "sub_avg",
}


# =============================================================================
# Fetch layer — one entry point, swappable backends behind it.
# =============================================================================
_STATE = {}                 # sync playwright state
_TLS = threading.local()    # per-thread HTTP sessions (sessions aren't thread-safe)
_BLOCK_TYPES = {"image", "media", "font", "stylesheet"}


def _looks_like_challenge(html):
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
_WARM = {}


def _warm_cookies():
    """Best-effort HTTP-backend warmup. Any failure just skips it."""
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


def _fetch_curl_cffi(url):
    from curl_cffi import requests as creq
    s = getattr(_TLS, "curl", None)
    if s is None:
        s = creq.Session(impersonate="chrome")
        _seed_cookies(s)
        _TLS.curl = s
    r = s.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def _fetch_cloudscraper(url):
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


def _fetch_playwright(url):
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


def _raw_fetch(url):
    if FETCH_BACKEND == "playwright":
        return _fetch_playwright(url)
    if FETCH_BACKEND == "curl_cffi":
        return _fetch_curl_cffi(url)
    if FETCH_BACKEND == "cloudscraper":
        return _fetch_cloudscraper(url)
    raise ValueError(f"Unknown sync FETCH_BACKEND: {FETCH_BACKEND!r}")


def fetch_html(url):
    """Sync fetch with global rate limit + jittered exponential backoff.
    Used by the HTTP backends and the sequential playwright backend."""
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            _rate_limit()
            html = _raw_fetch(url)
            if _looks_like_challenge(html):
                raise RuntimeError("JS challenge not cleared")
            return html
        except Exception as e:  # noqa: BLE001
            last = e
            wait = RETRY_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, RETRY_JITTER)
            print(f"    [retry {attempt}/{RETRIES}] {e} (waiting {wait:.1f}s)")
            time.sleep(wait)
    raise RuntimeError(f"failed after {RETRIES} attempts: {last}")


def cleanup_fetch():
    if "page" in _STATE:
        try:
            _STATE["browser"].close()
            _STATE["playwright"].stop()
        except Exception:
            pass


# =============================================================================
# playwright_async — ONE browser + a pool of concurrent tabs sharing one
# Cloudflare clearance. No thread-safety issue (single event loop) and no
# "N simultaneous challenges" (one shared context, warmed once).
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


async def _run_playwright_async(todo, handle):
    """Crawl `todo` = list of (name, url) concurrently; call handle(name, url, row, err)."""
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

    async def _worker(ctx, name, url):
        async with sem:
            if DELAY_SECONDS:
                await asyncio.sleep(DELAY_SECONDS * random.uniform(0.5, 1.0))
            try:
                html = await _load_retry(ctx, url)
                row = parse_fighter(html, name, url)
                handle(name, url, row, None)
            except Exception as e:  # noqa: BLE001
                handle(name, url, None, e)

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
            await asyncio.gather(*(_worker(ctx, n, u) for (n, u) in todo))
        finally:
            await browser.close()


# =============================================================================
# Parsing
# =============================================================================
def parse_fighter(html, fighter_name, fighter_url):
    soup = BeautifulSoup(html, "lxml")

    # name + record live in the title; record is "Record: 14-5-0"
    title = soup.select_one(".b-content__title-highlight")
    name = title.get_text(strip=True) if title else fighter_name
    rec_el = soup.select_one(".b-content__title-record")
    record = ""
    if rec_el:
        record = rec_el.get_text(strip=True).replace("Record:", "").strip()
    nick_el = soup.select_one(".b-content__Nickname")
    nickname = nick_el.get_text(strip=True) if nick_el else ""

    # all stat key/value items across the info boxes
    raw = {}
    for li in soup.select("li.b-list__box-list-item"):
        title_el = li.select_one("i.b-list__box-item-title")
        if not title_el:
            continue
        key = title_el.get_text(strip=True).rstrip(":").strip()
        value = li.get_text(" ", strip=True).replace(title_el.get_text(strip=True), "").strip()
        if key:
            raw[key] = value

    row = {k: "" for k in FIELDNAMES}
    row["fighter"] = name
    row["fighter_url"] = fighter_url
    row["record"] = record
    row["nickname"] = nickname
    for k, v in raw.items():
        col = KEY_MAP.get(k.lower())
        if col:
            row[col] = v
    row["raw_stats_json"] = json.dumps(raw, ensure_ascii=False)
    return row


# =============================================================================
# IO helpers (resume)
# =============================================================================
def load_done_urls(path):
    """Return set of fighter_urls already written, for resume."""
    done = set()
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                u = (r.get("fighter_url") or "").strip()
                if u:
                    done.add(u)
    return done


def read_input(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            name = (r.get(FIGHTER_COL) or "").strip()
            url = (r.get(URL_COL) or "").strip()
            if url:
                yield name, url


# =============================================================================
# Main
# =============================================================================
def main():
    if not os.path.exists(INPUT_CSV):
        print(f"FATAL: input CSV not found: {os.path.abspath(INPUT_CSV)}")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, OUTPUT_CSV)

    fighters = list(read_input(INPUT_CSV))
    if MAX_FIGHTERS is not None:
        fighters = fighters[:MAX_FIGHTERS]

    done = load_done_urls(out_path) if RESUME else set()
    if not RESUME and os.path.exists(out_path):
        os.remove(out_path)

    todo = [(n, u) for (n, u) in fighters if u not in done]
    total_all = len(fighters)
    total_todo = len(todo)
    conc = (max(1, PW_CONCURRENCY) if FETCH_BACKEND == "playwright_async"
            else 1 if FETCH_BACKEND == "playwright" else max(1, WORKERS))
    print(f"Backend={FETCH_BACKEND}  input={total_all} fighters  "
          f"already done={len(done)}  to crawl={total_todo}  concurrency={conc}")
    print(f"Output -> {out_path}\n")

    # open CSV in append mode; write header only if file is new/empty
    new_file = not os.path.exists(out_path) or os.path.getsize(out_path) == 0
    csv_file = open(out_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES, extrasaction="ignore")
    if new_file:
        writer.writeheader()
        csv_file.flush()

    failed_path = os.path.join(OUTPUT_DIR, "failed_fighters.txt")
    failed = []
    lock = threading.Lock()
    counter = {"n": 0, "ok": 0}

    def handle(name, url, row, err):
        with lock:
            counter["n"] += 1
            i = counter["n"]
            if err is None:
                writer.writerow(row)
                csv_file.flush()          # crash-safe: each fighter persisted immediately
                counter["ok"] += 1
                print(f"  [{i}/{total_todo}] {name} -> ok")
            else:
                failed.append(f"{url}\t{name}\t{err}")
                print(f"  [{i}/{total_todo}] [FAILED] {url} -> {err}")

    def work(name, url):
        time.sleep(DELAY_SECONDS)          # per-worker politeness spacing
        return parse_fighter(fetch_html(url), name, url)

    _warm_cookies()   # HTTP backends only; self-guarded

    try:
        if FETCH_BACKEND == "playwright_async":
            asyncio.run(_run_playwright_async(todo, handle))
        elif conc <= 1:
            for name, url in todo:         # sequential (single-tab playwright path)
                try:
                    handle(name, url, work(name, url), None)
                except Exception as e:
                    handle(name, url, None, e)
        else:
            with ThreadPoolExecutor(max_workers=conc) as ex:
                futs = {ex.submit(work, n, u): (n, u) for (n, u) in todo}
                for fut in as_completed(futs):
                    n, u = futs[fut]
                    try:
                        handle(n, u, fut.result(), None)
                    except Exception as e:
                        handle(n, u, None, e)
    except KeyboardInterrupt:
        print("\nInterrupted — progress is saved; re-run to resume.")
    finally:
        csv_file.close()
        cleanup_fetch()

    if failed:
        with open(failed_path, "a", encoding="utf-8") as f:
            f.write("\n".join(failed) + "\n")

    # optional JSON snapshot, rebuilt from the full CSV so it includes prior runs
    if OUTPUT_FORMAT in ("json", "both"):
        json_path = os.path.join(OUTPUT_DIR, OUTPUT_CSV.replace(".csv", ".json"))
        with open(out_path, newline="", encoding="utf-8") as f:
            all_rows = list(csv.DictReader(f))
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, ensure_ascii=False, indent=2)
        print(f"JSON snapshot -> {json_path}")

    print("\n" + "=" * 60)
    print(f"Done. crawled OK this run: {counter['ok']}  failed: {len(failed)}")
    print(f"CSV (resumable store): {out_path}")
    if failed:
        print(f"Failures logged to: {failed_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()