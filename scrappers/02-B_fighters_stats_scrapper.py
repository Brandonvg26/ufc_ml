# =============================================================================
# crawl_fighter_stats.py — read fighters.csv, crawl each ufcstats fighter page,
#                          one row per fighter (career stat block). Resumable.
# =============================================================================
#
# Dependencies (pip install, pick the line for your FETCH_BACKEND):
#   Core (always):     pip install beautifulsoup4 lxml
#   playwright path:   pip install playwright   &&   playwright install chromium
#   curl_cffi path:    pip install curl_cffi
#   cloudscraper path: pip install cloudscraper
#
# Quick start: `pip install beautifulsoup4 lxml playwright && playwright install chromium`
#              then hit Run.
#
# WHY playwright is the default:
#   ufcstats.com serves a JS "Checking your browser…" interstitial. curl_cffi /
#   cloudscraper (TLS-spoof, no JS execution) MAY clear it and are ~10x faster —
#   worth trying first by switching FETCH_BACKEND. But playwright runs real JS and
#   is the reliable choice for this site, so it's the safe default for a 1500+ job.
#
# RESUME: this writes/append to fighter_stats.csv and, on restart, SKIPS any
#   fighter_url already present. A 1576-page crawl WILL get interrupted; just
#   re-run and it picks up where it stopped. Set RESUME=False to force a fresh run.
#
# SCOPE: scrapes the career-stat block (record, nickname, physicals, SLpM/Str.Acc/
#   SApM/Str.Def/TD Avg/TD Acc/TD Def/Sub.Avg). The per-bout fight-history table is
#   NOT scraped here — ask and I'll add it as a second pass. raw_stats_json holds
#   the full key/value dump so nothing is silently dropped.
# =============================================================================

# ----------------------------- CONFIG (edit here) ----------------------------
INPUT_CSV      = r"ufc_data\fights.csv"      # the uploaded file; columns: ,fighter,fighter_url
FIGHTER_COL    = "fighter"
URL_COL        = "fighter_url"

OUTPUT_DIR     = "ufc_data"          # created if missing
OUTPUT_CSV     = "fighter_stats.csv" # streamed to disk; also the resume store
OUTPUT_FORMAT  = "csv"               # "csv" | "json" | "both"  (json built from final csv)

MAX_FIGHTERS   = None                # None = all; or an int for a quick test run
RESUME         = True                # skip fighter_urls already in OUTPUT_CSV
DELAY_SECONDS  = 1.5                 # polite pause between requests

FETCH_BACKEND  = "playwright"        # "playwright" | "curl_cffi" | "cloudscraper"
RETRIES        = 3
RETRY_BACKOFF  = 2.0                 # seconds * attempt number
TIMEOUT        = 30                  # per-request timeout (seconds)
CHALLENGE_WAIT = 15                  # playwright: max seconds to wait for JS challenge

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")
# -----------------------------------------------------------------------------

import os
import sys
import csv
import json
import time

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
# Fetch layer — one entry point, three swappable backends (mirrors event scraper)
# =============================================================================
_STATE = {}  # lazy backend state (playwright page, cached scraper, etc.)


def _looks_like_challenge(html):
    if not html:
        return True
    low = html.lower()
    return ("checking your browser" in low
            or "this site requires javascript" in low
            or "<title>loading" in low)


def _fetch_curl_cffi(url):
    from curl_cffi import requests as creq
    r = creq.get(url, impersonate="chrome", timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def _fetch_cloudscraper(url):
    import cloudscraper
    s = _STATE.get("cloudscraper")
    if s is None:
        s = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False})
        _STATE["cloudscraper"] = s
    r = s.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text


def _fetch_playwright(url):
    if "page" not in _STATE:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        _STATE.update(playwright=pw, browser=browser, context=context,
                      page=context.new_page())
    page = _STATE["page"]
    page.goto(url, wait_until="networkidle", timeout=TIMEOUT * 1000)
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
    raise ValueError(f"Unknown FETCH_BACKEND: {FETCH_BACKEND!r}")


def fetch_html(url):
    """Fetch with retries + backoff; treat the JS challenge as a failure to retry."""
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            html = _raw_fetch(url)
            if _looks_like_challenge(html):
                raise RuntimeError("JS challenge not cleared")
            return html
        except Exception as e:  # noqa: BLE001 - broad on purpose for retry
            last = e
            wait = RETRY_BACKOFF * attempt
            print(f"    [retry {attempt}/{RETRIES}] {e} (waiting {wait:.0f}s)")
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
        # value = the li text minus the title label
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
    print(f"Backend={FETCH_BACKEND}  input={total_all} fighters  "
          f"already done={len(done)}  to crawl={total_todo}")
    print(f"Output -> {out_path}\n")

    # open CSV in append mode; write header only if file is new/empty
    new_file = not os.path.exists(out_path) or os.path.getsize(out_path) == 0
    csv_file = open(out_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
    if new_file:
        writer.writeheader()
        csv_file.flush()

    failed_path = os.path.join(OUTPUT_DIR, "failed_fighters.txt")
    failed = []
    ok = 0

    try:
        for i, (name, url) in enumerate(todo, start=1):
            print(f"Fighter {i} of {total_todo}: {name}")
            try:
                html = fetch_html(url)
                row = parse_fighter(html, name, url)
                writer.writerow(row)
                csv_file.flush()  # crash-safe: each fighter persisted immediately
                ok += 1
            except Exception as e:  # noqa: BLE001 - one failure shouldn't kill the run
                print(f"    [FAILED] {url} -> {e}")
                failed.append(f"{url}\t{name}\t{e}")
            time.sleep(DELAY_SECONDS)
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
    print(f"Done. crawled OK this run: {ok}  failed: {len(failed)}")
    print(f"CSV (resumable store): {out_path}")
    if failed:
        print(f"Failures logged to: {failed_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()