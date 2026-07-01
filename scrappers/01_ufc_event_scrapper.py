# =============================================================================
# UFC Stats scraper — completed events  ->  one row per fight (CSV or JSON)
# =============================================================================
#
# Dependencies (pip install, pick the line for your chosen FETCH_BACKEND):
#
#   Core (always):     pip install beautifulsoup4 lxml
#   cloudscraper path: pip install cloudscraper
#   curl_cffi path:    pip install curl_cffi
#   playwright path:   pip install playwright   &&   playwright install chromium
#
# Quick start: `pip install beautifulsoup4 lxml cloudscraper`, then hit Run.
#
# IMPORTANT — ufcstats.com is behind a JS "Checking your browser…" challenge.
#   - "cloudscraper" / "curl_cffi" are pip-only and MAY pass it (not guaranteed;
#     I couldn't test the live challenge from where this was written).
#   - "playwright" runs a real headless browser and is the reliable fallback,
#     at the cost of one extra command: `playwright install chromium`.
#   If a run logs every event as "challenge not cleared", switch FETCH_BACKEND
#   to "playwright".
#
# Design note: every field you asked for (matchups, result, method, round, time,
# fighter links) plus per-fighter KD/STR/TD/SUB lives in the EVENT page's fight
# table. So this scrapes 1 request per event (~700 total), NOT 1 per fight
# (~8000). Round-by-round / control-time / sig-strike breakdowns would require
# visiting each fight-details page — see SCRAPE_FIGHT_DETAILS note at the bottom.
# =============================================================================

# ----------------------------- CONFIG (edit here) ----------------------------
MAX_EVENTS      = 150          # None = all completed events; or an int for testing
OUTPUT_DIR      = r"ufc_data/raw"    # created if missing
OUTPUT_FORMAT   = "csv"         # "csv" or "json"
DELAY_SECONDS   = 1.5           # polite pause between requests
FETCH_BACKEND   = "playwright"  # "cloudscraper" | "curl_cffi" | "playwright"

RETRIES         = 3             # attempts per URL before giving up
RETRY_BACKOFF   = 2.0           # seconds, multiplied by attempt number
TIMEOUT         = 30            # per-request timeout (seconds)
CHALLENGE_WAIT  = 15            # playwright: max seconds to wait for JS challenge to clear

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")

BASE_URL  = "http://ufcstats.com"
INDEX_URL = "http://ufcstats.com/statistics/events/completed?page=all"
# -----------------------------------------------------------------------------

import os
import sys
import json
import time
import csv
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
# Fetch layer — one entry point, three swappable backends behind it.
# =============================================================================
_PW = {}  # lazy playwright state: {playwright, browser, context, page}


def _looks_like_challenge(html: str) -> bool:
    if not html:
        return True
    low = html.lower()
    return ("checking your browser" in low
            or "this site requires javascript" in low
            or "<title>loading" in low)


def _fetch_cloudscraper(url: str) -> str:
    import cloudscraper
    scraper = _PW.get("cloudscraper")
    if scraper is None:
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        _PW["cloudscraper"] = scraper
    r = scraper.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text


def _fetch_curl_cffi(url: str) -> str:
    from curl_cffi import requests as creq
    r = creq.get(url, impersonate="chrome", timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def _fetch_playwright(url: str) -> str:
    if "page" not in _PW:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        _PW.update(playwright=pw, browser=browser, context=context,
                   page=context.new_page())
    page = _PW["page"]
    page.goto(url, wait_until="networkidle", timeout=TIMEOUT * 1000)
    # Poll until the JS challenge clears (or time out).
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
    raise ValueError(f"Unknown FETCH_BACKEND: {FETCH_BACKEND!r}")


def fetch_html(url: str) -> str:
    """Fetch with retries + backoff. Treats the JS challenge page as a failure
    so it retries instead of silently parsing garbage."""
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            html = _raw_fetch(url)
            if _looks_like_challenge(html):
                raise RuntimeError("JS challenge not cleared")
            return html
        except Exception as e:  # noqa: BLE001 - want broad catch for retry
            last_err = e
            wait = RETRY_BACKOFF * attempt
            print(f"    [retry {attempt}/{RETRIES}] {url} -> {e} "
                  f"(waiting {wait:.0f}s)")
            time.sleep(wait)
    raise RuntimeError(f"failed after {RETRIES} attempts: {last_err}")


def cleanup_fetch():
    if "page" in _PW:
        try:
            _PW["browser"].close()
            _PW["playwright"].stop()
        except Exception:
            pass


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
            # Header rows / non-fight rows won't have the data-link.
            fight_url = row.get("data-link", "").strip()
            cols = row.select("td.b-fight-details__table-col")
            if len(cols) < 10:
                continue

            # col 0: result flags (winner listed first by ufcstats convention)
            result_flags = _texts(cols[0], "i.b-flag__text")
            result_raw = "/".join(result_flags)

            # col 1: two fighters with links
            fighter_links = cols[1].select("a.b-link")
            f_names = [a.get_text(strip=True) for a in fighter_links]
            f_urls = [a.get("href", "").strip() for a in fighter_links]
            while len(f_names) < 2:
                f_names.append("")
                f_urls.append("")

            # cols 2..5: paired stats (fighter1, fighter2)
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
# Main
# =============================================================================
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"ufc_events.{OUTPUT_FORMAT}")
    failed_path = os.path.join(OUTPUT_DIR, "failed_events.txt")

    print(f"Backend={FETCH_BACKEND}  output={out_path}  format={OUTPUT_FORMAT}")
    print("Step 1: fetching completed-events index ...")

    try:
        index_html = fetch_html(INDEX_URL)
    except Exception as e:
        print(f"FATAL: could not fetch index page: {e}")
        if FETCH_BACKEND != "playwright":
            print("       The JS challenge likely blocked the pip-only backend.")
            print("       Set FETCH_BACKEND = 'playwright' and re-run "
                  "(pip install playwright && playwright install chromium).")
        cleanup_fetch()
        sys.exit(1)

    events = parse_index(index_html)
    if MAX_EVENTS is not None:
        events = events[:MAX_EVENTS]
    total = len(events)
    print(f"Found {total} completed events to scrape.\n")

    all_rows = []
    failed = []

    # Stream CSV to disk as we go so a crash mid-run doesn't lose everything.
    csv_file = csv_writer = None
    if OUTPUT_FORMAT == "csv":
        csv_file = open(out_path, "w", newline="", encoding="utf-8")
        csv_writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        csv_writer.writeheader()

    try:
        for i, event in enumerate(events, start=1):
            print(f"Event {i} of {total}: {event['event_name']}")
            try:
                html = fetch_html(event["event_url"])
                rows = parse_event(html, event)
                print(f"    -> {len(rows)} fights")
                all_rows.extend(rows)
                if csv_writer:
                    csv_writer.writerows(rows)
                    csv_file.flush()
            except Exception as e:
                print(f"    [FAILED] {event['event_url']} -> {e}")
                failed.append(f"{event['event_url']}\t{e}")
            time.sleep(DELAY_SECONDS)
    finally:
        if csv_file:
            csv_file.close()
        cleanup_fetch()

    if OUTPUT_FORMAT == "json":
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, ensure_ascii=False, indent=2)

    if failed:
        with open(failed_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))

    print("\n" + "=" * 60)
    print(f"Done. {len(all_rows)} fights from {total - len(failed)}/{total} events.")
    print(f"Output : {out_path}")
    if failed:
        print(f"Failed : {len(failed)} events logged to {failed_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# SCRAPE_FIGHT_DETAILS (extension, not implemented):
# Each fight row has `fight_url` -> http://ufcstats.com/fight-details/{id}, which
# carries round-by-round significant strikes, control time, strike target/position
# breakdowns, etc. Adding that = one extra request per fight (~8000 total). Ask
# and I'll bolt on a second pass that reads fight_url from the CSV above.
# -----------------------------------------------------------------------------