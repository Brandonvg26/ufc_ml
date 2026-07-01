# =============================================================================
# UFC fight-details scraper (DYNAMIC SCHEMA, pass-2) — reads a CSV of fight_url
# values and harvests each fight-details page -> one row per fight.
# =============================================================================
#
# Same philosophy as the event scraper: NO hand-named fieldnames. Column names
# come from the page's own headers/labels (slugified); every value is captured;
# naming is decided PER COLUMN across the whole run, so the schema is rectangular
# (a column that's ever multi-valued becomes _f1/_f2 for every fight, blank where
# absent; per-round columns are blank for fights that ended early). You filter
# after.
#
# The fight-details page is multi-section, so columns are namespaced:
#   persons      -> fighter_f1/f2 (+_url), outcome_f1/f2, nickname_f1/f2
#   details      -> method, round, time, time_format, referee, details, ...
#   stats tables -> <tablelabel>_<round>_<colheader>_f1/f2
#                   e.g. totals_overall_kd_f1, totals_round_1_sig_str_f2
#   If a table label or round can't be detected, it falls back to t{n}/row{n}
#   so DATA is never dropped — only the NAME degrades.
#
# Dependencies (pick the line matching FETCH_BACKEND):
#   Core (always):     pip install beautifulsoup4 lxml
#   cloudscraper path: pip install cloudscraper
#   curl_cffi path:    pip install curl_cffi
#   playwright path:   pip install playwright   &&   playwright install chromium
# Quick start: pip install beautifulsoup4 lxml playwright && playwright install chromium
#
# ufcstats.com sits behind a JS challenge; playwright (real browser) is reliable.
# =============================================================================

# ----------------------------- CONFIG (edit here) ----------------------------
INPUT_CSV     = r"ufc_data\fights_list.csv"     # CSV containing the fight-details URLs
URL_COLUMN    = "fight_url"     # column to read; if absent, the first column is used
MAX_FIGHTS    = None             # None = all rows; int caps it (use 2-3 to test)
OUTPUT_DIR    = r"ufc_data\raw"      # created if missing
FETCH_BACKEND = "playwright"    # "playwright" | "cloudscraper" | "curl_cffi"
DELAY_SECONDS = 1.0             # polite pause between requests, PER WORKER

# --- speed knobs ---
WORKERS        = 8              # parallel fetchers. HTTP backends scale well (try 8).
                                # Forced to 1 for playwright (sync API isn't thread-safe).
RESUME         = True           # skip URLs already saved in the JSONL from a prior run
PW_BLOCK_MEDIA = True           # playwright: don't download images/css/fonts (src stays in DOM)

RETRIES        = 3
RETRY_BACKOFF  = 2.0
TIMEOUT        = 30
CHALLENGE_WAIT = 15

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/124.0.0.0 Safari/537.36")
# -----------------------------------------------------------------------------

import os
import re
import sys
import csv
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from bs4 import BeautifulSoup

META_FIELDS = ["fight_url", "event_name", "event_url"]

# =============================================================================
# Fetch layer (identical to the other scrapers)
# =============================================================================
_STATE = {}
_TLS = threading.local()   # per-thread HTTP sessions (sessions aren't thread-safe)


def _looks_like_challenge(html):
    if not html:
        return True
    low = html.lower()
    return ("checking your browser" in low
            or "this site requires javascript" in low
            or "<title>loading" in low)


def _fetch_cloudscraper(url):
    import cloudscraper
    s = getattr(_TLS, "cloudscraper", None)
    if s is None:
        s = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False})
        _TLS.cloudscraper = s
    r = s.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text


def _fetch_curl_cffi(url):
    from curl_cffi import requests as creq
    s = getattr(_TLS, "curl", None)
    if s is None:
        s = creq.Session(impersonate="chrome")
        _TLS.curl = s
    r = s.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


_BLOCK_TYPES = {"image", "media", "font", "stylesheet"}


def _fetch_playwright(url):
    if "page" not in _STATE:
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=USER_AGENT)
        if PW_BLOCK_MEDIA:
            # Abort heavy resource downloads — the <img src> stays in the DOM,
            # so title/perf-bonus detection still works; we just skip the bytes.
            ctx.route("**/*", lambda r: (r.abort() if r.request.resource_type in _BLOCK_TYPES
                                         else r.continue_()))
        _STATE.update(playwright=pw, browser=browser, context=ctx, page=ctx.new_page())
    page = _STATE["page"]
    # domcontentloaded is far quicker than networkidle and enough for static pages.
    page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT * 1000)
    deadline = time.time() + CHALLENGE_WAIT
    html = page.content()
    while _looks_like_challenge(html) and time.time() < deadline:
        page.wait_for_timeout(1000)
        html = page.content()
    return html


def _raw_fetch(url):
    if FETCH_BACKEND == "cloudscraper":
        return _fetch_cloudscraper(url)
    if FETCH_BACKEND == "curl_cffi":
        return _fetch_curl_cffi(url)
    if FETCH_BACKEND == "playwright":
        return _fetch_playwright(url)
    raise ValueError("Unknown FETCH_BACKEND: %r" % FETCH_BACKEND)


def fetch_html(url):
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            html = _raw_fetch(url)
            if _looks_like_challenge(html):
                raise RuntimeError("JS challenge not cleared")
            return html
        except Exception as e:  # noqa: BLE001
            last = e
            wait = RETRY_BACKOFF * attempt
            print("    [retry %d/%d] %s -> %s (waiting %.0fs)" % (attempt, RETRIES, url, e, wait))
            time.sleep(wait)
    raise RuntimeError("failed after %d attempts: %s" % (RETRIES, last))


def cleanup_fetch():
    if "page" in _STATE:
        try:
            _STATE["browser"].close()
            _STATE["playwright"].stop()
        except Exception:
            pass


# =============================================================================
# Generic helpers: cell = {base, values[], links[], img}
# =============================================================================
def slug(text):
    s = re.sub(r"[^\w]+", "_", (text or "").strip().lower())
    return re.sub(r"_+", "_", s).strip("_") or "col"


def _cell(base, values, links=None, img=False):
    return {"base": base, "values": values, "links": links or [], "img": img}


# =============================================================================
# Fight-details parsing (best-effort generic; index fallbacks guarantee capture)
# =============================================================================
def _table_label(table, idx):
    """Best-effort section label for a stats table; fallback t{idx}."""
    prev = table.find_previous(
        lambda t: t.name in ("p", "a", "th", "h2", "section")
        and t.get_text(strip=True)
        and ("collapse-link_tot" in (t.get("class") or [])
             or t.get_text(strip=True).lower() in ("totals", "significant strikes")))
    label = slug(prev.get_text(strip=True)) if prev else "t%d" % idx
    cls = table.get("class") or []
    if "js-fight-table" in cls:        # ufcstats marks per-round tables this way
        label += "_perround"
    return label


def _harvest_table(table, idx):
    cells = []
    headers = [slug(th.get_text(" ", strip=True)) for th in table.select("thead th")]
    label = _table_label(table, idx)
    current_round = ""
    data_seen = 0
    for tr in table.find_all("tr"):
        text = tr.get_text(" ", strip=True)
        cols = tr.select("td.b-fight-details__table-col") or tr.select("td")
        # round-label row (e.g. "Round 1") carries no per-fighter data
        if re.match(r"^round\s*\d+$", text.strip().lower()) and not tr.select("p.b-fight-details__table-text"):
            current_round = slug(text)
            continue
        # column-header row inside thead -> skip
        if tr.find_parent("thead") is not None:
            continue
        if not cols:
            continue
        data_seen += 1
        rkey = current_round or ("overall" if data_seen == 1 else "row%d" % data_seen)
        for i, td in enumerate(cols):
            col = headers[i] if i < len(headers) else "col%d" % i
            base = "%s_%s_%s" % (label, rkey, col)
            vals = [p.get_text(strip=True) for p in td.select("p.b-fight-details__table-text")]
            if not vals:
                t = td.get_text(" ", strip=True)
                vals = [t] if t else []
            links = [a.get("href", "").strip() for a in td.select("a") if a.get("href")]
            cells.append(_cell(base, vals, links, bool(td.select_one("img"))))
    return cells


def _label_value(node):
    lab = node.select_one(".b-fight-details__label")
    if not lab:
        return None, None
    label = slug(lab.get_text(strip=True).rstrip(":"))
    full = node.get_text(" ", strip=True)
    val = full.replace(lab.get_text(" ", strip=True), "", 1).strip()
    return label, val


def parse_fight(html, fight_url):
    soup = BeautifulSoup(html, "lxml")
    meta = {"fight_url": fight_url, "event_name": "", "event_url": ""}
    cells = []

    ev = soup.select_one("h2.b-content__title a, h2.b-content__title a.b-link")
    if ev:
        meta["event_name"] = ev.get_text(strip=True)
        meta["event_url"] = ev.get("href", "").strip()

    # --- fighters / outcome / nickname ---
    names, nurls, outcomes, nicks = [], [], [], []
    for person in soup.select("div.b-fight-details__person"):
        a = person.select_one(".b-fight-details__person-name a")
        nm = person.select_one(".b-fight-details__person-name")
        names.append(a.get_text(strip=True) if a else (nm.get_text(strip=True) if nm else ""))
        nurls.append(a.get("href", "").strip() if a else "")
        st = person.select_one(".b-fight-details__person-status")
        outcomes.append(st.get_text(strip=True) if st else "")
        nk = person.select_one(".b-fight-details__person-title")
        nicks.append(nk.get_text(strip=True) if nk else "")
    if any(names):
        cells.append(_cell("fighter", names, nurls))
    if any(outcomes):
        cells.append(_cell("outcome", outcomes))
    if any(nicks):
        cells.append(_cell("nickname", nicks))

    # --- bout type / title belt / performance bonus ---
    bt = soup.select_one(".b-fight-details__fight-title")
    if bt:
        # Title text only, e.g. "UFC Interim Heavyweight Title Bout".
        # The belt/perf <img> icons carry no text; normalize stray whitespace.
        bout_type = re.sub(r"\s+", " ", bt.get_text(" ", strip=True)).strip()
        cells.append(_cell("bout_type", [bout_type]))
        # ufcstats uses belt.png for title fights and perf.png for performance
        # bonuses — distinct icons, so "any <img>" is NOT a title signal.
        img_srcs = " ".join(img.get("src", "") for img in bt.select("img"))
        is_title = ("belt.png" in img_srcs) or ("title bout" in bout_type.lower())
        cells.append(_cell("is_title_bout", [1 if is_title else 0]))
        cells.append(_cell("perf_bonus", [1 if "perf.png" in img_srcs else 0]))

    # --- details section: every label:value pair, names from the page ---
    # Iterate the label nodes directly (not their containers) so the first
    # label can't swallow the whole text block.
    seen_labels = set()
    for lab in soup.select(".b-fight-details__label"):
        label = slug(lab.get_text(strip=True).rstrip(":"))
        if not label or label in seen_labels:
            continue
        container = lab.parent
        full = container.get_text(" ", strip=True) if container else ""
        val = full.replace(lab.get_text(" ", strip=True), "", 1).strip()
        seen_labels.add(label)
        cells.append(_cell(label, [val]))

    # --- every stats table, harvested generically ---
    for idx, table in enumerate(soup.select("table.b-fight-details__table")):
        cells.extend(_harvest_table(table, idx))

    return {"meta": meta, "cells": cells}


# =============================================================================
# Flatten — per-column naming across the whole run (rectangular schema)
# =============================================================================
def build_spec(records):
    order, vmax, lmax, has_img = [], {}, {}, {}
    for rec in records:
        for c in rec["cells"]:
            b = c["base"]
            if b not in vmax:
                order.append(b)
                vmax[b] = lmax[b] = 0
                has_img[b] = False
            vmax[b] = max(vmax[b], len(c["values"]))
            lmax[b] = max(lmax[b], len(c["links"]))
            has_img[b] = has_img[b] or c["img"]
    return order, vmax, lmax, has_img


def fieldnames_from_spec(order, vmax, lmax, has_img):
    fields = list(META_FIELDS)
    for b in order:
        if vmax[b] <= 1:
            fields.append(b)
        else:
            fields += ["%s_f%d" % (b, i) for i in range(1, vmax[b] + 1)]
        if lmax[b] == 1:
            fields.append("%s_url" % b)
        elif lmax[b] > 1:
            fields += ["%s_f%d_url" % (b, i) for i in range(1, lmax[b] + 1)]
        if has_img[b]:
            fields.append("%s_img" % b)
    return fields


def flatten(rec, vmax, lmax, has_img):
    row = dict(rec["meta"])
    for c in rec["cells"]:
        b, vals, links = c["base"], c["values"], c["links"]
        if vmax[b] <= 1:
            row[b] = vals[0] if vals else ""
        else:
            for i in range(1, vmax[b] + 1):
                row["%s_f%d" % (b, i)] = vals[i - 1] if i - 1 < len(vals) else ""
        if lmax[b] == 1:
            row["%s_url" % b] = links[0] if links else ""
        elif lmax[b] > 1:
            for i in range(1, lmax[b] + 1):
                row["%s_f%d_url" % (b, i)] = links[i - 1] if i - 1 < len(links) else ""
        if has_img[b]:
            row["%s_img" % b] = 1 if c["img"] else 0
    return row


def write_csv(records, path):
    if not records:
        return []
    order, vmax, lmax, has_img = build_spec(records)
    fields = fieldnames_from_spec(order, vmax, lmax, has_img)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, restval="")
        w.writeheader()
        for rec in records:
            w.writerow(flatten(rec, vmax, lmax, has_img))
    return fields


# =============================================================================
# Input
# =============================================================================
def read_fight_urls(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = [r for r in csv.reader(f) if r]
    if not rows:
        return []
    header = [c.strip().lower() for c in rows[0]]
    if URL_COLUMN.lower() in header:           # normal: a fight_url column
        col, start = header.index(URL_COLUMN.lower()), 1
    elif rows[0] and rows[0][0].strip().lower().startswith("http"):  # headerless
        col, start = 0, 0
    else:                                      # unknown header -> assume first col
        col, start = 0, 1
    seen, urls = set(), []
    for r in rows[start:]:
        if len(r) > col:
            u = r[col].strip()
            if u and u not in seen:
                seen.add(u)
                urls.append(u)
    return urls


# =============================================================================
# Main
# =============================================================================
def main():
    if not os.path.exists(INPUT_CSV):
        print("FATAL: input CSV not found: %s (set INPUT_CSV)" % INPUT_CSV)
        sys.exit(1)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    csv_path = os.path.join(OUTPUT_DIR, "ufc_fight_details.csv")
    jsonl_path = os.path.join(OUTPUT_DIR, "ufc_fight_details_raw.jsonl")
    failed_path = os.path.join(OUTPUT_DIR, "ufc_failed_fights.txt")

    urls = read_fight_urls(INPUT_CSV)
    if MAX_FIGHTS is not None:
        urls = urls[:MAX_FIGHTS]

    # --- resume: reload prior records and skip URLs already saved ---
    records, failed = [], []
    done = set()
    mode = "w"
    if RESUME and os.path.exists(jsonl_path):
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    records.append(rec)
                    done.add(rec["meta"]["fight_url"])
                except Exception:
                    continue
        mode = "a"
    todo = [u for u in urls if u not in done]
    total = len(urls)
    workers = 1 if FETCH_BACKEND == "playwright" else max(1, WORKERS)
    print("Backend=%s  fights=%d  done=%d  to_fetch=%d  workers=%d"
          % (FETCH_BACKEND, total, len(done), len(todo), workers))

    lock = threading.Lock()
    counter = {"n": 0}
    jf = open(jsonl_path, mode, encoding="utf-8")

    def work(url):
        time.sleep(DELAY_SECONDS)          # per-worker politeness spacing
        return parse_fight(fetch_html(url), url)

    def handle(url, rec, err):
        with lock:
            counter["n"] += 1
            i = counter["n"]
            if err is None:
                records.append(rec)
                jf.write(json.dumps(rec, ensure_ascii=False) + "\n")
                jf.flush()
                print("  [%d/%d] %s -> %d cells" % (i, len(todo), url, len(rec["cells"])))
            else:
                failed.append("%s\t%s" % (url, err))
                print("  [%d/%d] [FAILED] %s -> %s" % (i, len(todo), url, err))

    try:
        if workers <= 1:
            for url in todo:                # sequential (playwright path)
                try:
                    handle(url, work(url), None)
                except Exception as e:
                    handle(url, None, e)
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = {ex.submit(work, u): u for u in todo}
                for fut in as_completed(futs):
                    u = futs[fut]
                    try:
                        handle(u, fut.result(), None)
                    except Exception as e:
                        handle(u, None, e)
    finally:
        jf.close()
        cleanup_fetch()

    fields = write_csv(records, csv_path)
    if failed:
        with open(failed_path, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))

    print("\n" + "=" * 60)
    print("Done. %d/%d fights parsed." % (len(records), total))
    print("CSV    : %s  (%d columns)" % (csv_path, len(fields)))
    print("JSONL  : %s  (raw per-fight backup)" % jsonl_path)
    if fields:
        print("Columns: %s" % ", ".join(fields))
    if failed:
        print("Failed : %d -> %s" % (len(failed), failed_path))
    print("=" * 60)


if __name__ == "__main__":
    main()