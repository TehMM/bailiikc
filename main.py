#!/usr/bin/env python3
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_PAGE = "https://judicial.ky/judgments/unreported-judgments/"
ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"
OUT_DIR = Path("data/pdfs")
LOG_PATH = Path("data/pdfs/scrape_log.txt")
SEEN_DB = Path("data/pdfs/downloaded.json")

HEADLESS = os.getenv("HEADLESS", "1") == "1"
MAX_LOADMORE = int(os.getenv("MAX_LOADMORE", "200"))  # safety cap
DOWNLOAD_DELAY_SEC = float(os.getenv("DOWNLOAD_DELAY_SEC", "0.6"))  # be polite

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
)

def log(msg: str) -> None:
    ts = time.strftime("[%Y-%m-%d %H:%M:%S]")
    out = f"{ts} {msg}"
    print(out, flush=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(out + "\n")

def load_seen() -> Dict[str, str]:
    if SEEN_DB.exists():
        try:
            return json.loads(SEEN_DB.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_seen(seen: Dict[str, str]) -> None:
    SEEN_DB.write_text(json.dumps(seen, indent=2, ensure_ascii=False), encoding="utf-8")

def sanitize_filename(name: str) -> str:
    # Keep it readable but safe on most filesystems
    name = re.sub(r"[\/\\\:\*\?\"\<\>\|]+", " ", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:180]  # avoid OS path length issues

def collect_download_buttons(page) -> List[Tuple[str, str, str]]:
    """
    Return list of tuples (fid, fname, security) extracted from rendered DOM.
    We look for elements that the site wires to the 'dl_bfile' AJAX: those carry
    data-fid, data-fname and (usually) data-s (nonce).
    """
    log("Scanning DOM for download buttons...")
    selectors = [
        "[data-fid][data-fname][data-s]",       # ideal: has everything
        "[data-fid][data-fname]",               # sometimes nonce lives elsewhere
        "button[data-fid], a[data-fid]"         # last resort
    ]

    # Try to ensure results are present
    try:
        page.wait_for_selector(selectors[0] + "," + selectors[1] + "," + selectors[2], timeout=25000)
    except PWTimeout:
        log("No results found after 25s; the page may be empty or blocked.")
        return []

    # Gather all candidates from most to least specific
    els = []
    for sel in selectors:
        els += page.query_selector_all(sel)

    seen_key = set()
    items = []
    nonce_fallback = None

    # Sometimes a global nonce sits in a script tag or on a parent container; harvest one
    # Try common places:
    # 1) Any element with data-s
    for n in page.query_selector_all("[data-s]"):
        val = n.get_attribute("data-s")
        if val and re.fullmatch(r"[A-Za-z0-9]+", val):
            nonce_fallback = val
            break
    # 2) Look for a script text that mentions "dl_bfile" and "security"
    if not nonce_fallback:
        scripts = page.query_selector_all("script")
        for s in scripts:
            txt = s.text_content() or ""
            m = re.search(r"dl_bfile.*?security[^A-Za-z0-9]+([A-Za-z0-9]{6,})", txt, flags=re.S)
            if m:
                nonce_fallback = m.group(1)
                break

    for el in els:
        fid = el.get_attribute("data-fid")
        fname = el.get_attribute("data-fname")
        sec = el.get_attribute("data-s") or nonce_fallback
        if not fid or not fname or not sec:
            continue
        fid = fid.strip()
        fname = fname.strip()
        sec = sec.strip()
        # fid should be numeric if correct
        if not re.fullmatch(r"\d{5,}", fid):
            # ignore entries that look like the old "Actions" hash codes
            continue
        key = fid + "|" + fname
        if key in seen_key:
            continue
        seen_key.add(key)
        items.append((fid, fname, sec))

    log(f"Found {len(items)} download candidates.")
    return items

def load_all_results(page):
    """
    Try to reveal as many rows as possible:
    - Click 'Load more' buttons if present (plugin-style)
    - Attempt slow infinite-scroll to trigger lazy loading
    """
    # First wait a bit for initial results
    page.wait_for_load_state("networkidle")
    # Repeatedly click "Load more" style controls
    loadmore_selectors = [
        "button.pt-cv-loadmore",         # Content Views Pro common button
        ".pt-cv-loadmore button",
        ".pt-cv-loadmore a",
        "button:has-text('Load more')",
        "button:has-text('Load More')",
    ]
    clicks = 0
    while clicks < MAX_LOADMORE:
        found = None
        for sel in loadmore_selectors:
            try:
                loc = page.locator(sel)
                if loc.count() and loc.first.is_visible():
                    found = loc.first
                    break
            except Exception:
                continue
        if not found:
            break
        try:
            found.click()
            clicks += 1
            log(f"Clicked 'Load more' ({clicks})...")
            page.wait_for_load_state("networkidle")
            time.sleep(0.4)
        except Exception:
            break

    # Also try infinite scroll a few times to be safe
    last_h = 0
    for _ in range(20):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.4)
        page.wait_for_load_state("networkidle")
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

def fetch_box_url(api, fid: str, fname: str, security: str) -> str:
    """
    POST to admin-ajax with action=dl_bfile to get the short-lived Box download URL.
    Returns the URL or raises on error.
    """
    log(f"Requesting Box URL for fid={fid} fname={fname}")
    data = {
        "action": "dl_bfile",
        "fid": fid,
        "fname": fname,
        "security": security
    }
    # The site expects typical XHR headers
    res = api.post(
        ADMIN_AJAX,
        form=data,
        headers={
            "Accept": "*/*",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://judicial.ky",
            "Referer": BASE_PAGE,
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=30_000
    )
    if res.status != 200:
        raise RuntimeError(f"AJAX HTTP {res.status}")
    try:
        js = res.json()
    except Exception:
        raise RuntimeError("AJAX returned non-JSON")

    # WordPress returns -1 for bad nonce; plugin returns {success:false} on other errors
    if js == -1 or js == "-1":
        raise RuntimeError("AJAX returned -1 (invalid nonce)")
    if not js.get("success") or "data" not in js or "fid" not in js["data"]:
        raise RuntimeError(f"AJAX payload malformed: {js}")

    box_url = js["data"]["fid"]
    # Unescape any \/ sequences
    box_url = box_url.replace("\\/", "/")
    return box_url

def stream_pdf(api, url: str, out_path: Path) -> None:
    r = api.get(url, timeout=120_000)
    if r.status not in (200, 206):
        # Box may 302 first; playwright follows automatically
        # If still not OK, raise.
        raise RuntimeError(f"Download status {r.status}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(r.body())

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    seen = load_seen()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=HEADLESS)
        context = browser.new_context(user_agent=UA, locale="en-US")
        page = context.new_page()

        log("Opening Unreported Judgments page...")
        page.goto(BASE_PAGE, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")

        # Try to expose all results we can
        load_all_results(page)

        # Collect all candidate download elements
        items = collect_download_buttons(page)
        if not items:
            log("No downloadable items detected. Exiting.")
            return

        api = context.request  # shares cookies/session

        downloaded = 0
        for fid, fname, sec in items:
            safe_name = sanitize_filename(fname)
            out_file = OUT_DIR / f"{safe_name}.pdf"
            # Skip if already downloaded (by file or database)
            if out_file.exists() or seen.get(fid) == str(out_file):
                log(f"Skipping fid={fid} (already downloaded).")
                continue

            try:
                box_url = fetch_box_url(api, fid, fname, sec)
                log(f"Streaming PDF from {box_url}")
                stream_pdf(api, box_url, out_file)
                log(f"Saved -> {out_file}")
                seen[fid] = str(out_file)
                downloaded += 1
                save_seen(seen)
                time.sleep(DOWNLOAD_DELAY_SEC)
            except Exception as e:
                log(f"Failed to download fid={fid}: {e}")

        log(f"Done. New PDFs downloaded: {downloaded}")
        context.close()
        browser.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("Interrupted by user")
        sys.exit(130)
