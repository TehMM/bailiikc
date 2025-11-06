# app/scraper/playwright_downloader.py
from pathlib import Path
import re, time, json
from typing import List, Tuple, Dict, Callable, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

BASE_PAGE = "https://judicial.ky/judgments/unreported-judgments/"
ADMIN_AJAX = "https://judicial.ky/wp-admin/admin-ajax.php"

UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    '(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
)

def _sanitize_filename(name: str) -> str:
    name = re.sub(r"[\/\\\:\*\?\"\<\>\|]+", " ", name).strip()
    name = re.sub(r"\s+", " ", name)
    return name[:180]

def _load_all_results(page, max_loadmore: int):
    page.wait_for_load_state("networkidle")
    selectors = [
        "button.pt-cv-loadmore", ".pt-cv-loadmore button",
        ".pt-cv-loadmore a", "button:has-text('Load more')",
        "button:has-text('Load More')",
    ]
    clicks = 0
    while clicks < max_loadmore:
        found = None
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if loc.count() and loc.first.is_visible():
                    found = loc.first
                    break
            except Exception:
                pass
        if not found:
            break
        try:
            found.click()
            clicks += 1
            page.wait_for_load_state("networkidle")
            time.sleep(0.4)
        except Exception:
            break

    last_h = 0
    for _ in range(20):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.4)
        page.wait_for_load_state("networkidle")
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

def _collect_buttons(page) -> List[Tuple[str, str, str]]:
    selectors = [
        "[data-fid][data-fname][data-s]",
        "[data-fid][data-fname]",
        "button[data-fid], a[data-fid]",
    ]
    try:
        page.wait_for_selector(selectors[0] + "," + selectors[1] + "," + selectors[2], timeout=25000)
    except PWTimeout:
        return []

    els = []
    for sel in selectors:
        els += page.query_selector_all(sel)

    nonce_fallback = None
    for n in page.query_selector_all("[data-s]"):
        val = n.get_attribute("data-s")
        if val and re.fullmatch(r"[A-Za-z0-9]+", val):
            nonce_fallback = val
            break
    if not nonce_fallback:
        for s in page.query_selector_all("script"):
            txt = s.text_content() or ""
            m = re.search(r"dl_bfile.*?security[^A-Za-z0-9]+([A-Za-z0-9]{6,})", txt, flags=re.S)
            if m:
                nonce_fallback = m.group(1)
                break

    seen = set()
    out: List[Tuple[str, str, str]] = []
    for el in els:
        fid = (el.get_attribute("data-fid") or "").strip()
        fname = (el.get_attribute("data-fname") or "").strip()
        sec = (el.get_attribute("data-s") or "" or nonce_fallback or "").strip()
        if not fid or not fname or not sec:
            continue
        if not re.fullmatch(r"\d{5,}", fid):
            # ignore the old “Actions code” style tokens
            continue
        key = fid + "|" + fname
        if key in seen:
            continue
        seen.add(key)
        out.append((fid, fname, sec))
    return out

def _fetch_box_url(api, fid: str, fname: str, security: str) -> str:
    res = api.post(
        ADMIN_AJAX,
        form={"action": "dl_bfile", "fid": fid, "fname": fname, "security": security},
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
    js = res.json()
    if js in (-1, "-1"):
        raise RuntimeError("AJAX returned -1 (invalid nonce)")
    if not js.get("success") or "data" not in js or "fid" not in js["data"]:
        raise RuntimeError(f"AJAX payload malformed: {js}")
    return js["data"]["fid"].replace("\\/", "/")

def _stream_pdf(api, url: str, out_path: Path) -> None:
    r = api.get(url, timeout=120_000)
    if r.status not in (200, 206):
        raise RuntimeError(f"Download status {r.status}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(r.body())

def download_all(
    out_dir: Path,
    headless: bool = True,
    max_loadmore: int = 200,
    delay_sec: float = 0.6,
    filter_pred: Optional[Callable[[str, str], bool]] = None,  # (fid, fname) -> bool
    logger: Optional[Callable[[str], None]] = None,
) -> Dict:
    """
    Returns: {"found": N, "downloaded": M, "skipped": K, "errors": [{"fid":..., "msg":...}, ...]}
    """
    def log(msg: str):
        if logger:
            logger(msg)

    out_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(user_agent=UA, locale="en-US")
        page = context.new_page()

        log("Opening Unreported Judgments page...")
        page.goto(BASE_PAGE, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        _load_all_results(page, max_loadmore=max_loadmore)

        items = _collect_buttons(page)
        log(f"Found {len(items)} download candidates on page.")
        if not items:
            context.close(); browser.close()
            return {"found": 0, "downloaded": 0, "skipped": 0, "errors": []}

        api = context.request
        downloaded = 0
        skipped = 0
        errors = []

        for fid, fname, sec in items:
            if filter_pred and not filter_pred(fid, fname):
                skipped += 1
                continue
            safe = _sanitize_filename(fname)
            out_path = out_dir / f"{safe}.pdf"
            if out_path.exists():
                log(f"Skipping fid={fid} ({safe}.pdf exists).")
                skipped += 1
                continue
            try:
                log(f"Requesting Box URL for fid={fid} fname={fname}")
                box_url = _fetch_box_url(api, fid, fname, sec)
                log(f"Streaming PDF from {box_url}")
                _stream_pdf(api, box_url, out_path)
                log(f"Saved -> {out_path}")
                downloaded += 1
                time.sleep(delay_sec)
            except Exception as e:
                msg = f"{type(e).__name__}: {e}"
                log(f"Failed fid={fid}: {msg}")
                errors.append({"fid": fid, "msg": msg})

        context.close(); browser.close()
        return {"found": len(items), "downloaded": downloaded, "skipped": skipped, "errors": errors}
