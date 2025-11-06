"""Selenium client helpers for interacting with the judiciary website."""
from __future__ import annotations

import json
import re
import time
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.remote.webdriver import WebDriver

from . import config
from .utils import ensure_dirs, log_line


NONCE_PATTERNS = [
    re.compile(r'["\'](?:_?nonce|security)["\']\s*[:=]\s*["\']([a-f0-9]{10})["\']', re.IGNORECASE),
    re.compile(r'dl_bfile[^;]*?["\']([a-f0-9]{10})["\']', re.IGNORECASE),
]


def make_driver() -> WebDriver:
    """Instantiate a headless Chrome WebDriver instance."""
    ensure_dirs()
    chrome_options = Options()
    chrome_options.binary_location = "/usr/bin/chromium"
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def get_nonce_and_cookies(driver: WebDriver, base_url: str, wait_seconds: int) -> tuple[str, dict[str, str]]:
    """Load the page and return the security nonce and cookies.

    Args:
        driver: Selenium WebDriver instance.
        base_url: URL of the page containing AJAX hooks.
        wait_seconds: Seconds to wait for scripts to execute.

    Returns:
        Tuple of (nonce string, cookies dictionary).
    """
    log_line(f"Loading base page {base_url}")
    driver.get(base_url)
    time.sleep(max(wait_seconds, 1))

    page_source = driver.page_source
    nonce = None
    for pattern in NONCE_PATTERNS:
        match = pattern.search(page_source)
        if match:
            nonce = match.group(1)
            break

    if not nonce:
        scripts = driver.find_elements("tag name", "script")
        for script in scripts:
            text = script.get_attribute("innerHTML") or ""
            for pattern in NONCE_PATTERNS:
                match = pattern.search(text)
                if match:
                    nonce = match.group(1)
                    break
            if nonce:
                break

    if not nonce:
        raise RuntimeError("Failed to locate AJAX security nonce on the page")

    cookies = {cookie["name"]: cookie["value"] for cookie in driver.get_cookies()}
    log_line(f"Extracted nonce {nonce} and {len(cookies)} cookies")
    return nonce, cookies


def selenium_ajax_get_box_url(driver: WebDriver, fid: str, fname: str, nonce: str) -> Optional[str]:
    """Request a Box download URL via the site's AJAX endpoint using Selenium.

    Args:
        driver: Active Selenium WebDriver instance.
        fid: Remote file identifier.
        fname: Filename parameter expected by the endpoint.
        nonce: Security nonce token.

    Returns:
        The Box download URL if successful, otherwise ``None``.
    """
    log_line(f"Requesting Box URL for fid={fid} fname={fname}")
    script = """
        const fid = arguments[0];
        const fname = arguments[1];
        const nonce = arguments[2];
        const callback = arguments[3];
        const body = new URLSearchParams({
            action: 'dl_bfile',
            fid: fid,
            fname: fname,
            security: nonce,
        });
        fetch('/wp-admin/admin-ajax.php', {
            method: 'POST',
            credentials: 'include',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
            },
            body: body.toString(),
        })
        .then(resp => resp.text())
        .then(text => callback(text))
        .catch(err => callback(JSON.stringify({error: err.message})));"""

    raw_response = driver.execute_async_script(script, fid, fname, nonce)
    try:
        payload = json.loads(raw_response)
    except json.JSONDecodeError:
        log_line(f"Failed to decode AJAX response: {raw_response[:120]}")
        return None

    if isinstance(payload, dict) and payload.get("success") and isinstance(payload.get("data"), dict):
        box_url = payload["data"].get("fid") or payload["data"].get("url")
        if box_url:
            log_line(f"Received Box URL for fid={fid}")
            return box_url

    log_line(f"AJAX request failed for fid={fid}: {payload}")
    return None


__all__ = ["make_driver", "get_nonce_and_cookies", "selenium_ajax_get_box_url"]
