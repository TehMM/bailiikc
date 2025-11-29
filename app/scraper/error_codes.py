from __future__ import annotations

"""Centralised error code taxonomy for scraper failures.

These codes are persisted in the downloads.error_code column and included in
structured logs so that we can explain why a download failed. The taxonomy is
intentionally small and internal-only but should stay stable for reporting.
"""


class ErrorCode:
    NETWORK = "network_error"
    HTTP_4XX = "http_4xx"
    HTTP_401 = "http_401_unauthorised"
    HTTP_403 = "http_403_forbidden"
    HTTP_404 = "http_404_not_found"
    HTTP_5XX = "http_5xx"
    BOX_RATE_LIMIT = "box_rate_limit"
    MALFORMED_PDF = "malformed_pdf"
    SITE_STRUCTURE = "site_structure_changed"
    INTERNAL = "internal_error"


__all__ = ["ErrorCode"]
