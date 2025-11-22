from __future__ import annotations

import re
from datetime import datetime
from typing import Iterable

_DATE_FORMATS: Iterable[str] = (
    "%Y-%m-%d",
    "%Y-%b-%d",
    "%d/%m/%Y",
    "%d-%b-%Y",
)


def sortable_date(value: str) -> str:
    """Return an ISO-like string suitable for sorting judgement dates.

    Normalises a variety of expected input formats to YYYY-MM-DD. Returns an
    empty string when the value cannot be reasonably parsed.
    """

    candidate = (value or "").strip()
    if not candidate:
        return ""

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(candidate, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    digits = re.sub(r"[^0-9]", "", candidate)
    if len(digits) >= 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return ""
