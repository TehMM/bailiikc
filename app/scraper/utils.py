from __future__ import annotations

import hashlib
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

from . import config

LOGGER = logging.getLogger("bailiikc")
_LOGGER_INITIALISED = False
_CURRENT_LOG_FILE: Path = config.LOG_FILE


def _configure_logger(log_path: Path) -> None:
    """Configure the shared application logger to write to ``log_path``."""

    global _LOGGER_INITIALISED, _CURRENT_LOG_FILE

    ensure_dirs()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    for handler in list(LOGGER.handlers):
        LOGGER.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # noqa: BLE001
            continue

    formatter = logging.Formatter(
        fmt="[%(asctime)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    LOGGER.setLevel(logging.INFO)
    LOGGER.addHandler(stream_handler)
    LOGGER.addHandler(file_handler)
    LOGGER.propagate = False

    _CURRENT_LOG_FILE = log_path
    _LOGGER_INITIALISED = True


def _ensure_logger() -> None:
    """Initialise the logger lazily using the default log file."""

    if _LOGGER_INITIALISED:
        return

    default_log = config.LOG_FILE
    default_log.parent.mkdir(parents=True, exist_ok=True)
    _configure_logger(default_log)


def setup_run_logger() -> Path:
    """Rotate to a fresh timestamped log file for the current run."""

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = config.LOG_DIR / f"scrape_{timestamp}.log"
    _configure_logger(log_path)
    LOGGER.info("Logging to %s", log_path)
    return log_path


def get_current_log_path() -> Path:
    """Return the path to the log file currently receiving log lines."""

    _ensure_logger()
    return _CURRENT_LOG_FILE


def ensure_dirs() -> None:
    """Ensure that the application's expected directory structure exists."""

    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.PDF_DIR.mkdir(parents=True, exist_ok=True)
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)

    if not config.METADATA_FILE.exists():
        config.METADATA_FILE.write_text(
            json.dumps({"downloads": []}, indent=2),
            encoding="utf-8",
        )


def log_line(message: str) -> None:
    """Write a timestamped log line to stdout and the active log file."""

    _ensure_logger()
    LOGGER.info(message)


def sanitize_filename(name: str) -> str:
    """
    Return a filesystem-safe filename derived from *name*.
    Keeps only alphanumerics, dot, underscore, dash.
    """
    cleaned = "".join(
        ch if ch.isalnum() or ch in {".", "_", "-"} else "_"
        for ch in name.strip()
    ).strip("._")

    return cleaned or "file"


def sanitize_filename_component(component: str | None) -> str:
    """Sanitise a filename component by removing unsafe characters."""

    if not component:
        return ""

    cleaned = "".join(ch if ord(ch) >= 32 else " " for ch in component)
    cleaned = re.sub(r"[\\/:*?\"<>|]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" .")

    return cleaned


def truncate_to_max_bytes(value: str, max_bytes: int) -> str:
    """Truncate *value* so its UTF-8 byte length does not exceed *max_bytes*."""

    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value

    encoded = encoded[:max_bytes]
    while encoded and (encoded[-1] & 0b11000000) == 0b10000000:
        encoded = encoded[:-1]

    return encoded.decode("utf-8", "ignore")


def build_pdf_path(pdf_dir: Path, case_token: str | None, title: str) -> Path:
    """Construct a safe PDF path under *pdf_dir* for the given case."""

    pdf_dir = Path(pdf_dir)
    safe_title = sanitize_filename_component(title) or "Judgment"

    short_token = sanitize_filename_component((case_token or "").strip())
    if short_token:
        short_token = truncate_to_max_bytes(short_token, 32)

    base = safe_title
    filename = truncate_to_max_bytes(base, 200 - len(".pdf")) + ".pdf"
    path = pdf_dir / filename

    if path.exists() and short_token:
        base = f"{safe_title} - {short_token}"
        filename = truncate_to_max_bytes(base, 200 - len(".pdf")) + ".pdf"
        path = pdf_dir / filename

    if path.exists() or len(path.name.encode("utf-8")) > 255:
        digest_source = case_token or title or safe_title
        digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()[:8]
        max_base_bytes = max(1, 200 - len(".pdf") - len(digest) - 3)
        base = truncate_to_max_bytes(safe_title, max_base_bytes)
        filename = f"{base} - {digest}.pdf"
        path = pdf_dir / filename

    if len(path.name.encode("utf-8")) > 255:
        trimmed_name = truncate_to_max_bytes(path.stem, 200 - len(".pdf"))
        path = pdf_dir / f"{trimmed_name}.pdf"

    return path


def load_metadata() -> dict[str, Any]:
    """
    Load the metadata JSON document from disk.

    Ensures a ``downloads`` list is always present.
    """
    ensure_dirs()

    try:
        with config.METADATA_FILE.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, FileNotFoundError):
        data = {"downloads": []}

    downloads = data.setdefault("downloads", [])
    if not isinstance(downloads, list):
        data["downloads"] = []
    return data


def save_metadata(meta: dict[str, Any]) -> None:
    """Persist metadata to disk atomically."""
    tmp_path = config.METADATA_FILE.with_suffix(".tmp")

    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(meta, handle, indent=2)

    tmp_path.replace(config.METADATA_FILE)


def find_metadata_entry(
    meta: dict[str, Any],
    *,
    slug: str | None = None,
    fid: str | None = None,
    filename: str | None = None,
) -> Tuple[dict[str, Any] | None, int]:
    """Return the matching metadata entry (and its index) if present."""

    downloads = meta.get("downloads") or []

    for index, entry in enumerate(downloads):
        identifiers = {
            entry.get("slug"),
            entry.get("fid"),
            entry.get("filename"),
            entry.get("local_filename"),
        }

        if slug and slug in identifiers:
            return entry, index
        if fid and fid in identifiers:
            return entry, index
        if filename and filename in identifiers:
            return entry, index

    return None, -1


def has_local_pdf(meta_entry: dict[str, Any] | None) -> bool:
    """Return ``True`` if *meta_entry* points to a valid local PDF file."""

    if not meta_entry:
        return False

    candidate_paths: list[Path] = []

    stored_path = meta_entry.get("local_path")
    if isinstance(stored_path, str) and stored_path.strip():
        candidate = Path(stored_path)
        if candidate.is_absolute():
            candidate_paths.append(candidate)
        else:
            candidate_paths.append((config.DATA_DIR / candidate).resolve())
            candidate_paths.append(config.PDF_DIR / candidate.name)

    stored_name = meta_entry.get("local_filename") or meta_entry.get("filename")
    if stored_name:
        candidate_paths.append(config.PDF_DIR / stored_name)

    for path in candidate_paths:
        try:
            if path.is_file() and path.stat().st_size > 1024:
                return True
        except OSError:
            continue

    return False


def is_duplicate(
    fid: str,
    filename: str,
    meta: dict[str, Any],
    slug: str | None = None,
) -> bool:
    """Return ``True`` when metadata confirms the file is already downloaded."""

    entry, index = find_metadata_entry(meta, slug=slug, fid=fid, filename=filename)

    if entry is None:
        return False

    if entry.get("downloaded") and has_local_pdf(entry):
        return True

    candidate_paths: list[Path] = []

    stored_path = entry.get("local_path")
    if isinstance(stored_path, str) and stored_path.strip():
        path_obj = Path(stored_path)
        if path_obj.is_file():
            candidate_paths.append(path_obj)

    stored_name = (
        entry.get("local_filename")
        or entry.get("filename")
        or filename
    )
    if stored_name:
        candidate_paths.append(config.PDF_DIR / stored_name)

    for path in candidate_paths:
        try:
            if path.is_file() and path.stat().st_size > 1024:
                entry.update(
                    {
                        "slug": slug or entry.get("slug") or fid,
                        "fid": entry.get("fid") or fid,
                        "local_filename": path.name,
                        "filename": path.name,
                        "local_path": str(path.resolve()),
                        "downloaded": True,
                        "filesize": path.stat().st_size,
                        "downloaded_at": datetime.utcnow().isoformat(timespec="seconds")
                        + "Z",
                    }
                )
                save_metadata(meta)
                return True
        except OSError:
            continue

    title = entry.get("title") or slug or fid or filename
    log_line(
        "Metadata entry for %s exists but no valid PDF is present; refreshing." % title
    )
    downloads = meta.get("downloads", [])
    if 0 <= index < len(downloads):
        downloads.pop(index)
        save_metadata(meta)

    return False


def record_result(
    meta: dict[str, Any],
    *,
    slug: str,
    fid: str,
    title: str,
    local_filename: str,
    source_url: str,
    size_bytes: int,
    category: str | None = None,
    judgment_date: str | None = None,
    court: str | None = None,
    cause_number: str | None = None,
    subject: str | None = None,
    downloaded_at: str | None = None,
    extra_fields: dict[str, Any] | None = None,
    local_path: str | None = None,
) -> None:
    """Persist download metadata for a successfully saved PDF."""

    downloads = meta.setdefault("downloads", [])
    entry, _ = find_metadata_entry(
        meta, slug=slug, fid=fid, filename=local_filename
    )

    if entry is None:
        entry = {}
        downloads.append(entry)

    entry.update(
        {
            "slug": slug,
            "fid": fid,
            "title": title,
            "local_filename": local_filename,
            "filename": local_filename,
            "downloaded": True,
            "filesize": int(size_bytes),
            "downloaded_at": downloaded_at
            or datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source_url": source_url,
        }
    )

    if local_path is not None:
        entry["local_path"] = local_path

    if category is not None:
        entry["category"] = category
    if judgment_date is not None:
        entry["judgment_date"] = judgment_date
    if court is not None:
        entry["court"] = court
    if cause_number is not None:
        entry["cause_number"] = cause_number
    if subject is not None:
        entry["subject"] = subject
    if extra_fields:
        entry.update(extra_fields)

    save_metadata(meta)


def list_pdfs() -> list[Path]:
    """Return all PDF files currently stored in the PDF directory."""
    ensure_dirs()
    return sorted(
        p for p in config.PDF_DIR.glob("*.pdf")
        if p.is_file()
    )


def build_zip(zip_name: str = config.ZIP_NAME) -> Path:
    """Create a ZIP archive containing all downloaded PDFs."""
    ensure_dirs()
    archive_path = config.DATA_DIR / zip_name

    with ZipFile(archive_path, "w", ZIP_DEFLATED) as archive:
        for pdf_path in list_pdfs():
            archive.write(pdf_path, pdf_path.name)

    return archive_path


def load_base_url() -> str:
    """Load the persisted base URL, or fall back to DEFAULT_BASE_URL."""
    ensure_dirs()

    if config.CONFIG_FILE.exists():
        content = config.CONFIG_FILE.read_text(encoding="utf-8").strip()
        if content:
            return content

    return config.DEFAULT_BASE_URL


def save_base_url(url: str) -> None:
    """Persist the base URL to the configuration file."""
    ensure_dirs()
    config.CONFIG_FILE.write_text(url.strip(), encoding="utf-8")


def reset_state(*, delete_pdfs: bool = False, delete_logs: bool = False) -> None:
    """Reset metadata and optionally remove downloaded assets."""

    ensure_dirs()

    if config.METADATA_FILE.exists():
        try:
            config.METADATA_FILE.unlink()
        except OSError:
            pass

    if delete_pdfs:
        for path in config.PDF_DIR.glob("*.pdf"):
            try:
                path.unlink()
            except OSError:
                continue

    if delete_logs:
        for path in config.LOG_DIR.glob("scrape_*.log"):
            try:
                path.unlink()
            except OSError:
                continue

        latest = config.LOG_FILE
        if latest.exists():
            try:
                latest.unlink()
            except OSError:
                pass

    # Recreate required files and reset logger to a clean default file.
    ensure_dirs()
    _configure_logger(config.LOG_FILE)


__all__ = [
    "ensure_dirs",
    "setup_run_logger",
    "get_current_log_path",
    "log_line",
    "sanitize_filename",
    "sanitize_filename_component",
    "truncate_to_max_bytes",
    "build_pdf_path",
    "load_metadata",
    "save_metadata",
    "find_metadata_entry",
    "has_local_pdf",
    "is_duplicate",
    "record_result",
    "list_pdfs",
    "build_zip",
    "load_base_url",
    "save_base_url",
    "reset_state",
]
