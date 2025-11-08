"""Utility helpers for filesystem, logging, and metadata management."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Tuple
from zipfile import ZIP_DEFLATED, ZipFile

from . import config


def ensure_dirs() -> None:
    """Ensure that the application's expected directory structure exists."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.PDF_DIR.mkdir(parents=True, exist_ok=True)

    if not config.LOG_FILE.exists():
        config.LOG_FILE.touch()

    if not config.METADATA_FILE.exists():
        config.METADATA_FILE.write_text(
            json.dumps({"downloads": []}, indent=2),
            encoding="utf-8",
        )


def log_line(message: str) -> None:
    """
    Write a timestamped log line to stdout and the scrape log file.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)

    with config.LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


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

    data.setdefault("downloads", [])
    return data


def save_metadata(meta: dict[str, Any]) -> None:
    """
    Persist metadata to disk atomically.
    """
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

    stored_name = meta_entry.get("local_filename") or meta_entry.get("filename")
    if not stored_name:
        return False

    pdf_path = config.PDF_DIR / stored_name
    if not pdf_path.is_file():
        return False

    try:
        size = pdf_path.stat().st_size
    except OSError:
        return False

    return size > 1024


def is_duplicate(
    fid: str,
    filename: str,
    meta: dict[str, Any],
    slug: str | None = None,
) -> bool:
    """Return ``True`` when metadata confirms the file is already downloaded."""

    downloads = meta.get("downloads", [])
    entry, index = find_metadata_entry(meta, slug=slug, fid=fid, filename=filename)

    if entry is None:
        return False

    if entry.get("downloaded") and has_local_pdf(entry):
        return True

    stored_name = (
        entry.get("local_filename")
        or entry.get("filename")
        or filename
    )
    pdf_path = config.PDF_DIR / stored_name

    if pdf_path.is_file() and pdf_path.stat().st_size > 1024:
        entry.update(
            {
                "slug": slug or entry.get("slug") or fid,
                "fid": entry.get("fid") or fid,
                "local_filename": stored_name,
                "filename": stored_name,
                "downloaded": True,
                "filesize": pdf_path.stat().st_size,
                "downloaded_at": datetime.utcnow().isoformat(timespec="seconds")
                + "Z",
            }
        )
        save_metadata(meta)
        return True

    title = entry.get("title") or slug or fid or filename
    log_line(
        "Metadata entry for %s exists but no valid PDF is present; refreshing." % title
    )

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
    downloaded_at: str | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> None:
    """
    Persist download metadata for a successfully saved PDF.
    """

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

    if category is not None:
        entry["category"] = category
    if judgment_date is not None:
        entry["judgment_date"] = judgment_date
    if extra_fields:
        entry.update(extra_fields)

    save_metadata(meta)


def list_pdfs() -> list[Path]:
    """
    Return all PDF files currently stored in the PDF directory.
    """
    ensure_dirs()
    return sorted(
        p for p in config.PDF_DIR.glob("*.pdf")
        if p.is_file()
    )


def build_zip(zip_name: str = config.ZIP_NAME) -> Path:
    """
    Create a ZIP archive containing all downloaded PDFs.

    Returns the path to the generated archive.
    """
    ensure_dirs()
    archive_path = config.DATA_DIR / zip_name

    with ZipFile(archive_path, "w", ZIP_DEFLATED) as archive:
        for pdf_path in list_pdfs():
            archive.write(pdf_path, pdf_path.name)

    return archive_path


def load_base_url() -> str:
    """
    Load the persisted base URL, or fall back to DEFAULT_BASE_URL.
    """
    ensure_dirs()

    if config.CONFIG_FILE.exists():
        content = config.CONFIG_FILE.read_text(encoding="utf-8").strip()
        if content:
            return content

    return config.DEFAULT_BASE_URL


def save_base_url(url: str) -> None:
    """
    Persist the base URL to the configuration file.
    """
    ensure_dirs()
    config.CONFIG_FILE.write_text(url.strip(), encoding="utf-8")


__all__ = [
    "ensure_dirs",
    "log_line",
    "sanitize_filename",
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
]
