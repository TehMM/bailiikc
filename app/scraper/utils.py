"""Utility helpers for filesystem, logging, and metadata management."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

from . import config


def ensure_dirs() -> None:
    """Ensure that the application's expected directory structure exists."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    config.PDF_DIR.mkdir(parents=True, exist_ok=True)
    if not config.LOG_FILE.exists():
        config.LOG_FILE.touch()
    if not config.METADATA_FILE.exists():
        config.METADATA_FILE.write_text(json.dumps({"downloads": []}, indent=2), encoding="utf-8")


def log_line(message: str) -> None:
    """Write a timestamped log line to stdout and the scrape log file.

    Args:
        message: The message to be recorded.
    """
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with config.LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()


def sanitize_filename(name: str) -> str:
    """Return a filesystem-safe filename derived from *name*.

    Args:
        name: Raw name from CSV metadata.

    Returns:
        Sanitised filename containing only alphanumerics, dots, underscores and dashes.
    """
    cleaned = "".join(ch if ch.isalnum() or ch in {".", "_", "-"} else "_" for ch in name.strip())
    cleaned = cleaned.strip("._")
    return cleaned or "file"


def load_metadata() -> dict[str, Any]:
    """Load the metadata JSON document from disk.

    Returns:
        Metadata dictionary with at least a ``downloads`` key containing a list.
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
    """Persist metadata to disk atomically.

    Args:
        meta: Metadata dictionary to be saved.
    """
    tmp_path = config.METADATA_FILE.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(meta, handle, indent=2)
    tmp_path.replace(config.METADATA_FILE)


def is_duplicate(fid: str, filename: str, meta: dict[str, Any]) -> bool:
    """Determine whether a case has already been downloaded.

    Args:
        fid: Remote file identifier from the CSV.
        filename: Local filename.
        meta: Loaded metadata dictionary.

    Returns:
        True if the fid or filename already exists in metadata.
    """
    for entry in meta.get("downloads", []):
        if entry.get("fid") == fid or entry.get("filename") == filename:
            return True
    return False


def record_result(meta: dict[str, Any], fid: str, filename: str, fields: dict[str, Any]) -> None:
    """Record a successful download within metadata and persist it.

    Args:
        meta: Metadata dictionary.
        fid: Remote file identifier.
        filename: Local filename.
        fields: Additional fields to include for the record.
    """
    record = {
        "fid": fid,
        "filename": filename,
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    record.update(fields)
    meta.setdefault("downloads", []).append(record)
    save_metadata(meta)


def list_pdfs() -> list[Path]:
    """Return all PDF files currently stored in the PDF directory."""
    ensure_dirs()
    return sorted(p for p in config.PDF_DIR.glob("*.pdf") if p.is_file())


def build_zip(zip_name: str = config.ZIP_NAME) -> Path:
    """Create a ZIP archive containing all downloaded PDFs.

    Args:
        zip_name: Name of the archive to create in the data directory.

    Returns:
        Path to the generated ZIP archive.
    """
    ensure_dirs()
    archive_path = config.DATA_DIR / zip_name
    with ZipFile(archive_path, "w", ZIP_DEFLATED) as archive:
        for pdf_path in list_pdfs():
            archive.write(pdf_path, pdf_path.name)
    return archive_path


def load_base_url() -> str:
    """Retrieve the persisted base URL or fall back to the default value."""
    ensure_dirs()
    if config.CONFIG_FILE.exists():
        content = config.CONFIG_FILE.read_text(encoding="utf-8").strip()
        if content:
            return content
    return config.DEFAULT_BASE_URL


def save_base_url(url: str) -> None:
    """Persist the base URL to the configuration file.

    Args:
        url: URL string to store.
    """
    ensure_dirs()
    config.CONFIG_FILE.write_text(url.strip(), encoding="utf-8")


__all__ = [
    "ensure_dirs",
    "log_line",
    "sanitize_filename",
    "load_metadata",
    "save_metadata",
    "is_duplicate",
    "record_result",
    "list_pdfs",
    "build_zip",
    "load_base_url",
    "save_base_url",
]
