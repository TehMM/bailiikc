# app/scraper/utils.py
from dataclasses import dataclass
from pathlib import Path

@dataclass
class _Paths:
    base: Path
    pdf_dir: Path
    scrape_log: Path

def paths() -> _Paths:
    base = Path("data/pdfs")
    return _Paths(
        base=base,
        pdf_dir=base,
        scrape_log=base / "scrape_log.txt",
    )

def ensure_dirs():
    p = paths()
    p.pdf_dir.mkdir(parents=True, exist_ok=True)
