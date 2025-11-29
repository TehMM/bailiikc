from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from . import config, consistency, db
from .config_validation import validate_runtime_config
from .logging_utils import _scraper_event
from .utils import disk_has_room, ensure_dirs, log_line


@dataclass
class HealthResult:
    ok: bool
    checks: dict[str, dict[str, Any]]


def run_health_checks(entrypoint: str = "cli") -> HealthResult:
    checks: dict[str, dict[str, Any]] = {}

    try:
        validate_runtime_config(entrypoint or "cli", mode=None)
        checks["config"] = {"ok": True}
    except ValueError as exc:
        checks["config"] = {"ok": False, "error": str(exc)}

    ensure_dirs()
    fs_ok = disk_has_room(config.MIN_FREE_MB, config.DATA_DIR)
    checks["filesystem"] = {
        "ok": fs_ok,
        "data_dir": str(config.DATA_DIR),
        "min_free_mb": config.MIN_FREE_MB,
    }

    try:
        db.initialize_schema()
        conn = db.get_connection()
        conn.execute("SELECT COUNT(*) FROM runs")
        checks["database"] = {"ok": True}
    except Exception as exc:  # noqa: BLE001
        checks["database"] = {"ok": False, "error": str(exc)}

    try:
        comparison = consistency.compare_latest_downloads_json_vs_db()
        checks["consistency"] = {
            "ok": bool(comparison.get("ok", False)),
            "details": comparison,
        }
    except Exception as exc:  # noqa: BLE001
        checks["consistency"] = {"ok": False, "error": str(exc)}

    strict_consistency = entrypoint == "cli"
    overall_ok = all(
        check.get("ok", False)
        for name, check in checks.items()
        if strict_consistency or name != "consistency"
    )

    try:
        _scraper_event(
            "state" if overall_ok else "error",
            phase="health",
            context="healthcheck",
            ok=overall_ok,
            checks=checks,
        )
    except Exception:
        pass

    return HealthResult(ok=overall_ok, checks=checks)


if __name__ == "__main__":  # pragma: no cover
    result = run_health_checks(entrypoint="cli")
    for name, info in result.checks.items():
        status = "OK" if info.get("ok") else "FAIL"
        log_line(f"[HEALTH] {name}: {status} {info}")
    raise SystemExit(0 if result.ok else 1)
