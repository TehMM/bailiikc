from types import SimpleNamespace

from app.scraper import cases_index, run, worklist


def _make_sync_result(version_id: int) -> SimpleNamespace:
    return SimpleNamespace(version_id=version_id)


def test_new_mode_uses_db_worklist_when_flag_enabled(monkeypatch):
    fake_items = [
        worklist.WorkItem(
            case_id=1,
            action_token_norm="TOKENONE",
            action_token_raw="tokenone",
            title="Case One",
            court="Court",
            category="Cat",
            judgment_date="2024-01-01",
            cause_number="123/2024",
            is_criminal=False,
            is_active=True,
            first_seen_version_id=2,
            last_seen_version_id=2,
            source=worklist.DEFAULT_SOURCE,
        )
    ]
    calls = []

    monkeypatch.setattr(run.config, "use_db_worklist_for_new", lambda: True)
    monkeypatch.setattr(
        run.worklist,
        "build_new_worklist",
        lambda version_id, source: calls.append((version_id, source)) or list(fake_items),
    )

    planned, ids = run._prepare_planned_cases(
        "new", _make_sync_result(7), source=worklist.DEFAULT_SOURCE
    )

    assert calls == [(7, worklist.DEFAULT_SOURCE)]
    assert set(planned.keys()) == {"TOKENONE"}
    assert ids["TOKENONE"] == 1


def test_full_mode_uses_db_worklist_when_flag_enabled(monkeypatch):
    fake_items = [
        worklist.WorkItem(
            case_id=9,
            action_token_norm="TOKENTWO",
            action_token_raw="tokentwo",
            title="Case Two",
            court="Court",
            category="Cat",
            judgment_date="2023-12-31",
            cause_number="456/2023",
            is_criminal=False,
            is_active=True,
            first_seen_version_id=3,
            last_seen_version_id=3,
            source=worklist.DEFAULT_SOURCE,
        )
    ]
    calls = []

    monkeypatch.setattr(run.config, "use_db_worklist_for_full", lambda: True)
    monkeypatch.setattr(
        run.worklist,
        "build_full_worklist",
        lambda version_id, source: calls.append((version_id, source)) or list(fake_items),
    )

    planned, ids = run._prepare_planned_cases(
        "full", _make_sync_result(11), source=worklist.DEFAULT_SOURCE
    )

    assert calls == [(11, worklist.DEFAULT_SOURCE)]
    assert set(planned.keys()) == {"TOKENTWO"}
    assert ids["TOKENTWO"] == 9


def test_legacy_planner_used_when_flags_disabled(monkeypatch, tmp_path):
    csv_path = tmp_path / "judgments_sample.csv"
    csv_path.write_text((Path(__file__).parent / "data" / "judgments_sample.csv").read_text())

    cases_index.load_cases_from_csv(str(csv_path))

    monkeypatch.setattr(run.config, "use_db_worklist_for_new", lambda: False)
    monkeypatch.setattr(run.config, "use_db_worklist_for_full", lambda: False)
    monkeypatch.setattr(
        run.worklist,
        "build_new_worklist",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db path should not be called")),
    )
    monkeypatch.setattr(
        run.worklist,
        "build_full_worklist",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("db path should not be called")),
    )

    planned_new, ids_new = run._prepare_planned_cases(
        "new", _make_sync_result(1), source=worklist.DEFAULT_SOURCE
    )
    planned_full, ids_full = run._prepare_planned_cases(
        "full", _make_sync_result(1), source=worklist.DEFAULT_SOURCE
    )

    assert len(planned_new) == len(cases_index.CASES_ALL)
    assert len(planned_full) == len(cases_index.CASES_ALL)
    assert ids_new == {}
    assert ids_full == {}
from pathlib import Path
