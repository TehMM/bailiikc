import threading
from pathlib import Path

from app.scraper import config, download_executor, run
from app.scraper.download_executor import DownloadExecutor


def test_single_worker_runs_inline():
    calls = []

    executor = DownloadExecutor(1)

    def fn():
        calls.append("called")
        return True, None

    ok, error = executor.submit("token", fn)
    executor.shutdown()

    assert ok is True
    assert error is None
    assert calls == ["called"]
    assert executor.peak_in_flight == 0


def test_peak_in_flight_tracks(monkeypatch):
    monkeypatch.setattr(config, "ENABLE_DOWNLOAD_EXECUTOR", True)
    executor = DownloadExecutor(2)
    start_event = threading.Event()
    release_event = threading.Event()

    def blocking() -> tuple[bool, None]:
        start_event.set()
        release_event.wait(timeout=5)
        return True, None

    thread = threading.Thread(target=lambda: executor.submit("one", blocking))
    thread.start()
    assert start_event.wait(timeout=5)

    ok, error = executor.submit("two", lambda: (True, None))
    release_event.set()
    thread.join(timeout=5)
    executor.shutdown()

    assert ok is True
    assert error is None
    assert executor.peak_in_flight >= 2


def test_queue_overflow_logs(monkeypatch):
    events: list[dict] = []
    monkeypatch.setattr(config, "ENABLE_DOWNLOAD_EXECUTOR", True)
    monkeypatch.setattr(config, "MAX_PENDING_DOWNLOADS", 1)
    monkeypatch.setattr(download_executor, "_scraper_event", lambda *args, **kwargs: events.append(kwargs))

    executor = DownloadExecutor(2)
    release_event = threading.Event()
    started_event = threading.Event()

    def blocking() -> tuple[bool, None]:
        started_event.set()
        release_event.wait(timeout=5)
        return True, None

    first_thread = threading.Thread(target=lambda: executor.submit("first", blocking))
    first_thread.start()
    assert started_event.wait(timeout=5)

    ok, error = executor.submit("second", lambda: (True, None))
    release_event.set()
    first_thread.join(timeout=5)
    executor.shutdown()

    assert ok is True
    assert error is None
    assert any(event.get("kind") == "queue_overflow" for event in events)


def test_queue_or_download_file_creates_stub_when_replay_skip_network(
    monkeypatch, tmp_path: Path
):
    dest = tmp_path / "stub.pdf"
    monkeypatch.setattr(config, "REPLAY_SKIP_NETWORK", True)

    ok, info = run.queue_or_download_file(
        "https://example.com/fake",
        dest,
        token="TESTTOKEN",
        max_retries=1,
        timeout=5,
    )

    assert ok is True
    assert isinstance(info, dict)
    assert dest.is_file()
    data = dest.read_bytes()
    assert data.startswith(run.REPLAY_STUB_PDF_HEADER)
    assert dest.stat().st_size >= run.box_client.MIN_PDF_BYTES


def test_log_download_executor_summary(monkeypatch):
    events: list[tuple[tuple, dict]] = []
    monkeypatch.setattr(run, "_scraper_event", lambda *args, **kwargs: events.append((args, kwargs)))
    monkeypatch.setattr(config, "MAX_PARALLEL_DOWNLOADS", 3)

    executor = DownloadExecutor(1)
    executor._peak_in_flight = 2  # type: ignore[attr-defined]

    run._log_download_executor_summary(executor)

    assert events
    args, kwargs = events[-1]
    assert args == ("state",)
    assert kwargs == {
        "phase": "download_executor",
        "kind": "summary",
        "peak_in_flight": 2,
        "max_parallel": 3,
    }
