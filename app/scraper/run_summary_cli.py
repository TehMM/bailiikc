from __future__ import annotations

"""CLI helper for printing run-level download summaries."""

import argparse
from typing import Sequence

from . import db_reporting


def _build_parser() -> argparse.ArgumentParser:
    """Return an argument parser for the run summary CLI."""

    parser = argparse.ArgumentParser(
        description="Show download summary for a scraper run.",
    )
    parser.add_argument(
        "--run-id",
        type=int,
        help="Run ID to summarise.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Summarise the most recent run.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for the run summary CLI."""

    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    run_id = args.run_id
    if args.latest:
        latest_fn = getattr(db_reporting, "latest_run_id", None)
        if callable(latest_fn) and run_id is None:
            run_id = latest_fn()
    if run_id is None:
        parser.error("You must provide --run-id or --latest")

    try:
        summary = db_reporting.summarise_downloads_for_run(run_id)
    except db_reporting.RunNotFoundError as exc:  # pragma: no cover - exercised via parser
        parser.error(str(exc))

    print(f"Run {summary.run_id}")
    for status, count in sorted(summary.status_counts.items()):
        print(f"  {status}: {count}")

    if summary.fail_reasons:
        print("\nFail reasons:")
        for code, count in sorted(summary.fail_reasons.items()):
            print(f"  {code}: {count}")

    if summary.skip_reasons:
        print("\nSkip reasons:")
        for code, count in sorted(summary.skip_reasons.items()):
            print(f"  {code}: {count}")

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
