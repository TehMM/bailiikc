from __future__ import annotations

import json
from typing import Any, Mapping

from . import db, sources


def create_run_with_source(
    *,
    trigger: str,
    mode: str,
    csv_version_id: int,
    target_source: str | None,
    extra_params: Mapping[str, Any] | None = None,
) -> int:
    """Create a run row with a normalised ``target_source`` field.

    ``extra_params`` are merged into ``params_json`` and may include legacy
    keys; ``target_source`` wins over any existing value in ``extra_params``.
    """

    params: dict[str, Any] = {}
    if extra_params:
        params.update(extra_params)

    raw_source = params.pop("target_source", target_source)
    if raw_source is not None:
        params["target_source"] = sources.coerce_source(str(raw_source))

    params_json = json.dumps(params, sort_keys=True)

    return db.create_run(
        trigger=trigger,
        mode=mode,
        csv_version_id=csv_version_id,
        params_json=params_json,
    )


__all__ = ["create_run_with_source"]
