from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("app")

# Required fields for a preset to be considered valid
_REQUIRED_FIELDS = {"id", "name", "sql"}


def _validate_presets(raw: list[Any]) -> list[dict[str, Any]]:
    """
    Validate a list of candidate presets.
    Skips (with a warning) any entry missing required fields.
    Returns the list of valid preset dicts.
    """
    valid: list[dict[str, Any]] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            logger.warning(
                "presets validation | skipping entry %d | reason=not a dict", i
            )
            continue
        missing = _REQUIRED_FIELDS - entry.keys()
        if missing:
            logger.warning(
                "presets validation | skipping preset index=%d id=%s | reason=missing fields: %s",
                i,
                entry.get("id", "<unknown>"),
                ", ".join(sorted(missing)),
            )
            continue
        valid.append(entry)
    return valid


def _resolve_json_path(base_dir: Path) -> Path | None:
    """
    Resolution order:
    1. AW_PRESETS_PATH environment variable
    2. Development location: <this file's directory>/presets.json
    3. Packaged fallback: base_dir/presets.json
    """
    # 1. Environment variable override
    env_path = os.getenv("AW_PRESETS_PATH", "").strip()
    if env_path:
        p = Path(env_path)
        if p.is_file():
            return p
        logger.warning(
            "presets load | AW_PRESETS_PATH set but file not found | path=%s", env_path
        )

    # 2. Dev location — sibling of this file
    dev_path = Path(__file__).resolve().parent / "presets.json"
    if dev_path.is_file():
        return dev_path

    # 3. Packaged fallback — next to EXE / repo root
    pkg_path = base_dir / "presets.json"
    if pkg_path.is_file():
        return pkg_path

    return None


def load_presets(base_dir: Path) -> list[dict[str, Any]]:
    """
    Load presets from JSON with fallback to Python PRESETS.

    Returns a list of valid preset dicts identical in structure to the
    Python PRESETS list so callers require no changes.
    """
    # Deferred import keeps the Python fallback decoupled from JSON loading
    from app.presets.doge import PRESETS as _python_presets  # noqa: PLC0415

    json_path = _resolve_json_path(base_dir)

    if json_path is None:
        logger.warning(
            "presets load failed | falling back to python presets | reason=presets.json not found in any search path"
        )
        return list(_python_presets)

    try:
        raw = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning(
            "presets load failed | falling back to python presets | reason=JSON parse error: %s | path=%s",
            exc,
            json_path,
        )
        return list(_python_presets)

    if not isinstance(raw, list):
        logger.warning(
            "presets load failed | falling back to python presets | reason=JSON root is not a list | path=%s",
            json_path,
        )
        return list(_python_presets)

    valid = _validate_presets(raw)

    if not valid:
        logger.warning(
            "presets load failed | falling back to python presets | reason=zero valid presets after validation | path=%s",
            json_path,
        )
        return list(_python_presets)

    logger.info(
        "presets load success | source=json | count=%d | path=%s",
        len(valid),
        json_path,
    )
    return valid
