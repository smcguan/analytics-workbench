from __future__ import annotations
import os
from pathlib import Path

APP_NAME = "AnalyticsWorkbench"

def _local_appdata_dir() -> Path:
    lad = os.environ.get("LOCALAPPDATA")
    if not lad:
        # last resort fallback
        return Path.home() / "AppData" / "Local"
    return Path(lad)

def get_aw_home() -> Path:
    # Allow override for dev/testing
    env = os.environ.get("AW_HOME")
    base = Path(env) if env else (_local_appdata_dir() / APP_NAME)
    base.mkdir(parents=True, exist_ok=True)
    return base

def datasets_root() -> Path:
    p = get_aw_home() / "data" / "datasets"
    p.mkdir(parents=True, exist_ok=True)
    return p

def exports_root() -> Path:
    p = get_aw_home() / "exports"
    p.mkdir(parents=True, exist_ok=True)
    return p

def logs_root() -> Path:
    p = get_aw_home() / "logs"
    p.mkdir(parents=True, exist_ok=True)
    return p
