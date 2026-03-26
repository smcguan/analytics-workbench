"""
File: services/session_log.py

Purpose
-------
Provide session-level event logging for Analytics Workbench.

This module tracks user actions (imports, queries, exports, AI calls) within a
single application session and can export the log as a structured JSON file.

Responsibilities
----------------
- manage a singleton SessionLog for the current app session
- record timestamped events with typed categories
- produce session summaries with event counts and durations
- auto-save to disk every 10 events for crash safety
- export completed sessions as JSON files

Important Notes
---------------
- Module-level singleton pattern — one session at a time.
- log_event() is safe to call before start_session(); it logs a warning and returns.
- _reset_session() is for test isolation only.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

try:
    from app.version import APP_VERSION
except ImportError:
    from backend.app.version import APP_VERSION

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------

class SessionEventType(str, Enum):
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    DATASET_IMPORT = "dataset_import"
    DATASET_DERIVED = "dataset_derived"
    DATASET_DELETE = "dataset_delete"
    REFERENCE_LOAD = "reference_load"
    REFERENCE_DELETE = "reference_delete"
    QUERY_RUN = "query_run"
    QUERY_SAVE = "query_save"
    EXPORT = "export"
    PASSPORT_EXPORT = "passport_export"
    RESULT_PASSPORT = "result_passport"
    INSIGHTS_GENERATED = "insights_generated"
    SUGGESTIONS_GENERATED = "suggestions_generated"
    AI_SQL_GENERATED = "ai_sql_generated"
    RESULT_NARRATIVE = "result_narrative"


@dataclass
class SessionEvent:
    event_type: SessionEventType
    timestamp: str  # ISO format
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class SessionLog:
    session_id: str              # UUID4
    started_at: str              # ISO format
    ended_at: str | None = None
    ai_mode: str = "cloud"       # "cloud" or "local"
    app_version: str = ""
    user: str = ""
    machine: str = ""
    name: str = ""               # User-assigned session name
    description: str = ""        # User-assigned session description
    events: list[SessionEvent] = field(default_factory=list)
    resume_state: dict = field(default_factory=dict)


# -----------------------------------------------------------------------------
# Module-level singleton
# -----------------------------------------------------------------------------

_current_session: SessionLog | None = None
_sessions_dir: Path | None = None


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

def start_session(ai_mode: str = "cloud") -> SessionLog:
    """Create and return a new session log singleton."""
    global _current_session

    now = datetime.now(timezone.utc).isoformat()

    # Resolve user with fallback
    try:
        user = os.getlogin()
    except OSError:
        user = os.environ.get("USERNAME", os.environ.get("USER", "unknown"))

    _current_session = SessionLog(
        session_id=str(uuid.uuid4()),
        started_at=now,
        ai_mode=ai_mode,
        app_version=APP_VERSION,
        user=user,
        machine=platform.node(),
    )

    log_event(SessionEventType.SESSION_START)
    return _current_session


def log_event(event_type: SessionEventType, details: dict | None = None) -> None:
    """Append an event to the current session.

    Safe to call before start_session() — logs a warning and returns.
    Auto-saves every 10 events for crash safety.
    """
    global _current_session

    if _current_session is None:
        logger.warning("log_event called before start_session — ignoring %s", event_type)
        return

    event = SessionEvent(
        event_type=event_type,
        timestamp=datetime.now(timezone.utc).isoformat(),
        details=details or {},
    )
    _current_session.events.append(event)

    # Auto-save every 10 events
    if len(_current_session.events) % 10 == 0:
        _auto_save()


def _build_resume_state(session: SessionLog) -> dict:
    """Derive resume state by scanning events in reverse.

    Finds the last dataset/SQL from query_run events, and the last
    active reference table (a load not followed by a delete).
    Also collects ALL imported datasets and ALL loaded references
    so multi-dataset sessions can be fully restored.
    """
    state: dict[str, Any] = {}

    # Collect all datasets and references (forward scan for complete picture)
    all_datasets: list[str] = []
    deleted_datasets: set[str] = set()
    all_references: list[dict] = []
    deleted_references: set[str] = set()

    for event in session.events:
        if event.event_type in (SessionEventType.DATASET_IMPORT, SessionEventType.DATASET_DERIVED):
            name = event.details.get("dataset", "")
            if name and name not in all_datasets:
                all_datasets.append(name)
            deleted_datasets.discard(name)
        if event.event_type == SessionEventType.DATASET_DELETE:
            deleted_datasets.add(event.details.get("dataset", ""))
        if event.event_type == SessionEventType.REFERENCE_LOAD:
            ref = {
                "name": event.details.get("reference_name", ""),
                "library_source": event.details.get("source", ""),
            }
            if ref["name"] and ref["name"] not in [r["name"] for r in all_references]:
                all_references.append(ref)
            deleted_references.discard(ref["name"])
        if event.event_type == SessionEventType.REFERENCE_DELETE:
            deleted_references.add(event.details.get("reference_name", ""))

    state["all_datasets"] = [d for d in all_datasets if d not in deleted_datasets]
    state["all_references"] = [r for r in all_references if r["name"] not in deleted_references]

    # Reverse scan for last active dataset/SQL/reference
    for event in reversed(session.events):
        if event.event_type == SessionEventType.QUERY_RUN and "dataset" not in state:
            state["dataset"] = event.details.get("dataset", "")
            state["last_sql"] = event.details.get("sql", "")
        # Fall back to DATASET_IMPORT if no QUERY_RUN event exists (user imported
        # but never ran SQL before saving — still need to restore the dataset).
        if event.event_type == SessionEventType.DATASET_IMPORT and "dataset" not in state:
            state["dataset"] = event.details.get("dataset", "")
        if event.event_type == SessionEventType.AI_SQL_GENERATED and "last_question" not in state:
            state["last_question"] = event.details.get("question", "")
        if event.event_type == SessionEventType.REFERENCE_LOAD and "reference" not in state:
            state["reference"] = {
                "name": event.details.get("reference_name", ""),
                "library_source": event.details.get("source", ""),
            }
        if event.event_type == SessionEventType.REFERENCE_DELETE:
            if "reference" not in state:
                state["reference"] = None  # was deleted, no active reference

    return state


def end_session() -> SessionLog | None:
    """End the current session, add a summary event, and return the log."""
    global _current_session

    if _current_session is None:
        logger.warning("end_session called but no session is active")
        return None

    _current_session.ended_at = datetime.now(timezone.utc).isoformat()

    # Build resume state before adding the SESSION_END event
    _current_session.resume_state = _build_resume_state(_current_session)

    summary = session_summary()
    log_event(SessionEventType.SESSION_END, details={
        "total_events": summary["event_count"],
        "total_queries": summary["queries_run"],
        "datasets_used": summary["datasets_used"],
    })

    session = _current_session
    return session


def get_current_session() -> SessionLog | None:
    """Return the current session singleton, or None if not started."""
    return _current_session


def _sanitize_filename(name: str) -> str:
    """Sanitize a user-provided name for use as a filename."""
    import re
    clean = name.strip()
    clean = re.sub(r'[<>:"/\\|?*]', '', clean)  # remove filesystem-unsafe chars
    clean = re.sub(r'\s+', '_', clean)  # spaces to underscores
    clean = re.sub(r'_+', '_', clean).strip('_')  # collapse multiple underscores
    return clean[:100] if clean else ""  # cap length


def _unique_filepath(directory: Path, base_name: str, ext: str = ".json") -> Path:
    """Return a unique filepath, appending _2, _3 etc. if file exists."""
    candidate = directory / f"{base_name}{ext}"
    if not candidate.exists():
        return candidate
    counter = 2
    while True:
        candidate = directory / f"{base_name}_{counter}{ext}"
        if not candidate.exists():
            return candidate
        counter += 1


def export_session(sessions_dir: Path) -> Path | None:
    """Write the current session to a JSON file on disk.

    Returns the path to the written file, or None if no session exists.
    If the session has a name, uses it as the filename (sanitized).
    Otherwise falls back to session_<uuid>_<date>.json.
    """
    if _current_session is None:
        logger.warning("export_session called but no session is active")
        return None

    sessions_dir = sessions_dir.resolve()
    sessions_dir.mkdir(parents=True, exist_ok=True)

    # Build resume state before serializing
    _current_session.resume_state = _build_resume_state(_current_session)

    # Use session name as filename if available.
    # Named sessions OVERWRITE the existing file — do not append _2, _3 etc.
    # The user explicitly chose this name; creating duplicates causes the
    # "two sessions with same name" confusion in Retrieve Session.
    sanitized = _sanitize_filename(_current_session.name) if _current_session.name else ""
    if sanitized:
        filepath = sessions_dir / f"{sanitized}.json"
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        filename = f"session_{_current_session.session_id}_{date_str}.json"
        filepath = sessions_dir / filename

    data = asdict(_current_session)
    # Write with explicit flush + fsync to guarantee file is on disk
    # before we return. This prevents the "save succeeded but file
    # doesn't exist yet" issue on Windows.
    import os as _os
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.flush()
        _os.fsync(f.fileno())

    logger.info("Session exported to %s", filepath)
    return filepath


def session_summary() -> dict:
    """Return a summary dict of the current session."""
    if _current_session is None:
        return {
            "session_id": None,
            "started_at": None,
            "ended_at": None,
            "ai_mode": None,
            "event_count": 0,
            "events_by_type": {},
            "datasets_used": [],
            "queries_run": 0,
            "duration_seconds": None,
        }

    events_by_type: dict[str, int] = {}
    datasets_used: set[str] = set()
    queries_run = 0

    for ev in _current_session.events:
        type_val = ev.event_type.value if isinstance(ev.event_type, Enum) else ev.event_type
        events_by_type[type_val] = events_by_type.get(type_val, 0) + 1

        if ev.event_type == SessionEventType.QUERY_RUN:
            queries_run += 1

        # Collect dataset names from event details
        ds = ev.details.get("dataset")
        if ds:
            datasets_used.add(ds)

    duration_seconds: float | None = None
    if _current_session.ended_at:
        start = datetime.fromisoformat(_current_session.started_at)
        end = datetime.fromisoformat(_current_session.ended_at)
        duration_seconds = (end - start).total_seconds()

    return {
        "session_id": _current_session.session_id,
        "started_at": _current_session.started_at,
        "ended_at": _current_session.ended_at,
        "ai_mode": _current_session.ai_mode,
        "name": _current_session.name,
        "description": _current_session.description,
        "event_count": len(_current_session.events),
        "events_by_type": events_by_type,
        "datasets_used": sorted(datasets_used),
        "queries_run": queries_run,
        "duration_seconds": duration_seconds,
    }


def set_sessions_dir(path: Path) -> None:
    """Store the sessions directory for auto-save. Called from main.py at startup."""
    global _sessions_dir
    _sessions_dir = path.resolve()
    _sessions_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Sessions directory set to %s", _sessions_dir)


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------

def _auto_save() -> None:
    """Auto-save current session to disk for crash safety.

    Always writes to the UUID-based filename, never to the named file.
    This prevents auto-save from creating 'MySession_2.json' after the user
    has already explicitly saved 'MySession.json' via export_session().
    Named exports are only done via explicit export_session() calls.
    """
    if _sessions_dir is None or _current_session is None:
        return
    try:
        import os as _os
        _sessions_dir.mkdir(parents=True, exist_ok=True)
        _current_session.resume_state = _build_resume_state(_current_session)
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        filepath = _sessions_dir / f"session_{_current_session.session_id}_{date_str}.json"
        data = asdict(_current_session)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
            f.flush()
            _os.fsync(f.fileno())
        logger.info("Session auto-saved to %s", filepath)
    except Exception:
        logger.exception("Auto-save failed")


def _reset_session() -> None:
    """Clear the singleton session. For test use only."""
    global _current_session, _sessions_dir
    _current_session = None
    _sessions_dir = None
