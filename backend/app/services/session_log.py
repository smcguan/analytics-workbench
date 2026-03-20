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
    """
    state: dict[str, Any] = {}

    for event in reversed(session.events):
        if event.event_type == SessionEventType.QUERY_RUN and "dataset" not in state:
            state["dataset"] = event.details.get("dataset", "")
            state["last_sql"] = event.details.get("sql", "")
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


def export_session(sessions_dir: Path) -> Path | None:
    """Write the current session to a JSON file on disk.

    Returns the path to the written file, or None if no session exists.
    """
    if _current_session is None:
        logger.warning("export_session called but no session is active")
        return None

    sessions_dir = sessions_dir.resolve()
    sessions_dir.mkdir(parents=True, exist_ok=True)

    # Build resume state before serializing
    _current_session.resume_state = _build_resume_state(_current_session)

    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    filename = f"session_{_current_session.session_id}_{date_str}.json"
    filepath = sessions_dir / filename

    data = asdict(_current_session)
    filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")

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
    """Auto-save current session state to disk if sessions_dir is configured."""
    if _sessions_dir is None:
        return
    try:
        export_session(_sessions_dir)
    except Exception:
        logger.exception("Auto-save failed")


def _reset_session() -> None:
    """Clear the singleton session. For test use only."""
    global _current_session, _sessions_dir
    _current_session = None
    _sessions_dir = None
