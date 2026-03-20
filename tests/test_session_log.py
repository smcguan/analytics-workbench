"""
test_session_log.py — unit tests for the session logging service

Covers:
  start_session, log_event, end_session, get_current_session,
  export_session, session_summary, _reset_session, auto-save

Run from project root:
    pytest tests/test_session_log.py -v
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from backend.app.services.session_log import (
    SessionEventType,
    _build_resume_state,
    _reset_session,
    end_session,
    export_session,
    get_current_session,
    log_event,
    session_summary,
    set_sessions_dir,
    start_session,
)
from backend.app.version import APP_VERSION


# ---------------------------------------------------------------------------
# FIXTURE — reset session singleton between tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clean_session():
    """Ensure each test starts with a clean session singleton."""
    _reset_session()
    yield
    _reset_session()


# ---------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------

def test_start_session_creates_log():
    """start_session populates session_id and started_at."""
    session = start_session()
    assert session.session_id is not None
    assert len(session.session_id) == 36  # UUID4 format
    assert session.started_at is not None
    # Verify ISO format parses without error
    datetime.fromisoformat(session.started_at)


def test_start_session_sets_version():
    """app_version matches the value from version.py."""
    session = start_session()
    assert session.app_version == APP_VERSION


def test_start_session_sets_user_and_machine():
    """user and machine are populated with non-empty strings."""
    session = start_session()
    assert isinstance(session.user, str)
    assert len(session.user) > 0
    assert isinstance(session.machine, str)
    assert len(session.machine) > 0


def test_log_event_appends():
    """Logging 3 events results in 3 items in the events list (plus SESSION_START)."""
    start_session()
    log_event(SessionEventType.QUERY_RUN, {"dataset": "test_ds", "sql": "SELECT 1"})
    log_event(SessionEventType.EXPORT, {"format": "xlsx"})
    log_event(SessionEventType.DATASET_IMPORT, {"dataset": "new_ds"})
    session = get_current_session()
    # 1 SESSION_START + 3 logged = 4 total
    assert len(session.events) == 4


def test_log_event_has_timestamp():
    """Each logged event has a valid ISO timestamp."""
    start_session()
    log_event(SessionEventType.QUERY_RUN)
    log_event(SessionEventType.EXPORT)
    session = get_current_session()
    for ev in session.events:
        assert ev.timestamp is not None
        datetime.fromisoformat(ev.timestamp)


def test_log_event_preserves_details():
    """The details dict is preserved exactly as passed."""
    start_session()
    details = {"dataset": "my_dataset", "rows": 42, "nested": {"key": "value"}}
    log_event(SessionEventType.QUERY_RUN, details)
    session = get_current_session()
    # Last event is the one we just logged
    last_event = session.events[-1]
    assert last_event.details == details


def test_log_event_before_start_is_safe():
    """Calling log_event before start_session does not crash."""
    # No start_session() call
    log_event(SessionEventType.QUERY_RUN, {"sql": "SELECT 1"})
    assert get_current_session() is None


def test_end_session_sets_ended_at():
    """end_session populates the ended_at timestamp."""
    start_session()
    session = end_session()
    assert session is not None
    assert session.ended_at is not None
    datetime.fromisoformat(session.ended_at)


def test_end_session_adds_summary_event():
    """end_session appends a SESSION_END event with summary details."""
    start_session()
    log_event(SessionEventType.QUERY_RUN, {"dataset": "ds1"})
    log_event(SessionEventType.QUERY_RUN, {"dataset": "ds2"})
    session = end_session()
    last_event = session.events[-1]
    assert last_event.event_type == SessionEventType.SESSION_END
    assert "total_events" in last_event.details
    assert "total_queries" in last_event.details
    assert "datasets_used" in last_event.details


def test_export_session_writes_file(tmp_path: Path):
    """export_session creates a JSON file with correct structure."""
    start_session()
    log_event(SessionEventType.QUERY_RUN)
    filepath = export_session(tmp_path)
    assert filepath is not None
    assert filepath.exists()
    data = json.loads(filepath.read_text(encoding="utf-8"))
    assert "session_id" in data
    assert "started_at" in data
    assert "events" in data
    assert isinstance(data["events"], list)


def test_export_session_filename_format(tmp_path: Path):
    """Exported filename matches session_{id}_{date}.json pattern."""
    session = start_session()
    filepath = export_session(tmp_path)
    pattern = rf"^session_{re.escape(session.session_id)}_\d{{8}}\.json$"
    assert re.match(pattern, filepath.name), f"Filename {filepath.name} does not match expected pattern"


def test_session_summary_counts():
    """session_summary reports correct event_count, queries_run, datasets_used."""
    start_session()
    log_event(SessionEventType.QUERY_RUN, {"dataset": "ds_alpha"})
    log_event(SessionEventType.QUERY_RUN, {"dataset": "ds_beta"})
    log_event(SessionEventType.EXPORT, {"dataset": "ds_alpha"})
    summary = session_summary()
    # 1 SESSION_START + 3 logged = 4
    assert summary["event_count"] == 4
    assert summary["queries_run"] == 2
    assert sorted(summary["datasets_used"]) == ["ds_alpha", "ds_beta"]


def test_session_summary_events_by_type():
    """events_by_type dict has correct counts per event type."""
    start_session()
    log_event(SessionEventType.QUERY_RUN)
    log_event(SessionEventType.QUERY_RUN)
    log_event(SessionEventType.EXPORT)
    summary = session_summary()
    assert summary["events_by_type"]["session_start"] == 1
    assert summary["events_by_type"]["query_run"] == 2
    assert summary["events_by_type"]["export"] == 1


def test_reset_clears_session():
    """After _reset_session, get_current_session returns None."""
    start_session()
    assert get_current_session() is not None
    _reset_session()
    assert get_current_session() is None


def test_auto_save_triggers(tmp_path: Path):
    """After 10 events total, auto-save writes a file to disk."""
    set_sessions_dir(tmp_path)
    start_session()
    # SESSION_START is event 1; log 9 more to hit 10
    for i in range(9):
        log_event(SessionEventType.QUERY_RUN, {"iteration": i})
    # At 10 events, auto-save should have triggered
    json_files = list(tmp_path.glob("session_*.json"))
    assert len(json_files) >= 1, "Auto-save did not create a file after 10 events"
    data = json.loads(json_files[0].read_text(encoding="utf-8"))
    assert data["session_id"] == get_current_session().session_id


# ---------------------------------------------------------------------------
# RESUME STATE TESTS
# ---------------------------------------------------------------------------

def test_build_resume_state_extracts_last_query():
    """_build_resume_state picks dataset and SQL from the last query_run event."""
    session = start_session()
    log_event(SessionEventType.QUERY_RUN, {"dataset": "ds_old", "sql": "SELECT 1"})
    log_event(SessionEventType.QUERY_RUN, {"dataset": "ds_new", "sql": "SELECT 2"})
    state = _build_resume_state(session)
    assert state["dataset"] == "ds_new"
    assert state["last_sql"] == "SELECT 2"


def test_build_resume_state_extracts_reference():
    """_build_resume_state finds the last active reference load."""
    session = start_session()
    log_event(SessionEventType.REFERENCE_LOAD, {
        "reference_name": "ira_list",
        "source": "ira_list.csv",
    })
    state = _build_resume_state(session)
    assert state["reference"]["name"] == "ira_list"
    assert state["reference"]["library_source"] == "ira_list.csv"


def test_build_resume_state_no_queries_returns_empty():
    """_build_resume_state returns empty dict when no query_run events exist."""
    session = start_session()
    log_event(SessionEventType.EXPORT, {"format": "xlsx"})
    state = _build_resume_state(session)
    assert "dataset" not in state
    assert "last_sql" not in state


def test_build_resume_state_deleted_reference_returns_none():
    """If a reference was loaded then deleted, resume_state.reference is None."""
    session = start_session()
    log_event(SessionEventType.REFERENCE_LOAD, {
        "reference_name": "myref",
        "source": "myref.csv",
    })
    log_event(SessionEventType.REFERENCE_DELETE, {"reference_name": "myref"})
    state = _build_resume_state(session)
    assert state["reference"] is None


def test_export_includes_resume_state(tmp_path: Path):
    """Exported session JSON includes a resume_state key with dataset info."""
    start_session()
    log_event(SessionEventType.QUERY_RUN, {"dataset": "test_ds", "sql": "SELECT 42"})
    filepath = export_session(tmp_path)
    assert filepath is not None
    data = json.loads(filepath.read_text(encoding="utf-8"))
    assert "resume_state" in data
    assert data["resume_state"]["dataset"] == "test_ds"
    assert data["resume_state"]["last_sql"] == "SELECT 42"
