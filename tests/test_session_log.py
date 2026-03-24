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


# ---------------------------------------------------------------------------
# ENDPOINT TESTS — POST /api/session/name
# ---------------------------------------------------------------------------

import pandas as pd
from fastapi.testclient import TestClient
import app.main as main_module
import app.services.session_log as app_session_log  # same module the app uses

INTERNAL_DATASET = "aw_test_internal"

def _create_test_parquet(ds_dir: Path, name: str = INTERNAL_DATASET) -> None:
    """Create a small parquet dataset for endpoint testing."""
    d = ds_dir / name
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([
        {"drug_name": "Keytruda", "amount": 8000},
        {"drug_name": "Opdivo", "amount": 6000},
    ])
    df.to_parquet(str(d / "source.parquet"), index=False)
    meta = {
        "row_count": 2,
        "column_count": 2,
        "columns": ["drug_name", "amount"],
        "original_type": "csv",
    }
    (d / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (d / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")


@pytest.fixture()
def endpoint_client(tmp_path):
    """TestClient with a clean session and a small test dataset.

    Uses app.services.session_log (same module instance as the running app)
    to reset and start a fresh session so endpoint tests see clean state.
    """
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    _create_test_parquet(ds_dir)

    orig_ds = main_module.DATASETS_DIR
    orig_ex = main_module.EXPORTS_DIR
    orig_wp = main_module.WORKSPACE_PATH
    main_module.DATASETS_DIR = ds_dir
    main_module.EXPORTS_DIR = tmp_path / "_exports"
    main_module.EXPORTS_DIR.mkdir(exist_ok=True)
    main_module.WORKSPACE_PATH = tmp_path / "workspace.json"

    # Reset and start a fresh session using the APP's module instance
    app_session_log._reset_session()
    app_session_log.start_session()

    with TestClient(main_module.app) as c:
        yield c

    app_session_log._reset_session()
    main_module.DATASETS_DIR = orig_ds
    main_module.EXPORTS_DIR = orig_ex
    main_module.WORKSPACE_PATH = orig_wp


def test_session_name_sets_name_and_description(endpoint_client):
    """POST /api/session/name with name and description sets them on the session."""
    resp = endpoint_client.post("/api/session/name", json={
        "name": "My Analysis",
        "description": "Investigating Part B spending",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "My Analysis"
    assert body["description"] == "Investigating Part B spending"

    # Verify via GET /api/session (uses same singleton as the app)
    session_resp = endpoint_client.get("/api/session")
    assert session_resp.status_code == 200
    data = session_resp.json()
    assert data["name"] == "My Analysis"
    assert data["description"] == "Investigating Part B spending"


def test_session_name_empty_name_allowed(endpoint_client):
    """POST /api/session/name with empty name is OK — clears the name."""
    # First set a name
    endpoint_client.post("/api/session/name", json={
        "name": "Temp Name",
        "description": "Temp",
    })
    # Now clear it
    resp = endpoint_client.post("/api/session/name", json={
        "name": "",
        "description": "",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == ""
    assert data["description"] == ""


def test_session_name_persists_in_export(endpoint_client):
    """Set name, call /api/session/export, verify exported JSON contains name and description."""
    endpoint_client.post("/api/session/name", json={
        "name": "Export Test Session",
        "description": "Testing export persistence",
    })
    resp = endpoint_client.get("/api/session/export")
    assert resp.status_code == 200
    data = resp.json()
    session_data = data["session"]
    assert session_data["name"] == "Export Test Session"
    assert session_data["description"] == "Testing export persistence"


def test_session_name_appears_in_session_endpoint(endpoint_client):
    """Set name, call GET /api/session, verify name/description in response."""
    endpoint_client.post("/api/session/name", json={
        "name": "Visible Session",
        "description": "Should appear in GET",
    })
    resp = endpoint_client.get("/api/session")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == "Visible Session"
    assert data["description"] == "Should appear in GET"


# ---------------------------------------------------------------------------
# SqlRequest.internal FLAG TESTS (Bug #13 regression)
# ---------------------------------------------------------------------------

def test_internal_sql_request_skips_session_logging(endpoint_client):
    """POST /api/sql with internal=true should NOT appear as a query_run event."""
    # Snapshot current query_run count before the request
    pre_resp = endpoint_client.get("/api/session")
    pre_events = pre_resp.json()["events"]
    pre_query_count = sum(1 for e in pre_events if e["event_type"] == "query_run")

    resp = endpoint_client.post("/api/sql", json={
        "dataset": INTERNAL_DATASET,
        "sql": "SELECT COUNT(*) AS n FROM dataset",
        "internal": True,
    })
    assert resp.status_code == 200

    post_resp = endpoint_client.get("/api/session")
    post_events = post_resp.json()["events"]
    post_query_count = sum(1 for e in post_events if e["event_type"] == "query_run")

    assert post_query_count == pre_query_count, (
        f"Internal query should not be logged, but query_run count changed "
        f"from {pre_query_count} to {post_query_count}"
    )


def test_normal_sql_request_logs_session_event(endpoint_client):
    """POST /api/sql with internal=false (or omitted) SHOULD appear as a query_run event."""
    # Snapshot current query_run count before the request
    pre_resp = endpoint_client.get("/api/session")
    pre_events = pre_resp.json()["events"]
    pre_query_count = sum(1 for e in pre_events if e["event_type"] == "query_run")

    resp = endpoint_client.post("/api/sql", json={
        "dataset": INTERNAL_DATASET,
        "sql": "SELECT COUNT(*) AS n FROM dataset",
    })
    assert resp.status_code == 200

    post_resp = endpoint_client.get("/api/session")
    post_events = post_resp.json()["events"]
    post_query_count = sum(1 for e in post_events if e["event_type"] == "query_run")

    assert post_query_count == pre_query_count + 1, (
        f"Normal query should be logged, but query_run count changed "
        f"from {pre_query_count} to {post_query_count} (expected {pre_query_count + 1})"
    )


def test_bug13_suggestions_do_not_log_query_run(endpoint_client):
    """Bug #13: opening suggestions should log suggestions_generated, not query_run.

    When analyst opens Suggestions, only 1 suggestions_generated event should
    appear — not a query_run for each suggestion in the list.
    """
    pre_resp = endpoint_client.get("/api/session")
    pre_events = pre_resp.json()["events"]
    pre_query_count = sum(1 for e in pre_events if e["event_type"] == "query_run")

    # Call suggestions endpoint (may fail if no OpenAI key, but the session
    # event is logged before the AI call returns)
    endpoint_client.get(f"/api/ai/suggest_questions?dataset={INTERNAL_DATASET}")

    post_resp = endpoint_client.get("/api/session")
    post_events = post_resp.json()["events"]
    post_query_count = sum(1 for e in post_events if e["event_type"] == "query_run")

    assert post_query_count == pre_query_count, (
        f"Suggestions fetch should not log query_run events, but count changed "
        f"from {pre_query_count} to {post_query_count}"
    )


def test_bug13_one_query_run_per_explicit_execution(endpoint_client):
    """Bug #13: running one SQL query should log exactly 1 query_run event."""
    pre_resp = endpoint_client.get("/api/session")
    pre_events = pre_resp.json()["events"]
    pre_query_count = sum(1 for e in pre_events if e["event_type"] == "query_run")

    # Run a single query — this simulates clicking Run SQL once
    endpoint_client.post("/api/sql", json={
        "dataset": INTERNAL_DATASET,
        "sql": "SELECT * FROM dataset LIMIT 5",
    })

    post_resp = endpoint_client.get("/api/session")
    post_events = post_resp.json()["events"]
    post_query_count = sum(1 for e in post_events if e["event_type"] == "query_run")

    assert post_query_count == pre_query_count + 1, (
        f"Expected exactly 1 new query_run, but count changed "
        f"from {pre_query_count} to {post_query_count}"
    )


# ===========================================================================
# WORKSPACE SNAPSHOT TESTS
# ===========================================================================

def test_workspace_save_and_get(endpoint_client):
    """POST /api/workspace saves, GET /api/workspace returns it."""
    resp = endpoint_client.post("/api/workspace", json={
        "dataset": INTERNAL_DATASET,
        "last_query": "SELECT 1",
        "last_tab": "query",
    })
    assert resp.status_code == 200

    resp = endpoint_client.get("/api/workspace")
    assert resp.status_code == 200
    ws = resp.json()["workspace"]
    assert ws is not None
    assert ws["dataset"]["name"] == INTERNAL_DATASET
    assert ws["last_query"] == "SELECT 1"


def test_workspace_delete(endpoint_client):
    """DELETE /api/workspace clears workspace.json."""
    endpoint_client.post("/api/workspace", json={"dataset": INTERNAL_DATASET})
    resp = endpoint_client.delete("/api/workspace")
    assert resp.status_code == 200

    resp = endpoint_client.get("/api/workspace")
    assert resp.json()["workspace"] is None


# ===========================================================================
# EXAMPLE CASE VALIDATION TESTS
# ===========================================================================

def test_example_cases_have_valid_metadata():
    """Every example case directory must have a valid metadata.json."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        meta_path = case_dir / "metadata.json"
        assert meta_path.exists(), f"Missing metadata.json in {case_dir.name}"
        meta = _json.loads(meta_path.read_text(encoding="utf-8"))
        assert "id" in meta, f"Missing 'id' in {case_dir.name}/metadata.json"
        assert "name" in meta, f"Missing 'name' in {case_dir.name}/metadata.json"
        assert "category" in meta, f"Missing 'category' in {case_dir.name}/metadata.json"


def test_example_cases_with_sessions_have_valid_session_json():
    """Example cases with has_session=true must have valid session.json."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        meta_path = case_dir / "metadata.json"
        if not meta_path.exists():
            continue
        meta = _json.loads(meta_path.read_text(encoding="utf-8"))
        if not meta.get("has_session"):
            continue

        session_path = case_dir / "session.json"
        assert session_path.exists(), f"has_session=true but no session.json in {case_dir.name}"
        session = _json.loads(session_path.read_text(encoding="utf-8"))
        events = session.get("events", [])
        assert len(events) >= 3, f"Too few events ({len(events)}) in {case_dir.name}/session.json"
        # Must have at least one query_run
        event_types = [e["event_type"] for e in events]
        assert "query_run" in event_types, f"No query_run events in {case_dir.name}/session.json"


def test_example_cases_have_data_files():
    """Example cases with a dataset_file must have the actual data file."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        meta_path = case_dir / "metadata.json"
        if not meta_path.exists():
            continue
        meta = _json.loads(meta_path.read_text(encoding="utf-8"))
        ds_file = meta.get("dataset_file")
        if ds_file:
            data_path = case_dir / "data" / ds_file
            assert data_path.exists(), f"Missing data file {ds_file} in {case_dir.name}/data/"


def test_example_cases_additional_datasets_exist():
    """Additional dataset files referenced in metadata must exist on disk."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        meta_path = case_dir / "metadata.json"
        if not meta_path.exists():
            continue
        meta = _json.loads(meta_path.read_text(encoding="utf-8"))
        for ds in meta.get("additional_datasets", []):
            data_path = case_dir / "data" / ds["file"]
            assert data_path.exists(), f"Missing additional dataset {ds['file']} in {case_dir.name}/data/"


def test_example_cases_reference_files_exist():
    """Reference table files listed in metadata must exist in reference/ dir."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        meta_path = case_dir / "metadata.json"
        if not meta_path.exists():
            continue
        meta = _json.loads(meta_path.read_text(encoding="utf-8"))
        ref_names = meta.get("reference_tables", [])
        if not ref_names:
            continue
        ref_dir = case_dir / "reference"
        assert ref_dir.exists(), f"reference/ dir missing in {case_dir.name} but reference_tables listed"
        for ref_name in ref_names:
            ref_file = ref_dir / (ref_name + ".csv")
            assert ref_file.exists(), f"Missing reference file {ref_name}.csv in {case_dir.name}/reference/"


def test_example_cases_session_event_types_valid():
    """All event types in session JSONs must be from the known set."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    valid_types = {
        "session_start", "session_end", "dataset_import", "query_run",
        "reference_load", "reference_delete", "export", "result_passport",
        "insights_generated", "suggestions_generated", "ai_sql_generated",
        "narration_only", "edit_panel_open", "edit_panel_choose",
        "edit_panel_check", "edit_panel_run", "inspect_schema",
        "inspect_preview", "ai_ask",
    }

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        session_path = case_dir / "session.json"
        if not session_path.exists():
            continue
        session = _json.loads(session_path.read_text(encoding="utf-8"))
        for i, ev in enumerate(session.get("events", [])):
            etype = ev.get("event_type", "")
            assert etype in valid_types, (
                f"Unknown event_type '{etype}' at step {i+1} in {case_dir.name}/session.json"
            )


def test_example_cases_session_query_sql_present():
    """Every query_run event must have non-empty SQL in details."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        session_path = case_dir / "session.json"
        if not session_path.exists():
            continue
        session = _json.loads(session_path.read_text(encoding="utf-8"))
        for i, ev in enumerate(session.get("events", [])):
            if ev["event_type"] == "query_run":
                sql = (ev.get("details") or {}).get("sql", "")
                assert sql.strip(), (
                    f"Empty SQL in query_run at step {i+1} in {case_dir.name}/session.json"
                )


def test_example_cases_ai_ask_events_have_question():
    """Every ai_ask event must have a non-empty question."""
    import json as _json
    cases_dir = Path("data/example_cases")
    if not cases_dir.exists():
        pytest.skip("No example_cases directory")

    for case_dir in cases_dir.iterdir():
        if not case_dir.is_dir():
            continue
        session_path = case_dir / "session.json"
        if not session_path.exists():
            continue
        session = _json.loads(session_path.read_text(encoding="utf-8"))
        for i, ev in enumerate(session.get("events", [])):
            if ev["event_type"] == "ai_ask":
                question = (ev.get("details") or {}).get("question", "")
                assert question.strip(), (
                    f"Empty question in ai_ask at step {i+1} in {case_dir.name}/session.json"
                )


def test_real_estate_case_structure():
    """Real estate example case has complete structure."""
    import json as _json
    case_dir = Path("data/example_cases/real_estate_market_analysis")
    if not case_dir.exists():
        pytest.skip("Real estate case not found")

    # Metadata
    meta = _json.loads((case_dir / "metadata.json").read_text(encoding="utf-8"))
    assert meta["id"] == "real_estate_market_analysis"
    assert meta["category"] == "Real Estate"
    assert meta["has_session"] is True
    assert len(meta.get("additional_datasets", [])) == 1
    assert len(meta.get("reference_tables", [])) == 2

    # Data files
    assert (case_dir / "data" / "austin_listings.csv").exists()
    assert (case_dir / "data" / "denver_listings.csv").exists()
    assert (case_dir / "reference" / "neighborhood_tier_map.csv").exists()
    assert (case_dir / "reference" / "property_benchmarks.csv").exists()

    # Session structure
    session = _json.loads((case_dir / "session.json").read_text(encoding="utf-8"))
    events = session["events"]
    types = [e["event_type"] for e in events]

    # Must have the key step types
    assert "inspect_schema" in types, "Missing inspect_schema step"
    assert "insights_generated" in types, "Missing insights_generated step"
    assert "ai_ask" in types, "Missing ai_ask step"
    assert "edit_panel_open" in types, "Missing edit_panel_open step"
    assert "edit_panel_check" in types, "Missing edit_panel_check step"
    assert types.count("ai_ask") == 2, "Expected 2 ai_ask steps"
    assert types.count("dataset_import") == 2, "Expected 2 dataset_import steps"


def test_real_estate_query_sql_executable():
    """All query_run SQL in the real estate session must execute against the CSV data."""
    import json as _json
    import duckdb

    case_dir = Path("data/example_cases/real_estate_market_analysis")
    if not case_dir.exists():
        pytest.skip("Real estate case not found")

    session = _json.loads((case_dir / "session.json").read_text(encoding="utf-8"))
    austin_csv = str(case_dir / "data" / "austin_listings.csv")
    denver_csv = str(case_dir / "data" / "denver_listings.csv")
    tier_csv = str(case_dir / "reference" / "neighborhood_tier_map.csv")

    con = duckdb.connect()

    for i, ev in enumerate(session["events"]):
        if ev["event_type"] != "query_run":
            continue
        d = ev["details"]
        sql = d["sql"]
        ds = d.get("dataset", "austin_listings")
        csv_path = denver_csv if "denver" in ds else austin_csv

        # Rewrite table names to CSV paths
        test_sql = sql.replace("dataset", f"read_csv_auto('{csv_path}')")
        test_sql = test_sql.replace("neighborhood_tier_map", f"read_csv_auto('{tier_csv}')")

        result = con.execute(test_sql).fetchall()
        expected = d.get("row_count") or ev.get("baseline", {}).get("expected_row_count")
        if expected is not None:
            assert len(result) == expected, (
                f"Step {i+1} ({ds}): expected {expected} rows, got {len(result)}"
            )


def test_session_description_persists(endpoint_client):
    """Session description set via /api/session/name persists in session."""
    client = endpoint_client

    # Set name and description
    resp = client.post("/api/session/name", json={
        "name": "Test Workflow",
        "description": "A test description for the About panel"
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["description"] == "A test description for the About panel"

    # Check it appears in /api/session
    session_resp = client.get("/api/session")
    session_data = session_resp.json()
    assert session_data.get("description") == "A test description for the About panel"
