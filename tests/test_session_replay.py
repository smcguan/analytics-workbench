"""
test_session_replay.py — tests for the session replay engine

Covers:
  Session file listing, loading, schema checking
  Replay of query_run, reference_load, reference_delete events
  Skipping non-replayable events
  Baselines: annotation and comparison
  Stop-on-failure mode
  Full session end-to-end

Run from project root:
    pytest tests/test_session_replay.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from app.services.session_replay import SessionReplayEngine


# ===========================================================================
# HELPERS
# ===========================================================================

def _create_test_dataset(ds_dir: Path, name: str = "test_ds") -> Path:
    """Create a small parquet dataset for testing."""
    d = ds_dir / name
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([
        {"drug_name": "Keytruda", "amount": 8000},
        {"drug_name": "Opdivo", "amount": 6000},
        {"drug_name": "Eliquis", "amount": 5000},
        {"drug_name": "Humira", "amount": 7000},
        {"drug_name": "Stelara", "amount": 4000},
    ])
    df.to_parquet(str(d / "source.parquet"), index=False)
    meta = {"row_count": 5, "column_count": 2}
    (d / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return d


def _create_reference(ref_dir: Path, name: str, rows: list[dict]) -> Path:
    """Create a small reference table parquet."""
    d = ref_dir / name
    d.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_parquet(str(d / "source.parquet"), index=False)
    meta = {"reference_name": name, "row_count": len(rows)}
    (d / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    return d


def _create_library_csv(lib_dir: Path, filename: str, rows: list[dict]) -> Path:
    """Create a CSV in the reference library directory."""
    df = pd.DataFrame(rows)
    path = lib_dir / filename
    df.to_csv(str(path), index=False)
    return path


def _write_session(sessions_dir: Path, filename: str, data: dict) -> Path:
    """Write a session JSON file."""
    filepath = sessions_dir / filename
    filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return filepath


def _make_session(events: list[dict], **kwargs) -> dict:
    """Build a minimal session dict."""
    return {
        "session_id": kwargs.get("session_id", "test-session-001"),
        "started_at": "2026-03-19T10:00:00+00:00",
        "ended_at": None,
        "ai_mode": "cloud",
        "app_version": "1.5.6",
        "user": "test",
        "machine": "test-pc",
        "events": events,
        **{k: v for k, v in kwargs.items() if k != "session_id"},
    }


def _make_engine(tmp_path: Path) -> tuple[SessionReplayEngine, dict]:
    """Set up dirs and return (engine, dirs_dict)."""
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    ref_dir = tmp_path / "references"
    ref_dir.mkdir()
    lib_dir = tmp_path / "library"
    lib_dir.mkdir()
    sess_dir = tmp_path / "sessions"
    sess_dir.mkdir()

    engine = SessionReplayEngine(ds_dir, ref_dir, lib_dir, sess_dir)
    return engine, {
        "ds_dir": ds_dir,
        "ref_dir": ref_dir,
        "lib_dir": lib_dir,
        "sess_dir": sess_dir,
    }


# ===========================================================================
# 1. test_list_session_files
# ===========================================================================

def test_list_session_files(tmp_path):
    """list_session_files finds JSON files in sessions_dir."""
    engine, dirs = _make_engine(tmp_path)
    _write_session(dirs["sess_dir"], "s1.json", _make_session([], session_id="aaa"))
    _write_session(dirs["sess_dir"], "s2.json", _make_session([
        {"event_type": "session_start", "timestamp": "2026-03-19T10:00:00+00:00", "details": {}},
    ], session_id="bbb"))

    files = engine.list_session_files()
    assert len(files) == 2
    filenames = [f["filename"] for f in files]
    assert "s1.json" in filenames
    assert "s2.json" in filenames
    bbb = next(f for f in files if f["session_id"] == "bbb")
    assert bbb["event_count"] == 1


# ===========================================================================
# 2. test_list_session_files_empty
# ===========================================================================

def test_list_session_files_empty(tmp_path):
    """Returns empty list when no session files exist."""
    engine, _ = _make_engine(tmp_path)
    assert engine.list_session_files() == []


# ===========================================================================
# 3. test_load_session_valid
# ===========================================================================

def test_load_session_valid(tmp_path):
    """Loads and parses a valid session JSON file."""
    engine, dirs = _make_engine(tmp_path)
    session_data = _make_session([
        {"event_type": "session_start", "timestamp": "2026-03-19T10:00:00+00:00", "details": {}},
    ])
    _write_session(dirs["sess_dir"], "test.json", session_data)

    loaded = engine.load_session("test.json")
    assert loaded["session_id"] == "test-session-001"
    assert len(loaded["events"]) == 1


# ===========================================================================
# 4. test_load_session_not_found
# ===========================================================================

def test_load_session_not_found(tmp_path):
    """Raises FileNotFoundError for missing session file."""
    engine, _ = _make_engine(tmp_path)
    with pytest.raises(FileNotFoundError):
        engine.load_session("nonexistent.json")


# ===========================================================================
# 5. test_schema_check_pass
# ===========================================================================

def test_schema_check_pass(tmp_path):
    """Schema check passes when all required columns exist."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    session_data = _make_session([], schema_requirements={
        "datasets": {"test_ds": ["drug_name", "amount"]}
    })
    status, mismatches = engine.check_schema(session_data)
    assert status == "pass"
    assert mismatches == []


# ===========================================================================
# 6. test_schema_check_missing_dataset
# ===========================================================================

def test_schema_check_missing_dataset(tmp_path):
    """Schema check fails when dataset is not found."""
    engine, dirs = _make_engine(tmp_path)

    session_data = _make_session([], schema_requirements={
        "datasets": {"missing_ds": ["col1"]}
    })
    status, mismatches = engine.check_schema(session_data)
    assert status == "fail"
    assert len(mismatches) == 1
    assert mismatches[0]["issue"] == "dataset_not_found"


# ===========================================================================
# 7. test_schema_check_missing_columns
# ===========================================================================

def test_schema_check_missing_columns(tmp_path):
    """Schema check reports which columns are missing."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    session_data = _make_session([], schema_requirements={
        "datasets": {"test_ds": ["drug_name", "nonexistent_col"]}
    })
    status, mismatches = engine.check_schema(session_data)
    assert status == "fail"
    assert len(mismatches) == 1
    assert "nonexistent_col" in mismatches[0]["missing"]


# ===========================================================================
# 8. test_schema_check_no_requirements
# ===========================================================================

def test_schema_check_no_requirements(tmp_path):
    """Schema check returns 'skipped' when no requirements block."""
    engine, _ = _make_engine(tmp_path)
    session_data = _make_session([])
    status, mismatches = engine.check_schema(session_data)
    assert status == "skipped"
    assert mismatches == []


# ===========================================================================
# 9. test_schema_check_extra_columns_ok
# ===========================================================================

def test_schema_check_extra_columns_ok(tmp_path):
    """Extra columns in the dataset do not cause failure."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    session_data = _make_session([], schema_requirements={
        "datasets": {"test_ds": ["drug_name"]}  # amount is extra, not checked
    })
    status, mismatches = engine.check_schema(session_data)
    assert status == "pass"
    assert mismatches == []


# ===========================================================================
# 10. test_skip_non_replayable_events
# ===========================================================================

def test_skip_non_replayable_events(tmp_path):
    """Non-replayable events are logged as 'skip'."""
    engine, dirs = _make_engine(tmp_path)
    events = [
        {"event_type": "session_start", "timestamp": "T", "details": {}},
        {"event_type": "export", "timestamp": "T", "details": {"format": "tsv"}},
        {"event_type": "insights_generated", "timestamp": "T", "details": {}},
        {"event_type": "ai_sql_generated", "timestamp": "T", "details": {}},
        {"event_type": "dataset_import", "timestamp": "T", "details": {}},
    ]
    _write_session(dirs["sess_dir"], "skip.json", _make_session(events))

    report = engine.replay("skip.json")
    assert report.skipped == 5
    assert report.passed == 0
    assert report.failed == 0
    assert report.errors == 0
    assert report.overall_status == "pass"


# ===========================================================================
# 11. test_replay_query_run_pass
# ===========================================================================

def test_replay_query_run_pass(tmp_path):
    """Query executes and row_count matches recorded value."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT * FROM dataset",
                "row_count": 5,
                "elapsed_seconds": 0.01,
            },
        },
    ]
    _write_session(dirs["sess_dir"], "pass.json", _make_session(events))

    report = engine.replay("pass.json")
    assert report.passed == 1
    assert report.failed == 0
    assert report.overall_status == "pass"
    assert report.steps[0].actual_row_count == 5


# ===========================================================================
# 12. test_replay_query_run_fail_rowcount
# ===========================================================================

def test_replay_query_run_fail_rowcount(tmp_path):
    """Row count mismatch is reported as failure."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT * FROM dataset",
                "row_count": 999,  # Wrong — actual is 5
            },
        },
    ]
    _write_session(dirs["sess_dir"], "fail.json", _make_session(events))

    report = engine.replay("fail.json")
    assert report.failed == 1
    assert report.overall_status == "fail"
    assert report.steps[0].expected_row_count == 999
    assert report.steps[0].actual_row_count == 5


# ===========================================================================
# 13. test_replay_query_run_sql_error
# ===========================================================================

def test_replay_query_run_sql_error(tmp_path):
    """Invalid SQL results in 'error' status."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELEKT BOGUS FROM dataset",
                "row_count": 1,
            },
        },
    ]
    _write_session(dirs["sess_dir"], "err.json", _make_session(events))

    report = engine.replay("err.json")
    assert report.errors == 1
    assert report.steps[0].status == "error"
    assert report.steps[0].error_detail is not None


# ===========================================================================
# 14. test_replay_query_with_reference
# ===========================================================================

def test_replay_query_with_reference(tmp_path):
    """Query with JOIN reference works when reference is loaded."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")
    _create_reference(dirs["ref_dir"], "myref", [
        {"drug_name": "Keytruda", "flag": 1},
        {"drug_name": "Opdivo", "flag": 1},
    ])

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT d.drug_name, r.flag FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name",
                "row_count": 2,
            },
        },
    ]
    _write_session(dirs["sess_dir"], "ref.json", _make_session(events))

    report = engine.replay("ref.json")
    assert report.passed == 1
    assert report.steps[0].actual_row_count == 2


# ===========================================================================
# 15. test_replay_reference_load_from_library
# ===========================================================================

def test_replay_reference_load_from_library(tmp_path):
    """reference_load with 'source' field loads from library CSV."""
    engine, dirs = _make_engine(tmp_path)
    _create_library_csv(dirs["lib_dir"], "ira_list.csv", [
        {"drug_name": "Keytruda", "round": 1},
    ])

    events = [
        {
            "event_type": "reference_load",
            "timestamp": "T",
            "details": {
                "reference_name": "ira_list",
                "row_count": 1,
                "source": "ira_list.csv",
            },
        },
    ]
    _write_session(dirs["sess_dir"], "lib.json", _make_session(events))

    report = engine.replay("lib.json")
    assert report.steps[0].status == "pass"
    # Verify reference was actually created
    ref_pq = dirs["ref_dir"] / "ira_list" / "source.parquet"
    assert ref_pq.exists()


# ===========================================================================
# 16. test_replay_reference_load_existing
# ===========================================================================

def test_replay_reference_load_existing(tmp_path):
    """reference_load passes when reference already exists on disk."""
    engine, dirs = _make_engine(tmp_path)
    _create_reference(dirs["ref_dir"], "existing_ref", [
        {"drug_name": "Keytruda", "flag": 1},
    ])

    events = [
        {
            "event_type": "reference_load",
            "timestamp": "T",
            "details": {
                "reference_name": "existing_ref",
                "row_count": 1,
            },
        },
    ]
    _write_session(dirs["sess_dir"], "exist.json", _make_session(events))

    report = engine.replay("exist.json")
    assert report.steps[0].status == "pass"


# ===========================================================================
# 17. test_replay_reference_load_missing
# ===========================================================================

def test_replay_reference_load_missing(tmp_path):
    """reference_load skips when source not found and reference not on disk."""
    engine, dirs = _make_engine(tmp_path)

    events = [
        {
            "event_type": "reference_load",
            "timestamp": "T",
            "details": {
                "reference_name": "gone_ref",
                "row_count": 1,
            },
        },
    ]
    _write_session(dirs["sess_dir"], "miss.json", _make_session(events))

    report = engine.replay("miss.json")
    assert report.steps[0].status == "skip"


# ===========================================================================
# 18. test_replay_full_session_all_pass
# ===========================================================================

def test_replay_full_session_all_pass(tmp_path):
    """End-to-end session with mixed events, all replayable ones pass."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {"event_type": "session_start", "timestamp": "T", "details": {}},
        {"event_type": "dataset_import", "timestamp": "T", "details": {"dataset": "test_ds"}},
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT COUNT(*) AS n FROM dataset",
                "row_count": 1,
            },
        },
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT drug_name FROM dataset WHERE amount > 6000",
                "row_count": 2,
            },
        },
        {"event_type": "export", "timestamp": "T", "details": {"format": "tsv"}},
    ]
    _write_session(dirs["sess_dir"], "full.json", _make_session(events))

    report = engine.replay("full.json")
    assert report.overall_status == "pass"
    assert report.passed == 2  # Two query_run events
    assert report.skipped == 3  # session_start, dataset_import, export


# ===========================================================================
# 19. test_replay_full_session_with_failure
# ===========================================================================

def test_replay_full_session_with_failure(tmp_path):
    """One query fails (wrong row_count), rest continue."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT * FROM dataset",
                "row_count": 999,  # Wrong
            },
        },
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT COUNT(*) AS n FROM dataset",
                "row_count": 1,  # Correct
            },
        },
    ]
    _write_session(dirs["sess_dir"], "mixed.json", _make_session(events))

    report = engine.replay("mixed.json")
    assert report.failed == 1
    assert report.passed == 1
    assert report.overall_status == "fail"


# ===========================================================================
# 20. test_replay_stop_on_failure
# ===========================================================================

def test_replay_stop_on_failure(tmp_path):
    """stop_on_failure=True stops at first failure."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT * FROM dataset",
                "row_count": 999,  # Wrong — should stop here
            },
        },
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT COUNT(*) AS n FROM dataset",
                "row_count": 1,
            },
        },
    ]
    _write_session(dirs["sess_dir"], "stop.json", _make_session(events))

    report = engine.replay("stop.json", stop_on_failure=True)
    assert report.failed == 1
    assert report.skipped == 1  # Second query was skipped
    assert report.passed == 0


# ===========================================================================
# 21. test_replay_no_baselines
# ===========================================================================

def test_replay_no_baselines(tmp_path):
    """Runs without baselines, uses event's recorded row_count."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT * FROM dataset WHERE amount > 7000",
                "row_count": 1,  # Only Keytruda (8000) matches
            },
        },
    ]
    session_data = _make_session(events)
    # No "baselines" key at all
    assert "baselines" not in session_data
    _write_session(dirs["sess_dir"], "nobl.json", session_data)

    report = engine.replay("nobl.json")
    assert report.passed == 1
    assert report.steps[0].expected_row_count == 1


# ===========================================================================
# 22. test_annotate_baselines
# ===========================================================================

def test_annotate_baselines(tmp_path):
    """annotate_baselines runs queries and records actual row counts."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {"event_type": "session_start", "timestamp": "T", "details": {}},
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT * FROM dataset",
                "row_count": 5,
            },
        },
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT COUNT(*) AS n FROM dataset",
                "row_count": 1,
            },
        },
    ]
    _write_session(dirs["sess_dir"], "annotate.json", _make_session(events))

    result = engine.annotate_baselines("annotate.json")
    baselines = result.get("baselines", [])
    assert len(baselines) == 2

    # Verify baselines have correct row counts
    bl_by_idx = {b["event_index"]: b for b in baselines}
    assert bl_by_idx[1]["expected_row_count"] == 5
    assert bl_by_idx[2]["expected_row_count"] == 1

    # Verify file was updated
    reloaded = engine.load_session("annotate.json")
    assert "baselines" in reloaded
    assert len(reloaded["baselines"]) == 2


# ===========================================================================
# 23. test_replay_with_baselines
# ===========================================================================

def test_replay_with_baselines(tmp_path):
    """Replay compares against annotated baselines, not event row_count."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    events = [
        {
            "event_type": "query_run",
            "timestamp": "T",
            "details": {
                "dataset": "test_ds",
                "sql": "SELECT * FROM dataset",
                "row_count": 999,  # Event says 999 (wrong)
            },
        },
    ]
    session_data = _make_session(events)
    # Baseline says 5 (correct — matches actual)
    session_data["baselines"] = [
        {"event_index": 0, "expected_row_count": 5, "tolerance": "exact"},
    ]
    _write_session(dirs["sess_dir"], "bl.json", session_data)

    report = engine.replay("bl.json")
    # Should pass because baseline (5) matches actual (5), even though event says 999
    assert report.passed == 1
    assert report.steps[0].expected_row_count == 5
    assert report.steps[0].actual_row_count == 5


# ===========================================================================
# 24. test_resume_endpoint_dataset_exists
# ===========================================================================

def test_resume_endpoint_dataset_exists(tmp_path):
    """Resume loads successfully when dataset exists on disk."""
    engine, dirs = _make_engine(tmp_path)
    _create_test_dataset(dirs["ds_dir"], "test_ds")

    session_data = _make_session([
        {"event_type": "query_run", "timestamp": "T", "details": {
            "dataset": "test_ds", "sql": "SELECT * FROM dataset", "row_count": 5,
        }},
    ], resume_state={
        "dataset": "test_ds",
        "last_sql": "SELECT * FROM dataset",
    })
    _write_session(dirs["sess_dir"], "resume_ok.json", session_data)

    loaded = engine.load_session("resume_ok.json")
    rs = loaded.get("resume_state", {})
    assert rs["dataset"] == "test_ds"
    assert rs["last_sql"] == "SELECT * FROM dataset"

    # Verify dataset actually exists
    ds_parquet = dirs["ds_dir"] / "test_ds" / "source.parquet"
    assert ds_parquet.exists()


# ===========================================================================
# 25. test_resume_endpoint_dataset_missing
# ===========================================================================

def test_resume_endpoint_dataset_missing(tmp_path):
    """Resume state references a dataset that doesn't exist on disk."""
    engine, dirs = _make_engine(tmp_path)

    session_data = _make_session([
        {"event_type": "query_run", "timestamp": "T", "details": {
            "dataset": "gone_ds", "sql": "SELECT 1", "row_count": 1,
        }},
    ], resume_state={
        "dataset": "gone_ds",
        "last_sql": "SELECT 1",
    })
    _write_session(dirs["sess_dir"], "resume_missing.json", session_data)

    loaded = engine.load_session("resume_missing.json")
    rs = loaded.get("resume_state", {})
    ds_parquet = dirs["ds_dir"] / rs["dataset"] / "source.parquet"
    assert not ds_parquet.exists()


# ===========================================================================
# 26. test_resume_endpoint_loads_library_reference
# ===========================================================================

def test_resume_endpoint_loads_library_reference(tmp_path):
    """Resume with a library_source reference triggers library import."""
    engine, dirs = _make_engine(tmp_path)
    _create_library_csv(dirs["lib_dir"], "ira_list.csv", [
        {"drug_name": "Keytruda", "round": 1},
    ])

    session_data = _make_session([
        {"event_type": "reference_load", "timestamp": "T", "details": {
            "reference_name": "ira_list", "source": "ira_list.csv", "row_count": 1,
        }},
    ], resume_state={
        "reference": {
            "name": "ira_list",
            "library_source": "ira_list.csv",
        },
    })
    _write_session(dirs["sess_dir"], "resume_ref.json", session_data)

    # Simulate what the resume endpoint does: load the library CSV as reference
    loaded = engine.load_session("resume_ref.json")
    rs = loaded.get("resume_state", {})
    ref_info = rs.get("reference")
    assert ref_info is not None
    assert ref_info["library_source"] == "ira_list.csv"

    # Actually load it through the engine's helper
    ref_name = engine._load_reference_from_library(ref_info["library_source"])
    assert ref_name is not None
    ref_pq = dirs["ref_dir"] / ref_name / "source.parquet"
    assert ref_pq.exists()


# ===========================================================================
# 27. test_resume_endpoint_no_resume_state_derives_from_events
# ===========================================================================

def test_resume_endpoint_no_resume_state_derives_from_events(tmp_path):
    """Older sessions without resume_state can derive it from events."""
    engine, dirs = _make_engine(tmp_path)

    # Session with no resume_state key
    session_data = _make_session([
        {"event_type": "session_start", "timestamp": "T", "details": {}},
        {"event_type": "reference_load", "timestamp": "T", "details": {
            "reference_name": "myref", "source": "myref.csv",
        }},
        {"event_type": "query_run", "timestamp": "T", "details": {
            "dataset": "test_ds", "sql": "SELECT 1 FROM dataset", "row_count": 1,
        }},
        {"event_type": "query_run", "timestamp": "T", "details": {
            "dataset": "test_ds", "sql": "SELECT 2 FROM dataset", "row_count": 1,
        }},
    ])
    # Deliberately no resume_state key
    assert "resume_state" not in session_data or session_data.get("resume_state") == {}
    _write_session(dirs["sess_dir"], "old_session.json", session_data)

    loaded = engine.load_session("old_session.json")
    rs = loaded.get("resume_state", {})
    # No resume_state in file — test the derive function from main.py
    if not rs:
        from backend.app.main import _derive_resume_state
        rs = _derive_resume_state(loaded.get("events", []))

    assert rs["dataset"] == "test_ds"
    assert rs["last_sql"] == "SELECT 2 FROM dataset"
    assert rs["reference"]["name"] == "myref"
    assert rs["reference"]["library_source"] == "myref.csv"


# ===========================================================================
# 28. test_list_session_files_includes_resume_state
# ===========================================================================

def test_list_session_files_includes_resume_state(tmp_path):
    """list_session_files returns resume_state from session JSON."""
    engine, dirs = _make_engine(tmp_path)
    session_data = _make_session([], resume_state={
        "dataset": "my_ds",
        "last_sql": "SELECT 1",
    })
    _write_session(dirs["sess_dir"], "rs.json", session_data)

    files = engine.list_session_files()
    assert len(files) == 1
    assert files[0]["resume_state"]["dataset"] == "my_ds"
    assert files[0]["resume_state"]["last_sql"] == "SELECT 1"
