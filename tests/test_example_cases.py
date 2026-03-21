"""
test_example_cases.py — tests for example cases and saved sessions endpoints

Covers:
  GET /api/example_cases — list available example cases
  POST /api/example_cases/{case_id}/load — load an example case
  GET /api/sessions/saved — list named saved sessions

Run from project root:
    pytest tests/test_example_cases.py -v
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from starlette.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_example_case(
    cases_dir: Path,
    case_id: str,
    metadata: dict,
    data_rows: list[dict] | None = None,
    data_filename: str = "sample.csv",
    ref_rows: list[dict] | None = None,
    ref_filename: str = "ref.csv",
) -> Path:
    """Create an example case directory structure for testing."""
    case_dir = cases_dir / case_id
    case_dir.mkdir(parents=True, exist_ok=True)

    # Write metadata.json
    (case_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8"
    )

    # Write data file
    if data_rows is not None:
        data_dir = case_dir / "data"
        data_dir.mkdir(exist_ok=True)
        df = pd.DataFrame(data_rows)
        df.to_csv(str(data_dir / data_filename), index=False)

    # Write reference file
    if ref_rows is not None:
        ref_dir = case_dir / "reference"
        ref_dir.mkdir(exist_ok=True)
        df = pd.DataFrame(ref_rows)
        df.to_csv(str(ref_dir / ref_filename), index=False)

    return case_dir


def _write_session_file(sessions_dir: Path, filename: str, data: dict) -> Path:
    """Write a session JSON file."""
    filepath = sessions_dir / filename
    filepath.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return filepath


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def temp_dirs(tmp_path):
    """Create temp directories and return them as a dict."""
    dirs = {
        "example_cases": tmp_path / "example_cases",
        "datasets": tmp_path / "datasets",
        "references": tmp_path / "references",
        "sessions": tmp_path / "sessions",
        "exports": tmp_path / "exports",
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


@pytest.fixture()
def patched_app(temp_dirs):
    """Import the app and patch directory constants for isolated testing."""
    import app.main as main_mod

    patches = {
        "EXAMPLE_CASES_DIR": temp_dirs["example_cases"],
        "DATASETS_DIR": temp_dirs["datasets"],
        "REFERENCES_DIR": temp_dirs["references"],
        "SESSIONS_DIR": temp_dirs["sessions"],
        "EXPORTS_DIR": temp_dirs["exports"],
    }

    with patch.multiple(main_mod, **patches):
        client = TestClient(main_mod.app, raise_server_exceptions=False)
        yield client, temp_dirs


# ===========================================================================
# EXAMPLE CASES — LIST
# ===========================================================================


def test_list_example_cases_empty(patched_app):
    """No example_cases dir content returns empty list."""
    client, dirs = patched_app
    resp = client.get("/api/example_cases")
    assert resp.status_code == 200
    body = resp.json()
    assert body["cases"] == []


def test_list_example_cases_no_dir(patched_app):
    """If EXAMPLE_CASES_DIR does not exist, return empty list."""
    client, dirs = patched_app
    import shutil
    shutil.rmtree(dirs["example_cases"])
    resp = client.get("/api/example_cases")
    assert resp.status_code == 200
    assert resp.json()["cases"] == []


def test_list_example_cases_with_metadata(patched_app):
    """Create a temp case dir with metadata.json, verify it is returned."""
    client, dirs = patched_app

    meta = {
        "id": "globe_analysis",
        "title": "GLOBE Analysis",
        "description": "Identify Part B GLOBE candidates",
        "dataset_display_name": "Part B Spending",
    }
    _create_example_case(
        dirs["example_cases"],
        "globe_analysis",
        metadata=meta,
        data_rows=[{"drug": "Keytruda", "spend": 8000}],
        ref_rows=[{"drug": "Humira", "status": "excluded"}],
    )

    resp = client.get("/api/example_cases")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["cases"]) == 1

    case = body["cases"][0]
    assert case["id"] == "globe_analysis"
    assert case["title"] == "GLOBE Analysis"
    assert case["has_data"] is True
    assert case["has_reference"] is True


def test_list_example_cases_no_data_no_reference(patched_app):
    """Case with metadata but no data/ or reference/ dirs."""
    client, dirs = patched_app

    meta = {"id": "empty_case", "title": "Empty Case"}
    _create_example_case(
        dirs["example_cases"],
        "empty_case",
        metadata=meta,
        data_rows=None,
        ref_rows=None,
    )

    resp = client.get("/api/example_cases")
    assert resp.status_code == 200
    case = resp.json()["cases"][0]
    assert case["has_data"] is False
    assert case["has_reference"] is False


def test_list_example_cases_skips_non_dirs(patched_app):
    """Files in example_cases dir are ignored — only subdirectories matter."""
    client, dirs = patched_app

    # Create a stray file
    (dirs["example_cases"] / "readme.txt").write_text("ignore me", encoding="utf-8")

    resp = client.get("/api/example_cases")
    assert resp.status_code == 200
    assert resp.json()["cases"] == []


# ===========================================================================
# EXAMPLE CASES — LOAD
# ===========================================================================


def test_load_example_case_not_found(patched_app):
    """404 for nonexistent case."""
    client, dirs = patched_app
    resp = client.post(
        "/api/example_cases/nonexistent/load",
        json={"mode": "resume"},
    )
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


def test_load_example_case_no_metadata(patched_app):
    """404 when case directory exists but has no metadata.json."""
    client, dirs = patched_app
    case_dir = dirs["example_cases"] / "no_meta"
    case_dir.mkdir(parents=True, exist_ok=True)

    resp = client.post(
        "/api/example_cases/no_meta/load",
        json={"mode": "resume"},
    )
    assert resp.status_code == 404
    assert "metadata" in resp.json()["detail"].lower()


def test_load_example_case_imports_dataset(patched_app):
    """Load a case with a CSV data file, verify dataset appears."""
    client, dirs = patched_app

    meta = {
        "id": "test_case",
        "title": "Test Case",
        "dataset_display_name": "Test Dataset",
    }
    _create_example_case(
        dirs["example_cases"],
        "test_case",
        metadata=meta,
        data_rows=[
            {"drug": "Keytruda", "spend": 8000},
            {"drug": "Opdivo", "spend": 6000},
        ],
    )

    resp = client.post(
        "/api/example_cases/test_case/load",
        json={"mode": "resume"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "loaded"
    assert body["case_id"] == "test_case"
    assert body["dataset"] is not None
    assert body["metadata"]["title"] == "Test Case"

    # Verify dataset was actually imported — parquet exists
    ds_name = body["dataset"]
    ds_parquet = dirs["datasets"] / ds_name / "source.parquet"
    assert ds_parquet.exists()


def test_load_example_case_loads_reference(patched_app):
    """Load a case with a reference CSV, verify reference is loaded."""
    client, dirs = patched_app

    meta = {
        "id": "ref_case",
        "title": "Reference Case",
        "dataset_display_name": "Ref Dataset",
    }
    _create_example_case(
        dirs["example_cases"],
        "ref_case",
        metadata=meta,
        data_rows=[{"drug": "Eliquis", "spend": 5000}],
        ref_rows=[
            {"drug": "Humira", "status": "excluded"},
            {"drug": "Enbrel", "status": "excluded"},
        ],
    )

    resp = client.post(
        "/api/example_cases/ref_case/load",
        json={"mode": "tutorial"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "loaded"
    assert len(body["references"]) == 1
    ref = body["references"][0]
    assert ref["row_count"] == 2
    assert ref["columns"] == 2

    # Verify reference parquet exists
    ref_parquet = dirs["references"] / ref["name"] / "source.parquet"
    assert ref_parquet.exists()


def test_load_example_case_default_mode(patched_app):
    """Load without specifying mode defaults to 'resume'."""
    client, dirs = patched_app

    meta = {"id": "default_mode", "title": "Default"}
    _create_example_case(
        dirs["example_cases"],
        "default_mode",
        metadata=meta,
        data_rows=[{"x": 1}],
    )

    resp = client.post(
        "/api/example_cases/default_mode/load",
        json={},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "loaded"


# ===========================================================================
# SAVED SESSIONS
# ===========================================================================


def test_sessions_saved_empty(patched_app):
    """No session files returns empty list."""
    client, dirs = patched_app
    resp = client.get("/api/sessions/saved")
    assert resp.status_code == 200
    assert resp.json()["sessions"] == []


def test_sessions_saved_returns_named_sessions_only(patched_app):
    """Only named sessions appear in the saved list.

    Unnamed auto-saves (session_{uuid}_{date}.json) are crash-recovery files
    and must not clutter the Retrieve Session list.
    """
    client, dirs = patched_app

    # Named session — should appear
    _write_session_file(dirs["sessions"], "session_named.json", {
        "session_id": "abc-123",
        "name": "GLOBE Analysis Session",
        "description": "Analyzed Part B GLOBE candidates",
        "started_at": "2026-03-19T10:00:00+00:00",
        "events": [{"event_type": "session_start", "timestamp": "2026-03-19T10:00:00+00:00"}],
        "ai_mode": "cloud",
    })

    # Unnamed auto-save — must NOT appear
    _write_session_file(dirs["sessions"], "session_unnamed.json", {
        "session_id": "def-456",
        "name": "",
        "started_at": "2026-03-19T09:00:00+00:00",
        "events": [],
        "ai_mode": "cloud",
    })

    resp = client.get("/api/sessions/saved")
    assert resp.status_code == 200
    body = resp.json()
    # Only the named session should appear — unnamed auto-save is excluded
    assert len(body["sessions"]) == 1

    named = body["sessions"][0]
    assert named["session_id"] == "abc-123"
    assert named["name"] == "GLOBE Analysis Session"
    assert named["description"] == "Analyzed Part B GLOBE candidates"
    assert named["event_count"] == 1
    assert named["filename"] == "session_named.json"


def test_sessions_saved_no_dir(patched_app):
    """If SESSIONS_DIR does not exist, return empty list."""
    client, dirs = patched_app
    import shutil
    shutil.rmtree(dirs["sessions"])
    resp = client.get("/api/sessions/saved")
    assert resp.status_code == 200
    assert resp.json()["sessions"] == []


def test_sessions_saved_sorted_reverse(patched_app):
    """Sessions are returned sorted by filename descending (newest first)."""
    client, dirs = patched_app

    _write_session_file(dirs["sessions"], "session_aaa.json", {
        "session_id": "aaa",
        "name": "First",
        "started_at": "2026-03-17T10:00:00+00:00",
        "events": [],
        "ai_mode": "cloud",
    })
    _write_session_file(dirs["sessions"], "session_zzz.json", {
        "session_id": "zzz",
        "name": "Second",
        "started_at": "2026-03-19T10:00:00+00:00",
        "events": [],
        "ai_mode": "cloud",
    })

    resp = client.get("/api/sessions/saved")
    body = resp.json()
    assert len(body["sessions"]) == 2
    # Reverse sorted by filename: zzz before aaa
    assert body["sessions"][0]["session_id"] == "zzz"
    assert body["sessions"][1]["session_id"] == "aaa"
