"""
test_reference_library.py — tests for Reference Table Library endpoints

Covers:
  GET  /api/reference_library — list available library files
  POST /api/reference_library/{filename}/load — load library file as reference
  404 for nonexistent library files
  Library manifest parsing

Run from project root:
    pytest tests/test_reference_library.py -v
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module


@pytest.fixture()
def lib_tmp(tmp_path):
    """Provide a temporary REFERENCE_LIBRARY_DIR with a test CSV and manifest."""
    original_lib = main_module.REFERENCE_LIBRARY_DIR
    original_ref = main_module.REFERENCES_DIR

    lib_dir = tmp_path / "library"
    lib_dir.mkdir()
    ref_dir = tmp_path / "references"
    ref_dir.mkdir()

    # Create a test CSV
    df = pd.DataFrame([
        {"drug": "TestDrugA", "round": 1},
        {"drug": "TestDrugB", "round": 2},
        {"drug": "TestDrugC", "round": 3},
    ])
    df.to_csv(str(lib_dir / "test_drugs.csv"), index=False)

    # Create manifest
    manifest = [
        {
            "filename": "test_drugs.csv",
            "name": "Test Drug List",
            "description": "Test reference for unit tests.",
            "columns": ["drug", "round"],
            "row_count": 3,
            "version": "2026-03",
            "join_hint": "JOIN reference ON dataset.drug LIKE reference.drug || '%'",
        }
    ]
    (lib_dir / "_library.json").write_text(json.dumps(manifest), encoding="utf-8")

    main_module.REFERENCE_LIBRARY_DIR = lib_dir
    main_module.REFERENCES_DIR = ref_dir

    yield lib_dir

    main_module.REFERENCE_LIBRARY_DIR = original_lib
    main_module.REFERENCES_DIR = original_ref


@pytest.fixture()
def client(lib_tmp):
    with TestClient(main_module.app) as c:
        yield c


# ===========================================================================
# GET /api/reference_library
# ===========================================================================

def test_list_library(client):
    resp = client.get("/api/reference_library")
    assert resp.status_code == 200
    items = resp.json()["library"]
    assert len(items) == 1
    assert items[0]["name"] == "Test Drug List"
    assert items[0]["row_count"] == 3


def test_list_library_returns_all_fields(client):
    resp = client.get("/api/reference_library")
    item = resp.json()["library"][0]
    assert "filename" in item
    assert "description" in item
    assert "columns" in item
    assert "version" in item
    assert "join_hint" in item


def test_list_library_empty_when_no_manifest(tmp_path):
    """Should return empty list if no _library.json exists."""
    original = main_module.REFERENCE_LIBRARY_DIR
    main_module.REFERENCE_LIBRARY_DIR = tmp_path / "empty_lib"
    main_module.REFERENCE_LIBRARY_DIR.mkdir()
    try:
        with TestClient(main_module.app) as c:
            resp = c.get("/api/reference_library")
            assert resp.json()["library"] == []
    finally:
        main_module.REFERENCE_LIBRARY_DIR = original


# ===========================================================================
# POST /api/reference_library/{filename}/load
# ===========================================================================

def test_load_library_file(client):
    resp = client.post("/api/reference_library/test_drugs.csv/load")
    assert resp.status_code == 200
    data = resp.json()
    assert data["reference"] is not None
    assert data["row_count"] == 3
    assert len(data["columns"]) == 2


def test_load_library_file_creates_reference(client):
    """Loading a library file should make it appear in /api/references."""
    client.post("/api/reference_library/test_drugs.csv/load")
    resp = client.get("/api/references")
    refs = resp.json()["references"]
    assert len(refs) >= 1


def test_load_nonexistent_library_file(client):
    resp = client.post("/api/reference_library/nonexistent.csv/load")
    assert resp.status_code == 404


def test_load_library_file_twice_overwrites(client):
    """Loading the same file twice should overwrite cleanly."""
    resp1 = client.post("/api/reference_library/test_drugs.csv/load")
    resp2 = client.post("/api/reference_library/test_drugs.csv/load")
    assert resp1.status_code == 200
    assert resp2.status_code == 200
    assert resp2.json()["row_count"] == 3
