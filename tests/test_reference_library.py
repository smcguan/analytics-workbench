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

    # Create a test CSV — use title-case-safe values since reference
    # import normalizes string columns to title case (Bug #8 fix)
    df = pd.DataFrame([
        {"drug": "Eliquis", "round": 1},
        {"drug": "Keytruda", "round": 2},
        {"drug": "Opdivo", "round": 3},
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


def test_auto_discover_csv_without_manifest(tmp_path):
    """CSVs in library dir should appear even without a _library.json entry."""
    original = main_module.REFERENCE_LIBRARY_DIR
    lib_dir = tmp_path / "auto_lib"
    lib_dir.mkdir()

    # Create a CSV with no manifest entry
    csv_path = lib_dir / "orphan_drugs.csv"
    csv_path.write_text("drug_name,designation_date\nKeytruda,2017-05-23\nOpdivo,2014-12-22\n")

    main_module.REFERENCE_LIBRARY_DIR = lib_dir
    try:
        with TestClient(main_module.app) as c:
            resp = c.get("/api/reference_library")
            items = resp.json()["library"]
            assert len(items) == 1
            assert items[0]["filename"] == "orphan_drugs.csv"
            assert items[0]["columns"] == ["drug_name", "designation_date"]
            assert items[0]["row_count"] == 2
            assert items[0]["name"] == "Orphan Drugs"
    finally:
        main_module.REFERENCE_LIBRARY_DIR = original


def test_auto_discover_merges_with_manifest(lib_tmp):
    """Auto-discovered CSVs should appear alongside manifest entries."""
    # lib_tmp already has test_drugs.csv in manifest
    # Add a new CSV without a manifest entry
    (lib_tmp / "extra_table.csv").write_text("code,label\nA,Alpha\nB,Beta\n")

    with TestClient(main_module.app) as c:
        resp = c.get("/api/reference_library")
        items = resp.json()["library"]
        filenames = [i["filename"] for i in items]
        assert "test_drugs.csv" in filenames, "Manifest entry should still be present"
        assert "extra_table.csv" in filenames, "Auto-discovered CSV should appear"
        # Manifest entry should keep its curated metadata
        manifest_item = next(i for i in items if i["filename"] == "test_drugs.csv")
        assert manifest_item["name"] == "Test Drug List"


def test_auto_discover_does_not_duplicate_manifest_entries(lib_tmp):
    """CSVs already in the manifest should not appear twice."""
    with TestClient(main_module.app) as c:
        resp = c.get("/api/reference_library")
        filenames = [i["filename"] for i in resp.json()["library"]]
        assert filenames.count("test_drugs.csv") == 1


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


# ===========================================================================
# Bug #7 regression — Library reference tables register in DuckDB
# ===========================================================================

def test_loaded_library_ref_queryable_via_sql(lib_tmp, tmp_path):
    """
    Bug #7 regression: loading a library CSV should make it queryable via SQL.

    Import a dataset, load a library reference, then run a JOIN query.
    The reference table must be resolvable by its registered name.
    """
    import app.main as main_module
    from fastapi.testclient import TestClient

    # Set up a temporary dataset
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    test_ds = ds_dir / "test_data"
    test_ds.mkdir()
    df = pd.DataFrame({"drug": ["Eliquis", "Keytruda", "Unknown"], "spend": [100, 200, 50]})
    df.to_parquet(str(test_ds / "source.parquet"), index=False)
    (test_ds / "_meta.json").write_text(
        json.dumps({"row_count": 3, "column_count": 2}), encoding="utf-8"
    )
    original_ds = main_module.DATASETS_DIR
    main_module.DATASETS_DIR = ds_dir

    try:
        with TestClient(main_module.app) as c:
            # Load the library reference
            resp = c.post("/api/reference_library/test_drugs.csv/load")
            assert resp.status_code == 200
            ref_name = resp.json()["reference"]

            # Run a SQL query using the reference
            sql_resp = c.post("/api/sql", json={
                "dataset": "test_data",
                "sql": f"SELECT d.drug, r.round FROM dataset d JOIN {ref_name} r ON d.drug = r.drug",
                "reference": ref_name,
            })
            assert sql_resp.status_code == 200
            rows = sql_resp.json()["rows"]
            assert len(rows) >= 1, "JOIN should return matching rows"
    finally:
        main_module.DATASETS_DIR = original_ds


# ===========================================================================
# Bug #8 — Reference Library case normalization on import
# ===========================================================================

def test_library_ref_stores_values_as_is(lib_tmp, tmp_path):
    """
    Bug #10 fix: String columns in library reference tables are stored
    exactly as they appear in the CSV — no title-casing applied.
    JOINs use LOWER() at query time instead.
    """
    import pyarrow.parquet as pq_test
    import app.main as main_module
    from fastapi.testclient import TestClient

    # Create a library CSV with mixed-case string values
    mixed_case_csv = lib_tmp / "mixed_case.csv"
    df = pd.DataFrame([
        {"generic_name": "apixaban", "category": "CARDIOVASCULAR"},
        {"generic_name": "SEMAGLUTIDE", "category": "diabetes"},
        {"generic_name": "Pembrolizumab", "category": "Oncology"},
    ])
    df.to_csv(str(mixed_case_csv), index=False)

    # Add to manifest
    manifest = json.loads((lib_tmp / "_library.json").read_text())
    manifest.append({
        "filename": "mixed_case.csv",
        "name": "Mixed Case Test",
        "description": "Test case preservation",
        "columns": ["generic_name", "category"],
        "row_count": 3,
        "version": "2026-03",
        "join_hint": "",
    })
    (lib_tmp / "_library.json").write_text(json.dumps(manifest))

    with TestClient(main_module.app) as c:
        resp = c.post("/api/reference_library/mixed_case.csv/load")
        assert resp.status_code == 200

        # Read the imported parquet and verify values are stored as-is
        ref_dir = main_module.REFERENCES_DIR
        ref_parquet = list(ref_dir.rglob("source.parquet"))
        assert len(ref_parquet) >= 1
        result_df = pd.read_parquet(ref_parquet[-1])
        names = result_df["generic_name"].tolist()
        # Values must be stored exactly as in CSV — no title-casing
        assert "apixaban" in names, f"Expected original 'apixaban', got {names}"
        assert "SEMAGLUTIDE" in names, f"Expected original 'SEMAGLUTIDE', got {names}"
        assert "Pembrolizumab" in names, f"Expected original 'Pembrolizumab', got {names}"
        categories = result_df["category"].tolist()
        assert "CARDIOVASCULAR" in categories
        assert "diabetes" in categories
        assert "Oncology" in categories


def test_case_insensitive_join_after_library_load(lib_tmp, tmp_path):
    """
    Bug #10 fix end-to-end: A JOIN between a primary dataset and a library
    reference returns rows when LOWER() is used on both sides. Reference
    values are stored as-is (no title-casing), so LOWER() is required.
    """
    import app.main as main_module
    from fastapi.testclient import TestClient

    # Create a library CSV with lowercase generic names (like IRA CSV)
    ira_csv = lib_tmp / "ira_test.csv"
    df_ira = pd.DataFrame([
        {"drug_name": "eliquis", "generic_name": "apixaban", "ira_round": 1},
        {"drug_name": "keytruda", "generic_name": "pembrolizumab", "ira_round": 2},
    ])
    df_ira.to_csv(str(ira_csv), index=False)
    manifest = json.loads((lib_tmp / "_library.json").read_text())
    manifest.append({
        "filename": "ira_test.csv",
        "name": "IRA Test",
        "description": "Test IRA join",
        "columns": ["drug_name", "generic_name", "ira_round"],
        "row_count": 2,
        "version": "2026-03",
        "join_hint": "",
    })
    (lib_tmp / "_library.json").write_text(json.dumps(manifest))

    # Create a primary dataset with Title Case generic names (like CMS data)
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    test_ds = ds_dir / "cms_drugs"
    test_ds.mkdir()
    df_cms = pd.DataFrame({
        "Gnrc_Name": ["Apixaban", "Pembrolizumab", "Metformin Hcl"],
        "Tot_Spndng": [5000000, 8000000, 200000],
    })
    df_cms.to_parquet(str(test_ds / "source.parquet"), index=False)
    (test_ds / "_meta.json").write_text(
        json.dumps({"row_count": 3, "column_count": 2}), encoding="utf-8"
    )
    original_ds = main_module.DATASETS_DIR
    main_module.DATASETS_DIR = ds_dir

    try:
        with TestClient(main_module.app) as c:
            resp = c.post("/api/reference_library/ira_test.csv/load")
            assert resp.status_code == 200
            ref_name = resp.json()["reference"]

            # JOIN with LOWER() on both sides — required since values are stored as-is
            sql_resp = c.post("/api/sql", json={
                "dataset": "cms_drugs",
                "sql": (
                    f"SELECT p.Gnrc_Name, i.drug_name, i.ira_round "
                    f"FROM dataset p "
                    f"INNER JOIN {ref_name} i ON LOWER(p.Gnrc_Name) = LOWER(i.generic_name)"
                ),
                "reference": ref_name,
            })
            assert sql_resp.status_code == 200
            rows = sql_resp.json()["rows"]
            assert len(rows) == 2, (
                f"Expected 2 matching rows (Apixaban, Pembrolizumab), got {len(rows)}: {rows}"
            )
    finally:
        main_module.DATASETS_DIR = original_ds


# ===========================================================================
# Bug #10 regression — DuckDB EXPLAIN validation with reference tables
# ===========================================================================

def test_duckdb_explain_validates_reference_table(lib_tmp, tmp_path):
    """
    Bug #10 regression: validate_sql_with_duckdb must create a 'reference'
    view when a reference table is loaded, so that AI-generated SQL with
    JOIN reference passes EXPLAIN validation.

    Previously, only a 'dataset' view was created. Any AI SQL containing
    JOIN reference failed with 'Catalog Error: Table reference does not exist',
    causing the generate_sql endpoint to return status=error.
    """
    from app.ai.sql_validator import validate_sql_with_duckdb
    import app.main as main_module
    from fastapi.testclient import TestClient

    # Set up dataset
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    test_ds = ds_dir / "test_data"
    test_ds.mkdir()
    df = pd.DataFrame({"drug": ["Eliquis", "Keytruda", "Unknown"], "spend": [100, 200, 50]})
    df.to_parquet(str(test_ds / "source.parquet"), index=False)
    (test_ds / "_meta.json").write_text(
        json.dumps({"row_count": 3, "column_count": 2}), encoding="utf-8"
    )
    original_ds = main_module.DATASETS_DIR
    main_module.DATASETS_DIR = ds_dir

    try:
        with TestClient(main_module.app) as c:
            # Load a library reference
            resp = c.post("/api/reference_library/test_drugs.csv/load")
            assert resp.status_code == 200
            ref_name = resp.json()["reference"]

            # Resolve the reference parquet path
            ref_pq = str(
                (main_module.REFERENCES_DIR / ref_name / "source.parquet").resolve()
            )
            ds_pq = str((test_ds / "source.parquet").resolve())

            def mock_source_path(name):
                return ds_pq, False

            # SQL with JOIN reference — should pass EXPLAIN when ref path provided
            sql = (
                "SELECT d.drug, r.round "
                "FROM dataset d "
                "JOIN reference r ON d.drug = r.drug"
            )

            # Without reference path — should FAIL (the original bug)
            ok_no_ref, msg_no_ref = validate_sql_with_duckdb(
                sql=sql,
                dataset_name="test_data",
                dataset_source_path_fn=mock_source_path,
                reference_parquet_path=None,
            )
            assert not ok_no_ref, (
                "EXPLAIN should fail without reference view"
            )
            assert "reference" in msg_no_ref.lower(), (
                f"Error should mention 'reference' table: {msg_no_ref}"
            )

            # With reference path — should PASS (the fix)
            ok_with_ref, msg_with_ref = validate_sql_with_duckdb(
                sql=sql,
                dataset_name="test_data",
                dataset_source_path_fn=mock_source_path,
                reference_parquet_path=ref_pq,
            )
            assert ok_with_ref, (
                f"EXPLAIN should pass with reference view: {msg_with_ref}"
            )
    finally:
        main_module.DATASETS_DIR = original_ds
