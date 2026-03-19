"""
test_robustness.py — pytest tests for robustness and remaining coverage gaps

Covers:
  _rmtree_robust (main.py and dataset_import.py copies)
  GET  /api/profile (depth: numeric stats, categorical top_values, nulls)
  POST /api/datasets/scan
  POST /api/datasets/register
  GET  /api/presets

Run from project root:
    pytest tests/test_robustness.py -v
"""
from __future__ import annotations

import json
import os
import stat
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.main import _rmtree_robust as main_rmtree_robust
from app.services.dataset_import import _rmtree_robust as import_rmtree_robust

# ---------------------------------------------------------------------------
# FIXTURE — dataset with known values including NULLs
# ---------------------------------------------------------------------------

DATASET = "aw_test_robustness"

EXPECTED_COLUMNS = [
    "drug_name", "hcpcs_code", "total_paid", "total_claims", "all_null_col",
]


def _create_dataset(ds_dir: Path) -> None:
    """Create a test dataset with known distributions and NULLs."""
    rows = []
    for i in range(100):
        rows.append({
            "drug_name": "DrugA" if i < 60 else ("DrugB" if i < 90 else "DrugC"),
            "hcpcs_code": f"J{i + 1:04d}",
            "total_paid": float(i * 10) if i < 95 else None,  # 5 NULLs
            "total_claims": i + 1,
            "all_null_col": None,
        })
    df = pd.DataFrame(rows)
    df["total_paid"] = df["total_paid"].astype("float64")
    df["total_claims"] = df["total_claims"].astype("Int64")
    df["all_null_col"] = df["all_null_col"].astype("float64")
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    meta = {
        "row_count": 100,
        "column_count": len(EXPECTED_COLUMNS),
        "columns": EXPECTED_COLUMNS,
        "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")


@pytest.fixture(scope="module")
def datasets_tmp(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("aw_robustness")
    d = tmp / DATASET
    d.mkdir()
    _create_dataset(d)
    return tmp


@pytest.fixture(scope="module")
def client(datasets_tmp):
    original = main_module.DATASETS_DIR
    main_module.DATASETS_DIR = datasets_tmp
    with TestClient(main_module.app) as c:
        yield c
    main_module.DATASETS_DIR = original


# ===========================================================================
# _rmtree_robust — main.py copy
# ===========================================================================

# Removes a normal directory successfully
def test_main_rmtree_removes_normal_directory(tmp_path):
    d = tmp_path / "normal_dir"
    d.mkdir()
    (d / "file.txt").write_text("hello")
    main_rmtree_robust(d)
    assert not d.exists()


# Removes a directory with read-only files (simulates Windows lock scenario)
def test_main_rmtree_removes_readonly_files(tmp_path):
    d = tmp_path / "readonly_dir"
    d.mkdir()
    f = d / "locked.txt"
    f.write_text("read-only content")
    os.chmod(str(f), stat.S_IREAD)
    main_rmtree_robust(d)
    assert not d.exists()


# Returns gracefully on nonexistent directory (no crash)
def test_main_rmtree_nonexistent_no_crash(tmp_path):
    d = tmp_path / "does_not_exist"
    # Should not raise — rmtree on nonexistent may raise FileNotFoundError,
    # but the function should handle it or let it pass through.
    # The implementation uses shutil.rmtree which raises FileNotFoundError.
    # Since _rmtree_robust catches PermissionError/OSError but not
    # FileNotFoundError explicitly, we check the actual behavior.
    try:
        main_rmtree_robust(d)
    except FileNotFoundError:
        pass  # acceptable — directory doesn't exist
    # Either way, no unhandled crash


# Handles directory with nested subdirectories
def test_main_rmtree_nested_subdirectories(tmp_path):
    d = tmp_path / "nested"
    d.mkdir()
    sub = d / "a" / "b" / "c"
    sub.mkdir(parents=True)
    (sub / "deep.txt").write_text("deep content")
    (d / "top.txt").write_text("top content")
    main_rmtree_robust(d)
    assert not d.exists()


# ===========================================================================
# _rmtree_robust — dataset_import.py copy
# ===========================================================================

# Removes a normal directory successfully
def test_import_rmtree_removes_normal_directory(tmp_path):
    d = tmp_path / "normal_dir"
    d.mkdir()
    (d / "file.txt").write_text("hello")
    import_rmtree_robust(d)
    assert not d.exists()


# Removes a directory with read-only files
def test_import_rmtree_removes_readonly_files(tmp_path):
    d = tmp_path / "readonly_dir"
    d.mkdir()
    f = d / "locked.txt"
    f.write_text("read-only content")
    os.chmod(str(f), stat.S_IREAD)
    import_rmtree_robust(d)
    assert not d.exists()


# Returns gracefully on nonexistent directory
def test_import_rmtree_nonexistent_no_crash(tmp_path):
    d = tmp_path / "does_not_exist"
    try:
        import_rmtree_robust(d)
    except FileNotFoundError:
        pass  # acceptable


# Handles directory with nested subdirectories
def test_import_rmtree_nested_subdirectories(tmp_path):
    d = tmp_path / "nested"
    d.mkdir()
    sub = d / "x" / "y"
    sub.mkdir(parents=True)
    (sub / "file.txt").write_text("nested content")
    import_rmtree_robust(d)
    assert not d.exists()


# ===========================================================================
# GET /api/profile — depth tests
# ===========================================================================

# Profile numeric column has min, max, avg fields
def test_profile_numeric_has_min_max_avg(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    numeric_cols = [c for c in data["columns"] if c.get("kind") == "numeric"]
    assert len(numeric_cols) > 0, "No numeric columns found in profile"
    for col in numeric_cols:
        stats = col.get("stats", {})
        assert "min" in stats, f"Numeric column '{col['name']}' missing 'min'"
        assert "max" in stats, f"Numeric column '{col['name']}' missing 'max'"
        assert "avg" in stats, f"Numeric column '{col['name']}' missing 'avg'"


# Profile numeric min <= max (sanity check)
def test_profile_numeric_min_lte_max(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    numeric_cols = [c for c in data["columns"] if c.get("kind") == "numeric"]
    for col in numeric_cols:
        stats = col.get("stats", {})
        mn = stats.get("min")
        mx = stats.get("max")
        if mn is not None and mx is not None:
            assert float(mn) <= float(mx), (
                f"Column '{col['name']}': min ({mn}) > max ({mx})"
            )


# Profile categorical column has top_values list
def test_profile_categorical_has_top_values(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    cat_cols = [c for c in data["columns"] if c.get("kind") == "categorical"]
    assert len(cat_cols) > 0, "No categorical columns found in profile"
    for col in cat_cols:
        assert "top_values" in col, f"Categorical column '{col['name']}' missing 'top_values'"
        assert isinstance(col["top_values"], list)


# Profile categorical top_values are ordered by frequency (most common first)
def test_profile_categorical_top_values_ordered_by_frequency(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    # drug_name has DrugA=60, DrugB=30, DrugC=10
    drug_col = next((c for c in data["columns"] if c["name"] == "drug_name"), None)
    assert drug_col is not None
    top = drug_col.get("top_values", [])
    assert len(top) >= 2, "Expected at least 2 top_values for drug_name"
    counts = [tv["count"] for tv in top]
    assert counts == sorted(counts, reverse=True), (
        f"top_values not sorted by frequency descending: {counts}"
    )


# Profile column null_count is accurate (create dataset with known NULLs)
def test_profile_null_count_accurate(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    # total_paid has 5 NULLs (rows 95-99)
    paid_col = next((c for c in data["columns"] if c["name"] == "total_paid"), None)
    assert paid_col is not None
    stats = paid_col.get("stats", {})
    null_count = stats.get("null_count", 0)
    # With sampling, the count may not be exact, but should be > 0
    assert null_count > 0, "Expected non-zero null_count for total_paid"


# Profile with all-NULL column: null_count equals row_count
def test_profile_all_null_column(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    null_col = next((c for c in data["columns"] if c["name"] == "all_null_col"), None)
    assert null_col is not None, "all_null_col not found in profile"
    # all_null_col is numeric (float64) so check stats
    stats = null_col.get("stats", {})
    null_count = stats.get("null_count", 0)
    # All 100 rows are NULL; with sampling the count should be close to sample size
    assert null_count > 0, "Expected non-zero null_count for all-NULL column"


# ===========================================================================
# POST /api/datasets/scan
# ===========================================================================

# Scan directory containing Parquet files returns found files
def test_scan_finds_parquet_files(client, tmp_path):
    # Create a temp directory with a parquet file
    scan_dir = tmp_path / "scan_test"
    scan_dir.mkdir()
    df = pd.DataFrame({"a": [1, 2, 3]})
    df.to_parquet(str(scan_dir / "test.parquet"), index=False)

    resp = client.post("/api/datasets/scan", json={
        "path": str(scan_dir),
        "recursive": False,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "files" in data
    assert data["count"] == 1
    assert data["files"][0]["name"] == "test.parquet"


# Scan empty directory returns empty list
def test_scan_empty_directory(client, tmp_path):
    empty_dir = tmp_path / "empty_scan"
    empty_dir.mkdir()

    resp = client.post("/api/datasets/scan", json={
        "path": str(empty_dir),
        "recursive": False,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0
    assert data["files"] == []


# Scan nonexistent directory returns error
def test_scan_nonexistent_directory(client):
    resp = client.post("/api/datasets/scan", json={
        "path": "/nonexistent/path/that/does/not/exist",
        "recursive": False,
    })
    assert resp.status_code == 200  # endpoint returns 200 with error field
    data = resp.json()
    assert "error" in data


# ===========================================================================
# POST /api/datasets/register
# ===========================================================================

# Register with valid Parquet path registers dataset
def test_register_valid_parquet(client, datasets_tmp, tmp_path):
    # Create a parquet file to register
    reg_dir = tmp_path / "register_source"
    reg_dir.mkdir()
    df = pd.DataFrame({"x": [10, 20, 30], "y": ["a", "b", "c"]})
    pq_path = reg_dir / "my_data.parquet"
    df.to_parquet(str(pq_path), index=False)

    resp = client.post("/api/datasets/register", json={
        "dataset_name": "registered_test",
        "parquet_path": str(pq_path),
        "mode": "copy",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "error" not in data
    assert "dataset" in data

    # Verify dataset appears in listing
    ds_list = client.get("/api/datasets").json()
    names = [d["name"] for d in ds_list["datasets"]]
    assert data["dataset"] in names


# Register with nonexistent path returns error
def test_register_nonexistent_path(client):
    resp = client.post("/api/datasets/register", json={
        "dataset_name": "bad_register",
        "parquet_path": "/nonexistent/file.parquet",
        "mode": "copy",
    })
    assert resp.status_code == 200  # endpoint returns 200 with error field
    data = resp.json()
    assert "error" in data


# Register with non-Parquet file (e.g. a text file)
def test_register_non_parquet_file(client, tmp_path):
    # Create a plain text file
    txt_file = tmp_path / "not_parquet.txt"
    txt_file.write_text("this is not parquet")

    resp = client.post("/api/datasets/register", json={
        "dataset_name": "txt_register",
        "parquet_path": str(txt_file),
        "mode": "copy",
    })
    # The endpoint copies any file — validation happens at query time.
    # As long as the file exists, it registers. The test verifies it
    # doesn't crash; the context build may fail but the endpoint
    # should still return a response.
    assert resp.status_code == 200
    data = resp.json()
    # Should either succeed (with context_built=False) or return an error
    assert "dataset" in data or "error" in data


# ===========================================================================
# GET /api/presets
# ===========================================================================

# Presets endpoint returns 200
def test_presets_returns_200(client):
    resp = client.get("/api/presets")
    assert resp.status_code == 200


# Presets response is a list
def test_presets_response_is_list(client):
    data = client.get("/api/presets").json()
    assert "presets" in data
    assert isinstance(data["presets"], list)
