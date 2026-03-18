"""
test_core.py — pytest tests for core inspection and management endpoints

Covers:
  GET  /api/version
  GET  /api/health
  GET  /api/datasets
  GET  /api/datasets/{name}/meta
  GET  /api/schema
  GET  /api/preview
  POST /api/datasets/{name}/delete

Run from project root:
    pytest tests/test_core.py -v
"""
from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module

# ---------------------------------------------------------------------------
# FIXTURE
# ---------------------------------------------------------------------------

DATASET = "aw_test_core"
FAKE_GRAIN = (
    "Each row represents one drug reimbursement record for a provider and HCPCS "
    "code in a given service year. The dataset contains one hundred synthetic rows "
    "used to test core inspection and metadata endpoints."
)

EXPECTED_COLUMNS = ["drug_name", "hcpcs_code", "total_paid", "total_claims", "service_year"]


def _create_dataset(ds_dir: Path) -> None:
    rows = [
        {
            "drug_name":    "DrugA" if i < 50 else "DrugB",
            "hcpcs_code":   f"J{i + 1:04d}",
            "total_paid":   100.0 if i < 50 else 200.0,
            "total_claims": i + 1,
            "service_year": 2023,
        }
        for i in range(100)
    ]
    df = pd.DataFrame(rows)
    df["total_paid"]   = df["total_paid"].astype("float64")
    df["total_claims"] = df["total_claims"].astype("int64")
    df["service_year"] = df["service_year"].astype("int64")
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    meta = {
        "row_count": 100, "column_count": len(EXPECTED_COLUMNS),
        "columns": EXPECTED_COLUMNS, "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    ctx = {"grain_description": FAKE_GRAIN,
           "grain_description_generated_at": datetime.now().isoformat()}
    (ds_dir / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")


@pytest.fixture(scope="module")
def datasets_tmp(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("aw_core")
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
# /api/version
# ===========================================================================

# Prevents version endpoint from disappearing or losing required fields
def test_version_returns_200(client):
    assert client.get("/api/version").status_code == 200


# Prevents version number from being blank or missing
def test_version_has_name_and_version(client):
    data = client.get("/api/version").json()
    assert data.get("name")
    assert data.get("version")


# Prevents duckdb_version from being stripped out of the version response
def test_version_includes_duckdb_version(client):
    data = client.get("/api/version").json()
    assert "duckdb_version" in data
    assert data["duckdb_version"]


# ===========================================================================
# /api/health
# ===========================================================================

# Prevents health endpoint from returning non-200 in a normal environment
def test_health_returns_200(client):
    assert client.get("/api/health").status_code == 200


# Prevents health checks block from being removed from the response
def test_health_has_status_and_checks(client):
    data = client.get("/api/health").json()
    assert "status" in data
    assert "checks" in data


# Prevents DuckDB check from being silently removed from health output
def test_health_duckdb_check_present(client):
    checks = client.get("/api/health").json()["checks"]
    assert "duckdb" in checks
    assert checks["duckdb"]["ok"] is True


# ===========================================================================
# /api/datasets
# ===========================================================================

# Prevents the datasets list from omitting a newly imported dataset
def test_datasets_includes_fixture(client):
    data = client.get("/api/datasets").json()
    names = [d["name"] for d in data["datasets"]]
    assert DATASET in names, f"Expected {DATASET} in dataset list; got: {names}"


# Prevents dataset entries from being returned without their required fields
def test_datasets_entries_have_required_fields(client):
    data = client.get("/api/datasets").json()
    for entry in data["datasets"]:
        for field in ("name", "row_count", "column_count"):
            assert field in entry, f"Dataset entry missing field '{field}': {entry}"


# ===========================================================================
# /api/datasets/{name}/meta
# ===========================================================================

# Prevents meta endpoint from returning wrong or zero row count
def test_dataset_meta_row_count(client):
    data = client.get(f"/api/datasets/{DATASET}/meta").json()
    assert data["row_count"] == 100


# Prevents column_count from being wrong in the metadata response
def test_dataset_meta_column_count(client):
    data = client.get(f"/api/datasets/{DATASET}/meta").json()
    assert data["column_count"] == len(EXPECTED_COLUMNS)


# Prevents file_size_bytes from being absent or zero
def test_dataset_meta_file_size_positive(client):
    data = client.get(f"/api/datasets/{DATASET}/meta").json()
    assert data.get("file_size_bytes", 0) > 0


# Prevents meta from returning 200 for a nonexistent dataset
def test_dataset_meta_404_for_nonexistent(client):
    assert client.get("/api/datasets/does_not_exist_xyz/meta").status_code == 404


# ===========================================================================
# /api/schema
# ===========================================================================

# Prevents schema from returning wrong column names
def test_schema_returns_correct_column_names(client):
    data = client.get(f"/api/schema?dataset={DATASET}").json()
    names = [c["name"] for c in data["columns"]]
    assert names == EXPECTED_COLUMNS, f"Schema columns: {names}"


# Prevents schema from losing the type field for any column
def test_schema_columns_have_type(client):
    data = client.get(f"/api/schema?dataset={DATASET}").json()
    for col in data["columns"]:
        assert col.get("type"), f"Column '{col['name']}' has no type"


# Prevents schema from returning 200 for a nonexistent dataset
def test_schema_404_for_nonexistent(client):
    assert client.get("/api/schema?dataset=does_not_exist_xyz").status_code == 404


# ===========================================================================
# /api/preview
# ===========================================================================

# Prevents preview from returning an empty result for a populated dataset
def test_preview_returns_rows(client):
    data = client.get(f"/api/preview?dataset={DATASET}").json()
    assert len(data["rows"]) > 0


# Prevents preview from returning rows without the expected columns
def test_preview_rows_have_correct_columns(client):
    data = client.get(f"/api/preview?dataset={DATASET}").json()
    row = data["rows"][0]
    for col in EXPECTED_COLUMNS:
        assert col in row, f"Preview row missing column '{col}'"


# Prevents preview from ignoring the limit parameter
def test_preview_limit_parameter_respected(client):
    data = client.get(f"/api/preview?dataset={DATASET}&limit=5").json()
    assert len(data["rows"]) == 5


# Prevents preview from returning 200 for a nonexistent dataset
def test_preview_404_for_nonexistent(client):
    assert client.get("/api/preview?dataset=does_not_exist_xyz").status_code == 404


# ===========================================================================
# /api/datasets/{name}/delete
# ===========================================================================

# Prevents the delete endpoint from failing or leaving the dataset behind
def test_delete_removes_dataset(client, datasets_tmp):
    # Create a throwaway dataset to delete
    name = "aw_test_delete_target"
    d = datasets_tmp / name
    d.mkdir()
    _create_dataset(d)

    resp = client.post(f"/api/datasets/{name}/delete")
    assert resp.status_code == 200
    assert resp.json().get("ok") is True
    assert not d.exists(), "Dataset directory still exists after delete"


# Prevents delete from returning 200 for a dataset that doesn't exist
def test_delete_404_for_nonexistent(client):
    assert client.post("/api/datasets/does_not_exist_xyz/delete").status_code == 404


# Bug 3 regression: if rmtree fails to fully remove the directory (e.g. due to
# a Windows file lock), the endpoint must return an error — not false success.
def test_delete_returns_500_when_rmtree_fails(client, datasets_tmp):
    from unittest.mock import patch

    name = "aw_test_delete_locked"
    d = datasets_tmp / name
    d.mkdir()
    _create_dataset(d)

    # Simulate a persistent file lock: _rmtree_robust raises PermissionError
    # after exhausting all retries.
    with patch(
        "app.main._rmtree_robust",
        side_effect=PermissionError("[WinError 32] The process cannot access the file"),
    ):
        resp = client.post(f"/api/datasets/{name}/delete")
    assert resp.status_code == 500
    assert "delete" in resp.json()["detail"].lower() or "failed" in resp.json()["detail"].lower()
    # Directory should still exist since delete failed
    assert d.exists()


# Bug 3 regression: after a successful delete, the directory must actually be gone.
# This guards against rmtree silently skipping locked files via the onerror callback.
def test_delete_actually_removes_all_files(client, datasets_tmp):
    name = "aw_test_delete_verify"
    d = datasets_tmp / name
    d.mkdir()
    _create_dataset(d)

    resp = client.post(f"/api/datasets/{name}/delete")
    assert resp.status_code == 200
    # The critical check: directory must not exist
    assert not d.exists(), (
        "Bug 3: delete returned ok=True but directory still exists. "
        "rmtree may have silently skipped locked files."
    )
