"""
test_queries.py — pytest tests for saved queries and dataset profile

Covers:
  GET  /api/queries
  POST /api/queries/save
  POST /api/queries/delete
  GET  /api/profile

Run from project root:
    pytest tests/test_queries.py -v
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module

# ---------------------------------------------------------------------------
# FIXTURE
# ---------------------------------------------------------------------------

DATASET = "aw_test_queries"

EXPECTED_COLUMNS = ["drug_name", "hcpcs_code", "total_paid", "total_claims"]


def _create_dataset(ds_dir: Path) -> None:
    rows = [
        {
            "drug_name":    "DrugA" if i < 50 else "DrugB",
            "hcpcs_code":   f"J{i + 1:04d}",
            "total_paid":   100.0 if i < 50 else 200.0,
            "total_claims": i + 1,
        }
        for i in range(100)
    ]
    df = pd.DataFrame(rows)
    df["total_paid"]   = df["total_paid"].astype("float64")
    df["total_claims"] = df["total_claims"].astype("int64")
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    meta = {
        "row_count": 100, "column_count": len(EXPECTED_COLUMNS),
        "columns": EXPECTED_COLUMNS, "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")


@pytest.fixture(scope="module")
def datasets_tmp(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("aw_queries")
    d = tmp / DATASET
    d.mkdir()
    _create_dataset(d)
    return tmp


@pytest.fixture(scope="module")
def queries_tmp(tmp_path_factory):
    """Isolated queries.json path so tests don't touch the real file."""
    return tmp_path_factory.mktemp("aw_queries_store") / "queries.json"


@pytest.fixture(scope="module")
def client(datasets_tmp, queries_tmp):
    orig_ds  = main_module.DATASETS_DIR
    orig_qp  = main_module.QUERIES_PATH
    main_module.DATASETS_DIR = datasets_tmp
    main_module.QUERIES_PATH = queries_tmp
    with TestClient(main_module.app) as c:
        yield c
    main_module.DATASETS_DIR = orig_ds
    main_module.QUERIES_PATH = orig_qp


# ===========================================================================
# GET /api/queries — list
# ===========================================================================

# Prevents the queries list from crashing when no queries file exists
def test_queries_list_empty_when_no_file(client):
    resp = client.get("/api/queries")
    assert resp.status_code == 200
    assert resp.json()["queries"] == []


# ===========================================================================
# POST /api/queries/save — SQL type
# ===========================================================================

# Prevents a valid SQL query from being rejected by the save endpoint
def test_queries_save_sql_type_returns_ok(client):
    resp = client.post("/api/queries/save", json={
        "name":    "Top drugs",
        "dataset": DATASET,
        "type":    "sql",
        "sql":     "SELECT drug_name, SUM(total_paid) AS s FROM dataset GROUP BY drug_name",
    })
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


# Prevents the saved query from losing its SQL content
def test_queries_save_sql_content_is_preserved(client):
    sql = "SELECT drug_name, COUNT(*) AS n FROM dataset GROUP BY drug_name"
    client.post("/api/queries/save", json={
        "name": "Drug count", "dataset": DATASET, "type": "sql", "sql": sql,
    })
    queries = client.get("/api/queries").json()["queries"]
    match = next((q for q in queries if q["name"] == "Drug count"), None)
    assert match is not None, "Saved query not found in list"
    assert match["sql"] == sql


# Prevents the saved query from having the wrong dataset recorded
def test_queries_save_records_correct_dataset(client):
    client.post("/api/queries/save", json={
        "name": "Dataset check", "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    queries = client.get("/api/queries").json()["queries"]
    match = next((q for q in queries if q["name"] == "Dataset check"), None)
    assert match["dataset"] == DATASET


# Prevents saving a query with same name from duplicating instead of replacing
def test_queries_save_overwrites_existing_name(client):
    name = "Overwrite test"
    client.post("/api/queries/save", json={
        "name": name, "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    client.post("/api/queries/save", json={
        "name": name, "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 5",
    })
    queries = client.get("/api/queries").json()["queries"]
    matches = [q for q in queries if q["name"] == name]
    assert len(matches) == 1, "Duplicate query names — expected overwrite"
    assert "LIMIT 5" in matches[0]["sql"]


# Prevents the replaced flag from being wrong on an overwrite
def test_queries_save_overwrite_sets_replaced_true(client):
    name = "Replace flag test"
    client.post("/api/queries/save", json={
        "name": name, "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    resp = client.post("/api/queries/save", json={
        "name": name, "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 2",
    })
    assert resp.json().get("replaced") is True


# ===========================================================================
# POST /api/queries/save — validation
# ===========================================================================

# Prevents empty query name from being silently accepted
def test_queries_save_empty_name_returns_400(client):
    resp = client.post("/api/queries/save", json={
        "name": "", "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    assert resp.status_code == 400


# Prevents whitespace-only name from being accepted
def test_queries_save_whitespace_name_returns_400(client):
    resp = client.post("/api/queries/save", json={
        "name": "   ", "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    assert resp.status_code == 400


# Prevents saving a query against a dataset that doesn't exist
def test_queries_save_nonexistent_dataset_returns_404(client):
    resp = client.post("/api/queries/save", json={
        "name": "Bad dataset", "dataset": "does_not_exist_xyz", "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    assert resp.status_code == 404


# Prevents invalid query type from being saved silently
def test_queries_save_invalid_type_returns_400(client):
    resp = client.post("/api/queries/save", json={
        "name": "Invalid type", "dataset": DATASET, "type": "invalid_type",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    assert resp.status_code == 400


# Prevents dangerous SQL from being saved (same safety as /api/sql)
def test_queries_save_blocks_dangerous_sql(client):
    resp = client.post("/api/queries/save", json={
        "name": "Evil query", "dataset": DATASET, "type": "sql",
        "sql": "DROP TABLE dataset",
    })
    assert resp.status_code == 400


# ===========================================================================
# POST /api/queries/delete
# ===========================================================================

# Prevents delete from failing on a query that exists
def test_queries_delete_existing_returns_ok(client):
    name = "To be deleted"
    client.post("/api/queries/save", json={
        "name": name, "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    resp = client.post("/api/queries/delete", json={"name": name})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


# Prevents deleted query from still appearing in the list
def test_queries_delete_removes_from_list(client):
    name = "Disappear me"
    client.post("/api/queries/save", json={
        "name": name, "dataset": DATASET, "type": "sql",
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    client.post("/api/queries/delete", json={"name": name})
    queries = client.get("/api/queries").json()["queries"]
    assert all(q["name"] != name for q in queries)


# Prevents deleting a nonexistent query from returning 200
def test_queries_delete_nonexistent_returns_404(client):
    resp = client.post("/api/queries/delete", json={"name": "does_not_exist_xyz"})
    assert resp.status_code == 404


# Prevents empty name from being accepted by delete
def test_queries_delete_empty_name_returns_400(client):
    resp = client.post("/api/queries/delete", json={"name": ""})
    assert resp.status_code == 400


# ===========================================================================
# GET /api/profile
# ===========================================================================

# Prevents the profile endpoint from returning non-200 for a valid dataset
def test_profile_returns_200(client):
    assert client.get(f"/api/profile?dataset={DATASET}").status_code == 200


# Prevents profile from losing its columns list
def test_profile_contains_columns(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    assert "columns" in data
    assert len(data["columns"]) > 0


# Prevents profile column names from being wrong
def test_profile_column_names_match_dataset(client):
    data = client.get(f"/api/profile?dataset={DATASET}").json()
    names = [c["name"] for c in data["columns"]]
    for col in EXPECTED_COLUMNS:
        assert col in names, f"Column '{col}' missing from profile"


# Prevents profile from returning 200 for a nonexistent dataset
def test_profile_404_for_nonexistent(client):
    assert client.get("/api/profile?dataset=does_not_exist_xyz").status_code == 404
