"""
test_sql_extended.py — extended SQL endpoint tests

Covers gaps not in test_sql.py:
  POST /api/sql       — edge cases: empty results, NULLs, division by zero
  POST /api/sql/export — both formats, size limits, error handling
  POST /api/sql/generate — schema-aware fallback (no AI)

Run from project root:
    pytest tests/test_sql_extended.py -v
"""
from __future__ import annotations

import io
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

DATASET = "aw_test_sql_ext"

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


@pytest.fixture(scope="module")
def datasets_tmp(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("aw_sql_ext")
    d = tmp / DATASET
    d.mkdir()
    _create_dataset(d)
    return tmp


@pytest.fixture(scope="module")
def client(datasets_tmp):
    orig_ds = main_module.DATASETS_DIR
    main_module.DATASETS_DIR = datasets_tmp
    with TestClient(main_module.app) as c:
        yield c
    main_module.DATASETS_DIR = orig_ds


# ===========================================================================
# POST /api/sql — edge cases
# ===========================================================================

# Prevents empty result set from causing a crash or wrong rowcount
def test_sql_empty_result_set_returns_zero_rows(client):
    resp = client.post("/api/sql", json={
        "dataset": DATASET,
        "sql": "SELECT * FROM dataset WHERE drug_name = 'DoesNotExist'",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["rowcount"] == 0
    assert data["rows"] == []
    assert isinstance(data["columns"], list)


# Prevents rowcount from being wrong when result is exactly 0
def test_sql_empty_result_rowcount_is_zero_not_none(client):
    resp = client.post("/api/sql", json={
        "dataset": DATASET,
        "sql": "SELECT * FROM dataset WHERE total_paid < 0",
    })
    assert resp.status_code == 200
    assert resp.json()["rowcount"] == 0


# Prevents NULL values in results from crashing JSON serialisation
def test_sql_null_values_serialise_as_none(client):
    # CASE expression produces NULL for DrugA rows
    resp = client.post("/api/sql", json={
        "dataset": DATASET,
        "sql": (
            "SELECT drug_name, "
            "CASE WHEN drug_name = 'DrugB' THEN total_paid ELSE NULL END AS paid "
            "FROM dataset LIMIT 10"
        ),
    })
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    null_rows = [r for r in rows if r["paid"] is None]
    assert len(null_rows) > 0, "Expected at least one NULL-valued row"


# Prevents scalar (single-row single-column) results from breaking the response
def test_sql_scalar_count_result_works(client):
    resp = client.post("/api/sql", json={
        "dataset": DATASET,
        "sql": "SELECT COUNT(*) AS n FROM dataset",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["rowcount"] == 1
    assert data["rows"][0]["n"] == 100


# Prevents a query returning exactly 1 row from being mistaken for an error
def test_sql_single_row_result_works(client):
    resp = client.post("/api/sql", json={
        "dataset": DATASET,
        "sql": "SELECT * FROM dataset LIMIT 1",
    })
    assert resp.status_code == 200
    assert resp.json()["rowcount"] == 1
    assert len(resp.json()["rows"]) == 1


# Prevents type errors in computed columns from returning 200 with bad data
def test_sql_type_error_in_expression_returns_400(client):
    # Adding an integer to a varchar is a type error in DuckDB
    resp = client.post("/api/sql", json={
        "dataset": DATASET,
        "sql": "SELECT total_claims + drug_name AS bad FROM dataset LIMIT 1",
    })
    assert resp.status_code == 400


# DuckDB returns inf for integer division by zero (not an error).
# The app sanitizes inf/nan to None so the JSON response is valid.
def test_sql_integer_division_by_zero_returns_sanitized(client):
    resp = client.post("/api/sql", json={
        "dataset": DATASET,
        "sql": "SELECT total_claims / 0 AS x FROM dataset LIMIT 1",
    })
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 1
    # inf is sanitized to None for JSON compliance
    assert rows[0]["x"] is None


# ===========================================================================
# POST /api/sql/export — format validation
# ===========================================================================

# Prevents unsupported export format from returning 200
def test_sql_export_invalid_format_returns_400(client):
    resp = client.post("/api/sql/export", json={
        "dataset": DATASET,
        "sql": "SELECT * FROM dataset LIMIT 5",
        "format": "pdf",
    })
    assert resp.status_code == 400


# Prevents nonexistent dataset from being exported silently
def test_sql_export_nonexistent_dataset_returns_404(client):
    resp = client.post("/api/sql/export", json={
        "dataset": "does_not_exist_xyz",
        "sql": "SELECT * FROM dataset LIMIT 5",
        "format": "tsv",
    })
    assert resp.status_code == 404


# Prevents blocked SQL from being exported (same safety as /api/sql)
def test_sql_export_blocks_dangerous_sql(client):
    resp = client.post("/api/sql/export", json={
        "dataset": DATASET,
        "sql": "DROP TABLE dataset",
        "format": "tsv",
    })
    assert resp.status_code == 400


# Prevents TSV export from returning wrong content type or empty body
def test_sql_export_tsv_returns_file(client):
    resp = client.post("/api/sql/export", json={
        "dataset": DATASET,
        "sql": "SELECT drug_name, total_paid FROM dataset LIMIT 5",
        "format": "tsv",
    })
    assert resp.status_code == 200
    ct = resp.headers.get("content-type", "")
    assert "tab-separated" in ct or "text" in ct or "octet" in ct
    # Body must have at least a header row and one data row
    content = resp.content.decode("utf-8", errors="replace")
    lines = [l for l in content.splitlines() if l.strip()]
    assert len(lines) >= 2, f"Expected header + data rows in TSV, got: {lines}"


# Prevents XLSX export from returning an empty or non-binary response
def test_sql_export_xlsx_returns_file(client):
    resp = client.post("/api/sql/export", json={
        "dataset": DATASET,
        "sql": "SELECT drug_name, total_paid FROM dataset LIMIT 5",
        "format": "xlsx",
    })
    assert resp.status_code == 200
    ct = resp.headers.get("content-type", "")
    assert "spreadsheet" in ct or "excel" in ct or "octet" in ct or "zip" in ct
    assert len(resp.content) > 100, "XLSX file appears empty"


# Prevents TSV column headers from being wrong or missing
def test_sql_export_tsv_column_headers_match_select(client):
    resp = client.post("/api/sql/export", json={
        "dataset": DATASET,
        "sql": "SELECT drug_name, total_paid FROM dataset LIMIT 3",
        "format": "tsv",
    })
    assert resp.status_code == 200
    first_line = resp.content.decode("utf-8").splitlines()[0]
    assert "drug_name" in first_line
    assert "total_paid" in first_line


# Prevents export from silently ignoring the SQL filter
def test_sql_export_tsv_respects_where_clause(client):
    resp = client.post("/api/sql/export", json={
        "dataset": DATASET,
        "sql": "SELECT * FROM dataset WHERE drug_name = 'DrugA'",
        "format": "tsv",
    })
    assert resp.status_code == 200
    lines = [l for l in resp.content.decode("utf-8").splitlines() if l.strip()]
    # header + 50 DrugA rows
    assert len(lines) == 51, f"Expected 51 lines (header + 50 rows), got {len(lines)}"


# ===========================================================================
# POST /api/sql/generate — schema-aware fallback (no AI)
# ===========================================================================

# Prevents the fallback from being broken or returning wrong status
def test_sql_generate_fallback_returns_200(client):
    resp = client.post("/api/sql/generate", json={
        "dataset": DATASET,
        "question": "show me the top drugs",
    })
    assert resp.status_code == 200


# Prevents the fallback from returning empty SQL
def test_sql_generate_fallback_returns_sql(client):
    resp = client.post("/api/sql/generate", json={
        "dataset": DATASET,
        "question": "show me the top drugs",
    })
    assert resp.json().get("sql"), "Fallback SQL must not be empty"


# Prevents the fallback SQL from referencing a nonexistent table
def test_sql_generate_fallback_sql_uses_dataset_table_name(client):
    resp = client.post("/api/sql/generate", json={
        "dataset": DATASET,
        "question": "anything",
    })
    sql = resp.json()["sql"]
    assert "FROM dataset" in sql, f"Fallback SQL must use 'dataset' table name, got: {sql}"


# Prevents the fallback from being confused with an AI response
def test_sql_generate_fallback_source_field_is_schema_starter(client):
    resp = client.post("/api/sql/generate", json={
        "dataset": DATASET,
        "question": "top drugs",
    })
    assert resp.json().get("source") == "schema_starter"


# Prevents the fallback from being used for nonexistent datasets
def test_sql_generate_fallback_404_for_nonexistent_dataset(client):
    resp = client.post("/api/sql/generate", json={
        "dataset": "does_not_exist_xyz",
        "question": "anything",
    })
    assert resp.status_code == 404


# Prevents missing dataset field from causing a 500
def test_sql_generate_fallback_missing_dataset_returns_400(client):
    resp = client.post("/api/sql/generate", json={"question": "anything"})
    assert resp.status_code in (400, 422)


# Prevents the limit in fallback SQL from ignoring a number in the question
def test_sql_generate_fallback_picks_up_limit_from_question(client):
    resp = client.post("/api/sql/generate", json={
        "dataset": DATASET,
        "question": "show me 25 rows",
    })
    assert resp.status_code == 200
    sql = resp.json()["sql"]
    assert "25" in sql, f"Expected LIMIT 25 from question, got: {sql}"
