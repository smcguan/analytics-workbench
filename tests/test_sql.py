"""
test_sql.py — pytest tests for the /api/sql execution endpoint

Covers:
  POST /api/sql — basic execution, SQL rewriting, safety validation,
                  error handling, and critical bug regressions

Run from project root:
    pytest tests/test_sql.py -v
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

DATASET = "aw_test_sql"

EXPECTED_COLUMNS = [
    "drug_name", "hcpcs_code", "total_paid", "total_claims", "service_year",
]


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
    tmp = tmp_path_factory.mktemp("aw_sql")
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


def _run(client, sql: str, dataset: str = DATASET) -> dict:
    """POST /api/sql and return the JSON response body."""
    return client.post("/api/sql", json={"dataset": dataset, "sql": sql})


# ===========================================================================
# RESPONSE SHAPE
# ===========================================================================

# Prevents required response keys from being silently dropped
def test_sql_response_has_required_keys(client):
    resp = _run(client, "SELECT * FROM dataset LIMIT 1")
    assert resp.status_code == 200
    data = resp.json()
    for key in ("columns", "rows", "rowcount", "elapsed_seconds", "visualization"):
        assert key in data, f"Response missing key: {key}"


# Prevents visualization block from disappearing between refactors
def test_sql_response_visualization_block_present(client):
    resp = _run(client, "SELECT * FROM dataset LIMIT 1")
    viz = resp.json()["visualization"]
    assert "recommended" in viz


# ===========================================================================
# SQL REWRITING — FROM dataset
# ===========================================================================

# Prevents the FROM dataset rewrite from breaking so queries return no rows
def test_sql_from_dataset_returns_data(client):
    resp = _run(client, "SELECT * FROM dataset LIMIT 10")
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 10


# Prevents FROM <dataset_name> backward-compat alias from breaking
def test_sql_from_dataset_name_alias_works(client):
    resp = _run(client, f"SELECT COUNT(*) AS n FROM {DATASET}")
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["n"] == 100


# Prevents trailing semicolons from causing a parse error in the wrapped query
def test_sql_trailing_semicolon_is_stripped(client):
    resp = _run(client, "SELECT COUNT(*) AS n FROM dataset;")
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["n"] == 100


# Prevents double-semicolons from breaking the strip logic
def test_sql_multiple_trailing_semicolons_stripped(client):
    resp = _run(client, "SELECT COUNT(*) AS n FROM dataset;;")
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["n"] == 100


# ===========================================================================
# CORRECT RESULTS
# ===========================================================================

# Prevents aggregation queries from returning wrong totals
def test_sql_aggregate_sum_is_correct(client):
    resp = _run(client, "SELECT SUM(total_paid) AS s FROM dataset")
    assert resp.status_code == 200
    # 50 rows * 100.0 + 50 rows * 200.0 = 15000.0
    assert resp.json()["rows"][0]["s"] == 15000.0


# Prevents WHERE filters from being silently ignored
def test_sql_where_filter_reduces_rowcount(client):
    resp = _run(client, "SELECT * FROM dataset WHERE drug_name = 'DrugA'")
    assert resp.status_code == 200
    assert resp.json()["rowcount"] == 50


# Prevents GROUP BY queries from returning wrong group counts
def test_sql_group_by_returns_correct_groups(client):
    resp = _run(client, "SELECT drug_name, COUNT(*) AS n FROM dataset GROUP BY drug_name ORDER BY drug_name")
    assert resp.status_code == 200
    rows = {r["drug_name"]: r["n"] for r in resp.json()["rows"]}
    assert rows == {"DrugA": 50, "DrugB": 50}


# Prevents rowcount from being wrong when rows > MAX_PREVIEW_ROWS
def test_sql_rowcount_reflects_full_result_not_preview(client):
    resp = _run(client, "SELECT * FROM dataset")
    assert resp.status_code == 200
    data = resp.json()
    assert data["rowcount"] == 100


# Prevents column names from being dropped or reordered in the response
def test_sql_column_names_match_select(client):
    resp = _run(client, "SELECT drug_name, total_paid FROM dataset LIMIT 1")
    assert resp.status_code == 200
    assert resp.json()["columns"] == ["drug_name", "total_paid"]


# Prevents CTE (WITH) queries from being rejected by the safety validator
def test_sql_cte_query_is_accepted(client):
    sql = (
        "WITH summary AS (SELECT drug_name, SUM(total_paid) AS s FROM dataset GROUP BY drug_name) "
        "SELECT * FROM summary ORDER BY s DESC"
    )
    resp = _run(client, sql)
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 2


# ===========================================================================
# SAFETY VALIDATION — BLOCKED KEYWORDS
# ===========================================================================

# Prevents non-SELECT statements from being executed silently
def test_sql_blocks_empty_sql(client):
    assert _run(client, "").status_code == 400


# Prevents INSERT from slipping through the safety validator
def test_sql_blocks_insert(client):
    assert _run(client, "INSERT INTO dataset VALUES (1)").status_code == 400


# Prevents UPDATE from slipping through
def test_sql_blocks_update(client):
    assert _run(client, "UPDATE dataset SET total_paid = 0").status_code == 400


# Prevents DELETE from slipping through
def test_sql_blocks_delete(client):
    assert _run(client, "DELETE FROM dataset WHERE 1=1").status_code == 400


# Prevents DROP from slipping through
def test_sql_blocks_drop(client):
    assert _run(client, "DROP TABLE dataset").status_code == 400


# Prevents ATTACH from slipping through (could expose other DuckDB databases)
def test_sql_blocks_attach(client):
    assert _run(client, "ATTACH 'evil.db' AS evil").status_code == 400


# ===========================================================================
# SAFETY — FALSE POSITIVE PREVENTION (Bug 2 regression)
# ===========================================================================

# Prevents 'update' inside a quoted NOT LIKE string from triggering the blocker.
# This was Bug 2: SQL with drug names like 'Opdivo', 'Keytruda' that happen to
# contain blocked words in LIKE patterns were silently rejected around ~26 conditions.
def test_sql_blocked_word_inside_quoted_string_is_allowed(client):
    # 'update' appears inside a quoted string — must NOT be blocked
    sql = "SELECT * FROM dataset WHERE drug_name NOT LIKE '%update%'"
    resp = _run(client, sql)
    assert resp.status_code == 200


# Prevents 'delete' inside a quoted IN list from triggering the blocker
def test_sql_blocked_word_inside_in_list_is_allowed(client):
    sql = "SELECT * FROM dataset WHERE drug_name NOT IN ('delete-drug', 'DrugA')"
    resp = _run(client, sql)
    assert resp.status_code == 200


# Regression: many NOT LIKE conditions where one value contains a blocked word
def test_sql_many_not_like_conditions_with_blocked_word_in_value(client):
    # Build 30 NOT LIKE conditions, one of which contains the word 'drop'
    conditions = " AND ".join(
        [f"drug_name NOT LIKE 'drug_{i}%'" for i in range(29)]
        + ["drug_name NOT LIKE '%drop-excluded%'"]
    )
    sql = f"SELECT COUNT(*) AS n FROM dataset WHERE {conditions}"
    resp = _run(client, sql)
    assert resp.status_code == 200
    # All 100 rows survive since none match the excluded names
    assert resp.json()["rows"][0]["n"] == 100


# Bug 2 stress test: 45 NOT LIKE conditions with multiple blocked keywords
# scattered in the quoted values.  This exceeds the ~26-condition threshold
# where the original bug manifested.
def test_sql_45_not_like_conditions_with_multiple_blocked_keywords(client):
    # Drug names that contain blocked SQL keywords inside quotes
    keyword_drugs = [
        "Updaterol", "Insertase", "Dropivir", "Deletumab",
        "Alterixin", "Copyzine", "Attachol", "Detachase",
        "Createnil", "Alteplase",
    ]
    generic_drugs = [f"GenericDrug_{i}" for i in range(35)]
    all_drugs = keyword_drugs + generic_drugs
    conditions = " AND ".join(
        f"drug_name NOT LIKE '{name}%'" for name in all_drugs
    )
    sql = f"SELECT COUNT(*) AS n FROM dataset WHERE {conditions}"
    resp = _run(client, sql)
    assert resp.status_code == 200, (
        f"Bug 2 regression: 45 NOT LIKE conditions with blocked keywords in "
        f"values should succeed, got {resp.status_code}: {resp.text[:300]}"
    )
    assert resp.json()["rows"][0]["n"] == 100


# Bug 2 stress test: 40 NOT IN values with blocked keywords
def test_sql_40_not_in_values_with_blocked_keywords(client):
    values = [f"Drug_{i}" for i in range(35)]
    values.extend(["DropExcluded", "UpdateTest", "DeleteOld", "AlterDrug", "InsertNew"])
    in_list = ", ".join(f"'{v}'" for v in values)
    sql = f"SELECT COUNT(*) AS n FROM dataset WHERE drug_name NOT IN ({in_list})"
    resp = _run(client, sql)
    assert resp.status_code == 200
    assert resp.json()["rows"][0]["n"] == 100


# ===========================================================================
# ERROR HANDLING — INVALID SQL (Bug 1 regression)
# ===========================================================================

# CRITICAL regression: invalid DuckDB syntax must return 400, not silent 200.
# Bug 1 was: DuckDB errors were being swallowed, returning unfiltered data
# instead of surfacing the actual error message.
# CHARINDEX is SQL Server syntax that DuckDB does not support.
def test_sql_invalid_duckdb_syntax_returns_400(client):
    # CHARINDEX is SQL Server-specific — DuckDB uses STRPOS instead.
    # This must return 400 with an error, not 200 with data.
    sql = "SELECT CHARINDEX('Drug', drug_name) AS pos FROM dataset"
    resp = _run(client, sql)
    assert resp.status_code == 400, (
        f"Expected 400 for invalid DuckDB syntax but got {resp.status_code}. "
        f"Body: {resp.text[:300]}"
    )


# Bug 1 clarification: regexp_replace with 'g' flag is VALID DuckDB syntax.
# The original bug report used this as an example of "invalid syntax", but
# DuckDB supports the 'g' flag. This test confirms it executes correctly
# and returns filtered results — not the entire unfiltered dataset.
def test_sql_regexp_replace_with_g_flag_is_valid_duckdb(client):
    sql = (
        "SELECT drug_name, regexp_replace(drug_name, 'Drug', 'Med', 'g') AS replaced "
        "FROM dataset WHERE drug_name = 'DrugA' LIMIT 5"
    )
    resp = _run(client, sql)
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) > 0
    # Verify the replacement actually worked — not returning unfiltered data
    for r in rows:
        assert r["replaced"] == "MedA"
        assert r["drug_name"] == "DrugA"


# Bug 1 regression: the actual DuckDB error message must be in the response
# detail field, not a generic "Internal Server Error"
def test_sql_error_detail_contains_duckdb_message(client):
    sql = "SELECT nonexistent_column_xyz FROM dataset"
    resp = _run(client, sql)
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    # The DuckDB error should mention the column name
    assert "nonexistent_column_xyz" in detail.lower() or "not found" in detail.lower()


# Prevents the error message from being swallowed — user must see what went wrong
def test_sql_error_response_contains_detail(client):
    sql = "SELECT nonexistent_column_xyz FROM dataset"
    resp = _run(client, sql)
    assert resp.status_code == 400
    body = resp.json()
    assert "detail" in body
    assert body["detail"]  # must be non-empty


# Prevents syntax errors from returning 200 with an empty result instead of 400
def test_sql_syntax_error_returns_400(client):
    resp = _run(client, "SELECT FROM WHERE")
    assert resp.status_code == 400


# ===========================================================================
# DATASET NOT FOUND
# ===========================================================================

# Prevents /api/sql from returning 500 for nonexistent datasets
def test_sql_404_for_nonexistent_dataset(client):
    resp = _run(client, "SELECT * FROM dataset", dataset="does_not_exist_xyz")
    assert resp.status_code == 404


# ===========================================================================
# FROM CLAUSE MISSING
# ===========================================================================

# Prevents queries without a FROM clause from silently executing against nothing
def test_sql_no_from_clause_returns_400(client):
    # SELECT 1 has no FROM and no dataset reference — rewrite should fail
    resp = _run(client, "SELECT 1 AS x")
    # Should be 400 because the FROM dataset rewrite can't find a match
    assert resp.status_code == 400
