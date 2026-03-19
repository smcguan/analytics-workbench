"""
test_result_passport.py — unit tests for POST /api/results/passport

Tests the Result Passport endpoint which generates a structured summary
of query results for clipboard sharing with external AI assistants.

Covers:
  Basic profiling — string and numeric columns
  Display-cap fix — total_rowcount overrides len(rows)
  Sampling note when row_count > sampled rows
  Data quality flags — high null rate, looks-numeric-but-string
  Empty/missing data handling
  Top values computation
  Numeric stats (min, max, mean, median)

Run from project root:
    pytest tests/test_result_passport.py -v
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def _passport(columns, rows, sql="", total_rowcount=None):
    body = {"columns": columns, "rows": rows, "sql": sql}
    if total_rowcount is not None:
        body["total_rowcount"] = total_rowcount
    resp = client.post("/api/results/passport", json=body)
    assert resp.status_code == 200
    return resp.json()


# ===========================================================================
# BASIC PROFILING
# ===========================================================================

def test_basic_string_column_profile():
    data = _passport(
        columns=["drug"],
        rows=[{"drug": "Keytruda"}, {"drug": "Opdivo"}, {"drug": "Keytruda"}],
    )
    assert data["row_count"] == 3
    assert data["column_count"] == 1
    profile = data["per_column_profile"][0]
    assert profile["column"] == "drug"
    assert profile["type"] == "string"
    assert profile["distinct_count"] == 2
    assert profile["top_values"][0]["value"] == "Keytruda"
    assert profile["top_values"][0]["count"] == 2


def test_basic_numeric_column_profile():
    data = _passport(
        columns=["spending"],
        rows=[{"spending": 100}, {"spending": 200}, {"spending": 300}],
    )
    profile = data["per_column_profile"][0]
    assert profile["type"] == "numeric"
    assert profile["min"] == 100
    assert profile["max"] == 300
    assert profile["mean"] == 200
    assert profile["median"] == 200


def test_mixed_columns():
    data = _passport(
        columns=["drug", "spending"],
        rows=[
            {"drug": "A", "spending": 100},
            {"drug": "B", "spending": 200},
        ],
    )
    assert data["column_count"] == 2
    types = {p["column"]: p["type"] for p in data["per_column_profile"]}
    assert types["drug"] == "string"
    assert types["spending"] == "numeric"


# ===========================================================================
# DISPLAY-CAP FIX — total_rowcount
# ===========================================================================

def test_total_rowcount_overrides_len_rows():
    """Row count should reflect total_rowcount, not the number of rows passed."""
    data = _passport(
        columns=["drug"],
        rows=[{"drug": "A"}, {"drug": "B"}],
        total_rowcount=304,
    )
    assert data["row_count"] == 304


def test_sampling_note_when_capped():
    """When total > sampled, a note should explain the sampling."""
    data = _passport(
        columns=["drug"],
        rows=[{"drug": "A"}],
        total_rowcount=500,
    )
    assert "note" in data
    assert "1 displayed rows" in data["note"]
    assert "500 total" in data["note"]


def test_no_note_when_not_capped():
    """When total == sampled, no sampling note needed."""
    data = _passport(
        columns=["drug"],
        rows=[{"drug": "A"}, {"drug": "B"}],
    )
    assert "note" not in data


def test_total_rowcount_none_falls_back_to_len():
    """Without total_rowcount, row_count should be len(rows)."""
    data = _passport(
        columns=["drug"],
        rows=[{"drug": "A"}, {"drug": "B"}, {"drug": "C"}],
    )
    assert data["row_count"] == 3


# ===========================================================================
# NULL HANDLING
# ===========================================================================

def test_null_count_and_pct():
    data = _passport(
        columns=["spending"],
        rows=[{"spending": 100}, {"spending": None}, {"spending": 300}, {"spending": None}],
    )
    profile = data["per_column_profile"][0]
    assert profile["null_count"] == 2
    assert profile["null_pct"] == 50.0


def test_all_null_column():
    data = _passport(
        columns=["value"],
        rows=[{"value": None}, {"value": None}],
    )
    profile = data["per_column_profile"][0]
    assert profile["null_count"] == 2
    assert profile["null_pct"] == 100.0


# ===========================================================================
# DATA QUALITY FLAGS
# ===========================================================================

def test_high_null_rate_flagged():
    rows = [{"x": None}] * 3 + [{"x": "A"}] * 2  # 60% null
    data = _passport(columns=["x"], rows=rows)
    flags = [f for f in data["data_quality_flags"] if f["flag"] == "high_null_rate"]
    assert len(flags) == 1
    assert flags[0]["column"] == "x"


def test_looks_numeric_but_string_flagged():
    """String column where top values are all numeric should be flagged.
    Must include at least one non-numeric value so the column stays typed
    as string (otherwise all-numeric values get classified as numeric)."""
    rows = [{"code": str(i)} for i in range(20)] + [{"code": "N/A"}]
    data = _passport(columns=["code"], rows=rows)
    flags = [f for f in data["data_quality_flags"]
             if f["flag"] == "looks_numeric_but_stored_as_text"]
    assert len(flags) == 1


# ===========================================================================
# TOP VALUES
# ===========================================================================

def test_top_values_limited_to_15():
    rows = [{"x": f"val_{i}"} for i in range(30)]
    data = _passport(columns=["x"], rows=rows)
    profile = data["per_column_profile"][0]
    assert len(profile["top_values"]) <= 15


def test_top_values_ordered_by_count():
    rows = [{"x": "A"}] * 5 + [{"x": "B"}] * 3 + [{"x": "C"}] * 1
    data = _passport(columns=["x"], rows=rows)
    top = data["per_column_profile"][0]["top_values"]
    assert top[0]["value"] == "A"
    assert top[0]["count"] == 5
    assert top[1]["value"] == "B"
    assert top[1]["count"] == 3


# ===========================================================================
# GRAIN HINT
# ===========================================================================

def test_grain_hint_contains_sql():
    sql = "SELECT drug, SUM(spending) FROM dataset GROUP BY drug"
    data = _passport(
        columns=["drug", "spending"],
        rows=[{"drug": "A", "spending": 100}],
        sql=sql,
    )
    assert data["grain_hint"] == sql


# ===========================================================================
# ERROR HANDLING
# ===========================================================================

def test_empty_rows_returns_400():
    resp = client.post("/api/results/passport", json={
        "columns": ["drug"],
        "rows": [],
    })
    assert resp.status_code == 400


def test_empty_columns_returns_400():
    resp = client.post("/api/results/passport", json={
        "columns": [],
        "rows": [{"drug": "A"}],
    })
    assert resp.status_code == 400
