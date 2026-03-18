"""
test_sanitize_row.py — unit tests for _sanitize_json_row

This function prevents FastAPI from crashing with 'Out of range float values
are not JSON compliant' when DuckDB returns inf or NaN in query results.

Covers:
  float('inf') and float('-inf') replaced with None
  float('nan') replaced with None
  Normal floats, ints, strings, None — passed through unchanged
  Mixed rows with some inf/nan and some normal values
  Empty rows

Run from project root:
    pytest tests/test_sanitize_row.py -v
"""
from __future__ import annotations

import math

from app.main import _sanitize_json_row


# ===========================================================================
# INF / NAN → None
# ===========================================================================

def test_positive_inf_replaced():
    row = {"x": float("inf")}
    assert _sanitize_json_row(row) == {"x": None}


def test_negative_inf_replaced():
    row = {"x": float("-inf")}
    assert _sanitize_json_row(row) == {"x": None}


def test_nan_replaced():
    row = {"x": float("nan")}
    result = _sanitize_json_row(row)
    assert result["x"] is None


# ===========================================================================
# NORMAL VALUES — passed through unchanged
# ===========================================================================

def test_normal_float_preserved():
    row = {"price": 42.99}
    assert _sanitize_json_row(row) == {"price": 42.99}


def test_zero_float_preserved():
    row = {"val": 0.0}
    assert _sanitize_json_row(row) == {"val": 0.0}


def test_negative_float_preserved():
    row = {"change": -15.5}
    assert _sanitize_json_row(row) == {"change": -15.5}


def test_integer_preserved():
    row = {"count": 100}
    assert _sanitize_json_row(row) == {"count": 100}


def test_string_preserved():
    row = {"name": "DrugA"}
    assert _sanitize_json_row(row) == {"name": "DrugA"}


def test_none_preserved():
    row = {"val": None}
    assert _sanitize_json_row(row) == {"val": None}


def test_bool_preserved():
    row = {"flag": True}
    assert _sanitize_json_row(row) == {"flag": True}


# ===========================================================================
# MIXED ROWS
# ===========================================================================

def test_mixed_row_only_inf_replaced():
    row = {
        "name": "DrugA",
        "paid": 100.50,
        "ratio": float("inf"),
        "count": 42,
        "note": None,
    }
    result = _sanitize_json_row(row)
    assert result["name"] == "DrugA"
    assert result["paid"] == 100.50
    assert result["ratio"] is None
    assert result["count"] == 42
    assert result["note"] is None


def test_multiple_inf_nan_in_same_row():
    row = {
        "a": float("inf"),
        "b": float("-inf"),
        "c": float("nan"),
        "d": 99.9,
    }
    result = _sanitize_json_row(row)
    assert result["a"] is None
    assert result["b"] is None
    assert result["c"] is None
    assert result["d"] == 99.9


# ===========================================================================
# EDGE CASES
# ===========================================================================

def test_empty_row():
    assert _sanitize_json_row({}) == {}


def test_very_large_float_not_replaced():
    """Large but finite floats should NOT be replaced."""
    row = {"big": 1e308}
    result = _sanitize_json_row(row)
    assert result["big"] == 1e308


def test_very_small_float_not_replaced():
    row = {"tiny": 1e-308}
    result = _sanitize_json_row(row)
    assert result["tiny"] == 1e-308


def test_result_is_json_serializable():
    """The sanitized row must be serializable by json.dumps."""
    import json
    row = {
        "a": float("inf"),
        "b": float("nan"),
        "c": "text",
        "d": 42,
        "e": None,
    }
    result = _sanitize_json_row(row)
    # This would raise ValueError without sanitization
    serialized = json.dumps(result)
    assert '"a": null' in serialized
    assert '"b": null' in serialized
