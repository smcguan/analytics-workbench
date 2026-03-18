"""
test_chart_recommender.py — unit tests for chart_recommender.recommend_chart

Covers:
  Bar chart recommendation (categorical x, numeric y, 2-50 rows)
  Line chart recommendation (datetime x, numeric y, 2+ rows)
  No-chart guard rails (wrong column count, not enough rows, non-numeric y)
  Column type inference edge cases (NaN, None, mixed types, date patterns)

Run from project root:
    pytest tests/test_chart_recommender.py -v
"""
from __future__ import annotations

import pytest

from app.services.chart_recommender import recommend_chart


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------

def _rows(x_col: str, y_col: str, pairs: list[tuple]) -> list[dict]:
    """Build row dicts from (x, y) pairs."""
    return [{x_col: x, y_col: y} for x, y in pairs]


# ===========================================================================
# GUARD RAILS — no chart recommended
# ===========================================================================

def test_no_chart_when_zero_columns():
    assert recommend_chart([], [])["recommended"] is False


def test_no_chart_when_one_column():
    rows = [{"a": 1}, {"a": 2}]
    assert recommend_chart(["a"], rows)["recommended"] is False


def test_no_chart_when_three_columns():
    rows = [{"a": "X", "b": 1, "c": 2}, {"a": "Y", "b": 3, "c": 4}]
    assert recommend_chart(["a", "b", "c"], rows)["recommended"] is False


def test_no_chart_when_zero_rows():
    assert recommend_chart(["a", "b"], [])["recommended"] is False


def test_no_chart_when_one_row():
    rows = [{"region": "West", "revenue": 100}]
    assert recommend_chart(["region", "revenue"], rows)["recommended"] is False


def test_no_chart_when_y_column_not_numeric():
    rows = _rows("region", "name", [("West", "Alice"), ("East", "Bob")])
    result = recommend_chart(["region", "name"], rows)
    assert result["recommended"] is False
    assert "numeric" in result["reason"].lower()


def test_no_chart_when_y_column_all_none():
    rows = _rows("region", "value", [("West", None), ("East", None)])
    result = recommend_chart(["region", "value"], rows)
    assert result["recommended"] is False


# ===========================================================================
# BAR CHART — categorical x, numeric y, 2-50 rows
# ===========================================================================

def test_bar_chart_basic():
    rows = _rows("region", "revenue", [("West", 100), ("East", 200)])
    result = recommend_chart(["region", "revenue"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "bar"
    assert result["x_column"] == "region"
    assert result["y_column"] == "revenue"


def test_bar_chart_at_50_rows():
    """Exactly 50 rows is the boundary — should still recommend bar."""
    pairs = [(f"Cat_{i}", i * 10) for i in range(50)]
    rows = _rows("category", "amount", pairs)
    result = recommend_chart(["category", "amount"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "bar"


def test_no_bar_chart_at_51_rows():
    """51 rows exceeds the bar chart limit."""
    pairs = [(f"Cat_{i}", i * 10) for i in range(51)]
    rows = _rows("category", "amount", pairs)
    result = recommend_chart(["category", "amount"], rows)
    assert result["recommended"] is False
    assert "too many" in result["reason"].lower()


def test_bar_chart_title_formatting():
    rows = _rows("drug_name", "total_paid", [("DrugA", 100), ("DrugB", 200)])
    result = recommend_chart(["drug_name", "total_paid"], rows)
    assert result["recommended"] is True
    assert result["title"]  # non-empty title
    assert "Drug Name" in result["title"] or "Total Paid" in result["title"]


def test_bar_chart_with_float_y_values():
    rows = _rows("region", "pct", [("West", 0.45), ("East", 0.55)])
    result = recommend_chart(["region", "pct"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "bar"


def test_bar_chart_with_negative_y_values():
    rows = _rows("region", "change", [("West", -10.5), ("East", 25.3)])
    result = recommend_chart(["region", "change"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "bar"


# ===========================================================================
# LINE CHART — datetime x, numeric y
# ===========================================================================

def test_line_chart_by_column_name_date():
    rows = _rows("date", "sales", [("2024-01-01", 100), ("2024-02-01", 200)])
    result = recommend_chart(["date", "sales"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_by_column_name_month():
    rows = _rows("month", "revenue", [("Jan", 100), ("Feb", 200)])
    result = recommend_chart(["month", "revenue"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_by_column_name_year():
    rows = _rows("year", "count", [(2020, 100), (2021, 200)])
    result = recommend_chart(["year", "count"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_by_column_name_quarter():
    rows = _rows("quarter", "revenue", [("Q1", 100), ("Q2", 200)])
    result = recommend_chart(["quarter", "revenue"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_by_column_name_timestamp():
    rows = _rows("timestamp", "events", [("2024-01-01T00:00", 10), ("2024-01-02T00:00", 20)])
    result = recommend_chart(["timestamp", "events"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_by_iso_date_values():
    """Column name is generic but values are ISO dates."""
    rows = _rows("period", "amount", [("2024-01-15", 100), ("2024-02-15", 200)])
    result = recommend_chart(["period", "amount"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_by_year_month_values():
    rows = _rows("period", "amount", [("2024-01", 100), ("2024-02", 200)])
    result = recommend_chart(["period", "amount"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_by_year_only_values():
    rows = _rows("period", "amount", [("2020", 100), ("2021", 200), ("2022", 300)])
    result = recommend_chart(["period", "amount"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_line_chart_allows_more_than_50_rows():
    """Line charts have no upper row limit (unlike bar charts)."""
    pairs = [(f"2020-01-{i+1:02d}", i * 10) for i in range(100)]
    rows = _rows("date", "value", pairs)
    result = recommend_chart(["date", "value"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


# ===========================================================================
# COLUMN TYPE INFERENCE — edge cases
# ===========================================================================

def test_numeric_detection_with_none_values():
    """None values in y column should not prevent numeric detection."""
    rows = [
        {"cat": "A", "val": 10},
        {"cat": "B", "val": None},
        {"cat": "C", "val": 30},
    ]
    result = recommend_chart(["cat", "val"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "bar"


def test_numeric_detection_with_string_numbers():
    """Numeric strings should still be detected as numeric."""
    rows = _rows("cat", "val", [("A", "100"), ("B", "200"), ("C", "300")])
    result = recommend_chart(["cat", "val"], rows)
    assert result["recommended"] is True


def test_categorical_detection_with_mixed_case():
    rows = _rows("Name", "Count", [("Alice", 10), ("Bob", 20)])
    result = recommend_chart(["Name", "Count"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "bar"


def test_date_pattern_q_format():
    """Q1 2024 format should be recognized as datetime."""
    rows = _rows("qtr", "val", [("Q1 2024", 100), ("Q2 2024", 200)])
    result = recommend_chart(["qtr", "val"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_date_pattern_month_name_year():
    """'January 2024' format should be recognized as datetime."""
    rows = _rows("period", "val", [("January 2024", 100), ("February 2024", 200)])
    result = recommend_chart(["period", "val"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_four_digit_ids_treated_as_dates():
    """4-digit IDs (1001, 1002) match the year pattern — recommender
    treats them as datetime and recommends a line chart. This documents the
    known behavior: the recommender cannot distinguish year-like numbers from IDs."""
    rows = _rows("id", "value", [(1001, 10), (1002, 20)])
    result = recommend_chart(["id", "value"], rows)
    assert result["recommended"] is True
    assert result["chart_type"] == "line"


def test_no_chart_for_large_numeric_ids():
    """5+ digit IDs don't match the year pattern — no chart."""
    rows = _rows("id", "value", [(10001, 10), (10002, 20)])
    result = recommend_chart(["id", "value"], rows)
    assert result["recommended"] is False


# ===========================================================================
# RESPONSE STRUCTURE
# ===========================================================================

def test_recommended_response_has_all_keys():
    rows = _rows("region", "revenue", [("West", 100), ("East", 200)])
    result = recommend_chart(["region", "revenue"], rows)
    for key in ("recommended", "chart_type", "x_column", "y_column", "title", "reason"):
        assert key in result, f"Missing key: {key}"


def test_no_chart_response_has_all_keys():
    result = recommend_chart([], [])
    for key in ("recommended", "chart_type", "x_column", "y_column", "title", "reason"):
        assert key in result, f"Missing key: {key}"


def test_no_chart_response_nulls():
    result = recommend_chart([], [])
    assert result["chart_type"] is None
    assert result["x_column"] is None
    assert result["y_column"] is None
    assert result["title"] is None
    assert isinstance(result["reason"], str)
