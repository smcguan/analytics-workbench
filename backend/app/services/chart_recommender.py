"""
============================================================
FILE: chart_recommender.py
LOCATION: backend/app/services/chart_recommender.py
============================================================

PURPOSE
-------
Inspect a SQL result set and recommend a simple chart type.

This module is intentionally deterministic — no AI is used.
Chart selection follows simple, predictable rules based on
result shape, column count, and inferred column types.

SUPPORTED CHART TYPES
---------------------
bar   — categorical x-axis, numeric y-axis
line  — date/time x-axis, numeric y-axis

RECOMMENDATION RULES
--------------------
Recommend bar chart when:
  - exactly 2 columns
  - first column is categorical (text/string-like)
  - second column is numeric
  - 2 to 50 rows (readable as a bar chart)

Recommend line chart when:
  - exactly 2 columns
  - first column is date/time-like (name or values suggest dates)
  - second column is numeric
  - 2 or more rows

Recommend nothing when:
  - fewer than 2 columns
  - more than 2 columns (too complex for simple auto-chart)
  - no numeric second column
  - only 1 row (nothing to compare)
  - result looks like a raw detail dump

DESIGN PRINCIPLE
----------------
Keep this module simple and side-effect free.
It takes columns and rows, returns a dict.
No FastAPI, no DuckDB, no I/O.
============================================================
"""

from __future__ import annotations

import re
from typing import Any


# ============================================================
# PUBLIC ENTRYPOINT
# ============================================================

def recommend_chart(
    columns: list[str],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Inspect a SQL result and return a chart recommendation.

    Parameters
    ----------
    columns : list[str]
        Column names in result order.
    rows : list[dict]
        Result rows as dicts keyed by column name.

    Returns
    -------
    dict with keys:
        recommended  bool
        chart_type   "bar" | "line" | None
        x_column     str | None
        y_column     str | None
        title        str | None
        reason       str
    """

    # --- Guard: need at least 2 columns and 2 rows to chart anything ---
    if not columns or len(columns) != 2:
        return _no_chart("Result must have exactly 2 columns for auto-charting.")

    if not rows or len(rows) < 2:
        return _no_chart("Not enough rows to chart (need at least 2).")

    # --- Guard: cap bar chart rows at 50 for readability ---
    x_col = columns[0]
    y_col = columns[1]

    # --- Check second column is numeric ---
    if not _col_is_numeric(y_col, rows):
        return _no_chart(f"Second column '{y_col}' does not appear to be numeric.")

    # --- Classify first column ---
    if _col_is_datetime(x_col, rows):
        return _recommend("line", x_col, y_col,
                          f"{_title_case(y_col)} over {_title_case(x_col)}",
                          "date/time x-axis with numeric measure")

    if _col_is_categorical(x_col, rows):
        if len(rows) > 50:
            return _no_chart(f"Too many rows ({len(rows)}) for a readable bar chart (max 50).")
        return _recommend("bar", x_col, y_col,
                          f"{_title_case(y_col)} by {_title_case(x_col)}",
                          "categorical x-axis with numeric measure")

    return _no_chart(f"First column '{x_col}' is not clearly categorical or date-like.")


# ============================================================
# COLUMN TYPE INFERENCE
# ============================================================

# Date-like column name patterns
_DATE_NAME_PATTERNS = re.compile(
    r"\b(date|month|year|week|day|quarter|period|time|timestamp|dt)\b",
    re.IGNORECASE,
)

# Date-like value patterns (ISO dates, year-month, year)
_DATE_VALUE_PATTERNS = re.compile(
    r"^\d{4}[-/]\d{2}([-/]\d{2})?$"   # 2024-01 or 2024-01-15
    r"|^\d{4}$"                         # 2024
    r"|^Q[1-4]\s+\d{4}$"               # Q1 2024
    r"|^\w{3,9}\s+\d{4}$",             # January 2024
    re.IGNORECASE,
)


def _col_is_datetime(col_name: str, rows: list[dict]) -> bool:
    """Return True if the column looks like a date or time dimension."""
    # Check column name first — fastest signal
    if _DATE_NAME_PATTERNS.search(col_name):
        return True

    # Check sample values
    sample = [str(r.get(col_name, "") or "").strip() for r in rows[:10]]
    matches = sum(1 for v in sample if v and _DATE_VALUE_PATTERNS.match(v))
    return matches >= max(1, len(sample) // 2)


def _col_is_numeric(col_name: str, rows: list[dict]) -> bool:
    """Return True if the column values are numeric."""
    sample = [r.get(col_name) for r in rows[:20]]
    non_null = [v for v in sample if v is not None]
    if not non_null:
        return False

    numeric_count = 0
    for v in non_null:
        try:
            float(v)
            numeric_count += 1
        except (TypeError, ValueError):
            pass

    return numeric_count >= max(1, len(non_null) // 2)


def _col_is_categorical(col_name: str, rows: list[dict]) -> bool:
    """
    Return True if the column looks like a categorical dimension.

    A column is categorical if:
    - values are strings (not parseable as float)
    - OR values are integers that look like labels (small range, few unique values)
    """
    sample = [r.get(col_name) for r in rows[:20]]
    non_null = [v for v in sample if v is not None]
    if not non_null:
        return False

    # If most values are non-numeric strings, it's categorical
    non_numeric = 0
    for v in non_null:
        try:
            float(str(v))
        except (TypeError, ValueError):
            non_numeric += 1

    if non_numeric >= max(1, len(non_null) // 2):
        return True

    # If values are integers but look like labels (e.g. year numbers, IDs)
    # treat as categorical if there are fewer unique values than rows
    unique_vals = set(str(v) for v in non_null)
    if len(unique_vals) < len(rows) * 0.9:
        return True

    return False


# ============================================================
# TITLE HELPERS
# ============================================================

def _title_case(col_name: str) -> str:
    """Convert a snake_case or camelCase column name to readable Title Case."""
    # Split on underscores and camelCase boundaries
    words = re.sub(r"([a-z])([A-Z])", r"\1 \2", col_name)
    words = re.sub(r"[_\-]+", " ", words)
    return words.strip().title()


# ============================================================
# RESPONSE BUILDERS
# ============================================================

def _recommend(
    chart_type: str,
    x_col: str,
    y_col: str,
    title: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "recommended": True,
        "chart_type": chart_type,
        "x_column": x_col,
        "y_column": y_col,
        "title": title,
        "reason": reason,
    }


def _no_chart(reason: str) -> dict[str, Any]:
    return {
        "recommended": False,
        "chart_type": None,
        "x_column": None,
        "y_column": None,
        "title": None,
        "reason": reason,
    }
