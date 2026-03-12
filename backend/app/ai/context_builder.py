from __future__ import annotations

"""
============================================================
FILE: context_builder.py
LOCATION: backend/app/ai/context_builder.py
============================================================

PURPOSE
-------
This module builds the dataset context used by the AI SQL
generation system.

Its job is to inspect the selected dataset and return a
compact summary that helps the language model generate
better SQL.

WHY THIS FILE EXISTS
--------------------
If the model only sees the user's question, it has to guess:

- what columns exist
- which columns are numeric
- which columns are categorical
- what kinds of values appear in the dataset

That leads to bad SQL such as:
- hallucinated column names
- invalid filters
- poor grouping choices
- weak aggregations

This file reduces those problems by giving the model:

1. schema information
2. sample rows
3. numeric statistics
4. low-cardinality categorical values

AI PIPELINE POSITION
--------------------

User question
      ↓
build_context(...)              <-- this file
      ↓
prompt builder / provider
      ↓
OpenAI model
      ↓
raw SQL response
      ↓
parser / validator / execution

IMPORTANT DESIGN RULE
---------------------
The model should think of the dataset as a logical SQL table
named:

    dataset

Even though the real data lives in Parquet files.

This file supports that design by returning metadata that
describes the dataset without exposing it as a real SQL table.

============================================================
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import duckdb
from fastapi import HTTPException


# ============================================================
# HELPER — SQL PATH ESCAPING
# ------------------------------------------------------------
# DuckDB SQL statements embed file paths inside single quotes.
#
# If a path contains a single quote character, it must be
# escaped correctly or the SQL string will break.
# ============================================================
def _sql_escape_path(p: str) -> str:
    """
    Escape single quotes for safe SQL embedding.
    """
    return p.replace("'", "''")


# ============================================================
# HELPER — TYPE DETECTION: NUMERIC
# ------------------------------------------------------------
# The AI needs to know which columns are good candidates for:
# - SUM
# - AVG
# - MIN / MAX
# - ORDER BY numeric value
#
# This helper classifies column types that should be treated
# as numeric.
# ============================================================
def _is_numeric_type(type_name: str) -> bool:
    """
    Return True if the DuckDB type looks numeric.
    """
    t = type_name.upper()
    return any(
        x in t
        for x in [
            "INT",
            "DOUBLE",
            "FLOAT",
            "DECIMAL",
            "REAL",
            "HUGEINT",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
        ]
    )


# ============================================================
# HELPER — TYPE DETECTION: TEXT
# ------------------------------------------------------------
# Text columns are important because some of them are
# low-cardinality categorical fields, which are useful for:
# - filters
# - GROUP BY
# - showing common real values to the AI
# ============================================================
def _is_text_type(type_name: str) -> bool:
    """
    Return True if the DuckDB type looks text-like.
    """
    t = type_name.upper()
    return any(x in t for x in ["CHAR", "VARCHAR", "STRING", "TEXT"])


# ============================================================
# HELPER — JSON-SAFE VALUE NORMALIZATION
# ------------------------------------------------------------
# Some DuckDB / Python values are not directly JSON friendly.
#
# Examples:
# - datetime objects
# - date objects
# - Decimal values
#
# We normalize them so they can safely be included in the
# context returned to the AI layer.
# ============================================================
def _json_safe_value(value: Any) -> Any:
    """
    Convert values into JSON-safe representations.

    Rules:
    - datetime/date -> ISO string
    - Decimal       -> float
    - everything else unchanged
    """
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    return value


# ============================================================
# MAIN CONTEXT BUILDER
# ------------------------------------------------------------
# This is the most important function in the file.
#
# It does four main things:
#
# 1. Resolve the dataset file path
# 2. Read schema information
# 3. Read sample rows
# 4. Compute helpful context features:
#    - numeric stats
#    - categorical values
#
# The returned structure is then used by the AI prompt layer.
# ============================================================
def build_context(
    dataset_name: str,
    dataset_source_path_fn,
    max_sample_rows: int = 5,
    max_categorical_values: int = 10,
) -> dict[str, Any]:
    """
    Build a compact schema-aware context for the AI SQL generator.

    Returned context includes:
    - dataset name
    - logical table name
    - physical source path
    - schema columns
    - sample rows
    - numeric stats
    - low-cardinality categorical values

    Why this matters:
    The more grounded the model is in the real dataset, the
    more accurate and reliable the generated SQL becomes.
    """

    # --------------------------------------------------------
    # STEP 1 — Resolve the dataset source path
    # --------------------------------------------------------
    # This asks the caller's resolver function where the
    # selected dataset actually lives on disk.
    #
    # Example:
    #   "demo" -> "C:/.../demo.parquet"
    #
    try:
        src, _is_glob = dataset_source_path_fn(dataset_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception:
        raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset_name}")

    # Escape the path before embedding it in SQL
    esc = _sql_escape_path(src)

    # Open a local DuckDB connection for inspection
    con = duckdb.connect()

    try:
        # ----------------------------------------------------
        # STEP 2 — Read the schema
        # ----------------------------------------------------
        # DuckDB DESCRIBE gives us the available columns and
        # their data types.
        #
        schema_cur = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{esc}')"
        )

        schema_rows = schema_cur.fetchall()

        columns: list[dict[str, str]] = []

        for row in schema_rows:
            columns.append(
                {
                    "name": str(row[0]),
                    "type": str(row[1]),
                }
            )

        # ----------------------------------------------------
        # STEP 3 — Read sample rows
        # ----------------------------------------------------
        # Sample rows are very helpful for the model because
        # they show realistic value patterns, not just names.
        #
        sample_cur = con.execute(
            f"SELECT * FROM read_parquet('{esc}') LIMIT {int(max_sample_rows)}"
        )

        sample_cols = [d[0] for d in sample_cur.description]
        sample_rows_raw = sample_cur.fetchall()

        sample_rows = [
            {k: _json_safe_value(v) for k, v in zip(sample_cols, row)}
            for row in sample_rows_raw
        ]

        # ----------------------------------------------------
        # STEP 4 — Compute numeric stats
        # ----------------------------------------------------
        # For numeric columns, the model benefits from seeing:
        # - min
        # - max
        # - avg
        #
        # That helps with:
        # - aggregation reasoning
        # - understanding scale
        # - picking likely measure columns
        #
        numeric_stats: list[dict[str, Any]] = []

        for col in columns:
            col_name = col["name"]
            col_type = col["type"]

            if not _is_numeric_type(col_type):
                continue

            try:
                stats = con.execute(
                    f'''
                    SELECT
                        MIN("{col_name}"),
                        MAX("{col_name}"),
                        AVG("{col_name}")
                    FROM read_parquet('{esc}')
                    '''
                ).fetchone()

                numeric_stats.append(
                    {
                        "column": col_name,
                        "min": _json_safe_value(stats[0]),
                        "max": _json_safe_value(stats[1]),
                        "avg": _json_safe_value(stats[2]),
                    }
                )

            except Exception:
                # We do not want one problematic column to
                # break the entire context build.
                continue

        # ----------------------------------------------------
        # STEP 5 — Collect categorical values
        # ----------------------------------------------------
        # For text columns, we try to collect a small set of
        # distinct values when cardinality is low.
        #
        # This is extremely helpful for:
        # - filters
        # - GROUP BY
        # - avoiding hallucinated category names
        #
        # Example:
        #   region -> ["East", "West", "South"]
        #
        categorical_values: list[dict[str, Any]] = []

        for col in columns:
            col_name = col["name"]
            col_type = col["type"]

            if not _is_text_type(col_type):
                continue

            try:
                distinct_rows = con.execute(
                    f'''
                    SELECT DISTINCT "{col_name}"
                    FROM read_parquet('{esc}')
                    WHERE "{col_name}" IS NOT NULL
                    LIMIT {int(max_categorical_values) + 1}
                    '''
                ).fetchall()

                values = [
                    str(row[0]) for row in distinct_rows
                    if row and row[0] is not None
                ]

                # Only include low-cardinality fields.
                # If there are too many values, it is probably
                # not useful to dump them into the prompt.
                if 0 < len(values) <= int(max_categorical_values):
                    categorical_values.append(
                        {
                            "column": col_name,
                            "values": values,
                        }
                    )

            except Exception:
                # Again, we keep the overall build resilient.
                continue

        # ----------------------------------------------------
        # STEP 6 — Return the final AI context object
        # ----------------------------------------------------
        # table_name is intentionally fixed as "dataset"
        # because the AI prompt and execution system are built
        # around that logical table name.
        #
        return {
            "dataset_name": dataset_name,
            "table_name": "dataset",
            "source_path": src,
            "columns": columns,
            "sample_rows": sample_rows,
            "numeric_stats": numeric_stats,
            "categorical_values": categorical_values,
        }

    except Exception as e:
        # If anything major fails during inspection, return a
        # clean API-facing error rather than a raw exception.
        raise HTTPException(
            status_code=400,
            detail=f"Failed to inspect dataset '{dataset_name}': {e}",
        )

    finally:
        # Always close the DuckDB connection
        con.close()