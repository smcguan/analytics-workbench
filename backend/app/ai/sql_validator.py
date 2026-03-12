"""
============================================================
FILE: sql_validator.py
LOCATION: backend/app/ai/sql_validator.py
============================================================

PURPOSE
-------
This module validates AI-generated SQL before execution.

It performs two kinds of protection:

1. SQL safety validation
   Blocks dangerous SQL such as DROP, DELETE, UPDATE, etc.

2. DuckDB semantic validation
   Uses DuckDB EXPLAIN to verify that the SQL is valid
   for the actual dataset schema.

WHY THIS EXISTS
---------------
LLMs can generate SQL that looks correct but references:

- nonexistent columns
- nonexistent tables
- invalid syntax

This module catches those problems before query execution.

IMPORTANT ARCHITECTURAL RULE
----------------------------
The AI is expected to generate SQL against a logical table
named:

    dataset

Because the actual data lives in Parquet files, DuckDB does
not automatically know about a table called "dataset".

So during semantic validation, we create a temporary DuckDB
view named "dataset" that points at the Parquet file.

That lets DuckDB validate SQL like:

    SELECT * FROM dataset LIMIT 10

============================================================
"""

from __future__ import annotations

import re

import duckdb


# ------------------------------------------------------------
# HELPER — Escape file paths for safe SQL embedding
# ------------------------------------------------------------
def _sql_escape_path(p: str) -> str:
    return p.replace("'", "''")


# ------------------------------------------------------------
# SQL SAFETY VALIDATION
# ------------------------------------------------------------
# This blocks dangerous or non-read-only SQL.
# ------------------------------------------------------------
def validate_generated_sql(sql: str) -> tuple[bool, str]:
    """
    Validate that the SQL is read-only and safe to run.

    Allowed:
        SELECT
        WITH ... SELECT

    Blocked:
        INSERT
        UPDATE
        DELETE
        DROP
        ALTER
        CREATE
        COPY
        ATTACH
        DETACH
    """

    s = (sql or "").strip()
    if not s:
        return False, "Generated SQL is empty."

    lowered = s.lower()

    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "Only SELECT and WITH queries are allowed."

    blocked = [
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "create",
        "copy",
        "attach",
        "detach",
    ]

    for token in blocked:
        if re.search(rf"\b{token}\b", lowered):
            return False, f"Blocked SQL keyword: {token}"

    # Optional extra protection:
    # block multiple statements separated by semicolons
    stripped = s.rstrip(";").strip()
    if ";" in stripped:
        return False, "Multiple SQL statements are not allowed."

    return True, "SQL passed safety validation."


# ------------------------------------------------------------
# DUCKDB SEMANTIC VALIDATION
# ------------------------------------------------------------
# This checks whether the SQL is valid against the real
# dataset schema.
#
# IMPORTANT:
# We create a temporary DuckDB view named "dataset" so the AI
# can consistently generate SQL against that logical name.
# ------------------------------------------------------------
def validate_sql_with_duckdb(
    sql: str,
    dataset_name: str,
    dataset_source_path_fn,
) -> tuple[bool, str]:
    """
    Validate AI-generated SQL using DuckDB EXPLAIN.

    This catches:
    - hallucinated columns
    - invalid table references
    - syntax errors

    The function creates a temporary view named "dataset"
    backed by the Parquet file, then runs EXPLAIN on the SQL.
    """

    try:
        # ----------------------------------------------------
        # STEP 1 — Resolve dataset source path
        # ----------------------------------------------------
        src, _is_glob = dataset_source_path_fn(dataset_name)
        esc = _sql_escape_path(src)

        # ----------------------------------------------------
        # STEP 2 — Open DuckDB connection
        # ----------------------------------------------------
        con = duckdb.connect()

        try:
            # ------------------------------------------------
            # STEP 3 — Create logical view named "dataset"
            # ------------------------------------------------
            # This is the key fix.
            #
            # The AI prompt tells the model to write SQL using:
            #
            #     dataset
            #
            # So we create a temporary DuckDB view with that
            # exact name, backed by the Parquet file.
            #
            con.execute("DROP VIEW IF EXISTS dataset")
            con.execute(
                f"CREATE TEMP VIEW dataset AS "
                f"SELECT * FROM read_parquet('{esc}')"
            )

            # ------------------------------------------------
            # STEP 4 — Validate query using EXPLAIN
            # ------------------------------------------------
            # DuckDB will parse the SQL and verify that all
            # referenced columns and tables are valid.
            #
            con.execute(f"EXPLAIN {sql}")

            return True, "SQL passed DuckDB semantic validation."

        finally:
            con.close()

    except Exception as e:
        return False, f"Generated SQL failed DuckDB validation: {e}"