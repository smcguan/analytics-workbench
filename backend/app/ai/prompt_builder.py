"""
============================================================
FILE: prompt_builder.py
LOCATION: backend/app/ai/prompt_builder.py
============================================================

PURPOSE
-------
This module builds the prompt sent to the LLM for natural
language to SQL generation.

Its main job is to give the model:

1. Clear SQL generation instructions
2. The dataset schema
3. Sample rows and context
4. Strict rules that reduce hallucinations

VERY IMPORTANT ARCHITECTURAL RULE
---------------------------------
The AI must ALWAYS generate SQL against a logical table named:

    dataset

It must NEVER use the actual dataset name, such as:

    demo
    sales
    customers

Why?

Because the real data lives in Parquet files, not in physical
DuckDB tables with those names.

Later in the pipeline, the backend maps the logical table
name `dataset` to the real Parquet source.

If the LLM generates:

    SELECT * FROM demo

validation and execution will fail.

If the LLM generates:

    SELECT * FROM dataset

the system works correctly.

============================================================
"""

from __future__ import annotations

import json
from typing import Any


# ------------------------------------------------------------
# HELPER — Format schema columns into readable text
# ------------------------------------------------------------
def _format_columns(columns: list[dict[str, Any]]) -> str:
    """
    Format column metadata with semantic hints to help
    the LLM choose correct SQL operations.
    """

    if not columns:
        return "(no columns available)"

    lines = []

    for col in columns:

        name = col.get("name", "unknown")
        col_type = col.get("type", "").lower()

        # classify the column for the AI
        if any(x in col_type for x in ["int", "double", "float", "decimal"]):
            hint = "numeric measure"

        elif any(x in col_type for x in ["date", "time"]):
            hint = "date"

        elif "char" in col_type or "string" in col_type:
            hint = "categorical"

        else:
            hint = col_type

        lines.append(f"- {name} ({hint})")

    return "\n".join(lines)
# ------------------------------------------------------------
# HELPER — Format sample rows for prompt grounding
# ------------------------------------------------------------
def _format_sample_rows(sample_rows: list[dict[str, Any]]) -> str:
    """
    Convert sample rows into compact JSON lines so the model
    can infer value patterns and realistic data content.
    """
    if not sample_rows:
        return "(no sample rows available)"

    lines = []
    for i, row in enumerate(sample_rows, start=1):
        lines.append(f"{i}. {json.dumps(row, default=str)}")
    return "\n".join(lines)


# ------------------------------------------------------------
# HELPER — Format numeric stats if present
# ------------------------------------------------------------
def _format_numeric_stats(numeric_stats: list[dict[str, Any]]) -> str:
    """
    Format numeric column statistics to help the model reason
    about aggregations, ranges, and averages.
    """
    if not numeric_stats:
        return "(no numeric stats available)"

    lines = []
    for item in numeric_stats:
        lines.append(
            f"- {item.get('column')}: "
            f"min={item.get('min')}, "
            f"max={item.get('max')}, "
            f"avg={item.get('avg')}"
        )
    return "\n".join(lines)


# ------------------------------------------------------------
# HELPER — Format categorical values if present
# ------------------------------------------------------------
def _format_categorical_values(categorical_values: list[dict[str, Any]]) -> str:
    """
    Format low-cardinality categorical values so the model
    sees real filter/grouping values from the dataset.
    """
    if not categorical_values:
        return "(no categorical values available)"

    lines = []
    for item in categorical_values:
        values = item.get("values", [])
        values_text = ", ".join(str(v) for v in values)
        lines.append(f"- {item.get('column')}: {values_text}")
    return "\n".join(lines)


# ============================================================
# MAIN PROMPT BUILDER
# ============================================================
def build_generate_sql_prompt(
    context: dict[str, Any],
    question: str,
) -> str:
    """
    Build the prompt used for LLM-based SQL generation.

    This prompt is intentionally strict because we want the
    model to generate SQL that matches the architecture of the
    backend exactly.
    """

    dataset_name = context.get("dataset_name", "unknown")
    columns_text = _format_columns(context.get("columns", []))
    sample_rows_text = _format_sample_rows(context.get("sample_rows", []))
    numeric_stats_text = _format_numeric_stats(context.get("numeric_stats", []))
    categorical_values_text = _format_categorical_values(
        context.get("categorical_values", [])
    )

    prompt = f"""
You are a SQL generation assistant for DuckDB.

Your task is to generate a SINGLE read-only SQL query that answers the user's question.

============================================================
CRITICAL RULES
============================================================

1. The data must ALWAYS be queried using the table name:

   dataset

2. NEVER use the actual dataset name as a SQL table name.

   The dataset name for this request is:

   {dataset_name}

   But this is ONLY an identifier for the request.
   It is NOT a SQL table name.

3. Correct example:
   SELECT * FROM dataset LIMIT 10

4. Incorrect example:
   SELECT * FROM {dataset_name} LIMIT 10

5. Only use columns that actually exist in the schema below.

6. Only generate read-only SQL.
   Allowed:
   - SELECT
   - WITH ... SELECT

   Forbidden:
   - INSERT
   - UPDATE
   - DELETE
   - DROP
   - ALTER
   - CREATE
   - COPY
   - ATTACH
   - DETACH

7. Return ONLY valid JSON.
   Do not wrap the response in markdown or code fences.

8. If the question cannot be answered from the schema, return:
   - status = "error"
   - sql = ""
   - message explaining why
   - warnings = []

============================================================
RETURN FORMAT
============================================================

Return JSON with exactly this structure:

{{
  "status": "ok",
  "sql": "SELECT ...",
  "message": "Brief explanation of what the query does.",
  "warnings": []
}}

If there is an error, return:

{{
  "status": "error",
  "sql": "",
  "message": "Explain why the request cannot be answered.",
  "warnings": []
}}

============================================================
AVAILABLE SCHEMA
============================================================

Dataset request name:
{dataset_name}

SQL table name to use:
dataset

Columns:
{columns_text}

Sample rows:
{sample_rows_text}

Numeric column stats:
{numeric_stats_text}

Categorical values:
{categorical_values_text}

============================================================
USER QUESTION
============================================================

{question}
""".strip()

    return prompt