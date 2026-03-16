from __future__ import annotations

"""
============================================================
FILE: provider_openai.py
LOCATION: backend/app/ai/provider_openai.py
============================================================

PURPOSE
-------
This module is the OpenAI provider layer for AI-assisted SQL
generation and AI-generated suggested questions.

It now supports two AI tasks:

1. SQL generation
2. Dataset-aware question suggestion

============================================================
"""

import json
import os
import sys
from pathlib import Path
from typing import Any

from .context_builder import build_context


# ============================================================
# API KEY LOADING
# ============================================================
def _read_key_from_env_file() -> str | None:
    candidates = []

    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).resolve().parent / ".env")

    candidates.append(Path.cwd() / ".env")

    for path in candidates:
        try:
            if not path.exists():
                continue

            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("OPENAI_API_KEY="):
                    return line.split("=", 1)[1].strip()
        except Exception:
            pass

    return None


# ============================================================
# FORMAT HELPERS
# ============================================================
def _format_columns(columns: list[dict[str, Any]]) -> str:
    if not columns:
        return "(none)"

    lines = []
    for col in columns:
        lines.append(f'- {col["name"]} ({col["type"]})')
    return "\n".join(lines)


def _format_sample_rows(sample_rows: list[dict[str, Any]]) -> str:
    if not sample_rows:
        return "(none)"

    lines = []
    for i, row in enumerate(sample_rows, start=1):
        parts = [f"{k}={repr(v)}" for k, v in row.items()]
        lines.append(f"{i}. " + ", ".join(parts))
    return "\n".join(lines)


def _format_numeric_stats(numeric_stats: list[dict[str, Any]]) -> str:
    if not numeric_stats:
        return "(none)"

    lines = []
    for stat in numeric_stats:
        lines.append(
            f'- {stat["column"]}: min={stat["min"]}, max={stat["max"]}, avg={stat["avg"]}'
        )
    return "\n".join(lines)


def _format_categorical_values(categorical_values: list[dict[str, Any]]) -> str:
    if not categorical_values:
        return "(none)"

    lines = []
    for item in categorical_values:
        values = ", ".join(repr(v) for v in item["values"])
        lines.append(f'- {item["column"]}: {values}')
    return "\n".join(lines)


# ============================================================
# SQL PROMPT BUILDER
# ============================================================
def build_sql_prompt(
    *,
    dataset_name: str,
    question: str,
    dataset_source_path_fn,
) -> str:
    """
    Build the schema-aware prompt used for SQL generation.
    """
    context = build_context(
        dataset_name=dataset_name,
        dataset_source_path_fn=dataset_source_path_fn,
    )

    columns_text = _format_columns(context.get("columns", []))
    sample_rows_text = _format_sample_rows(context.get("sample_rows", []))
    numeric_stats_text = _format_numeric_stats(context.get("numeric_stats", []))
    categorical_values_text = _format_categorical_values(
        context.get("categorical_values", [])
    )

    prompt = f"""
You are a SQL generation assistant. You write SQL exclusively for DuckDB.

Your job is to generate a single, correct, runnable DuckDB SQL query that answers the user's question.

CRITICAL RULES — FOLLOW EXACTLY:
- Return ONLY valid JSON. No markdown. No code fences. No explanation outside the JSON.
- The SQL must be a single SELECT or WITH ... SELECT statement.
- Do not use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, COPY, ATTACH, or DETACH.
- Use ONLY the column names listed under "Available columns". Never invent column names.
- Always reference the table as: dataset
- Do not qualify column names with a table prefix (write "revenue", not "dataset.revenue").
- End the query without a semicolon.
- If the request cannot be answered from the available columns, return status="error".

DUCKDB SYNTAX — CRITICAL DIFFERENCES FROM MYSQL/SQLITE:
Date and time functions (DuckDB argument order is OPPOSITE to SQLite):
  - CORRECT:   strftime('%Y-%m', order_date)       -- format string FIRST
  - WRONG:     STRFTIME(order_date, '%Y-%m')        -- this is SQLite, will fail in DuckDB
  - CORRECT:   DATE_TRUNC('month', order_date)      -- truncate to month
  - CORRECT:   EXTRACT(year FROM order_date)         -- extract year
  - CORRECT:   order_date::DATE                      -- cast to date type

String functions:
  - Use || for string concatenation (not CONCAT in simple cases)
  - LIKE is case-sensitive by default; use ILIKE for case-insensitive

Aggregation and window functions:
  - QUALIFY clause is supported for filtering window functions
  - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col) for median

Type casting:
  - Use col::INTEGER, col::FLOAT, col::VARCHAR (DuckDB cast syntax)
  - Or CAST(col AS INTEGER) — both work

NULL handling:
  - COALESCE(col, 0) to replace nulls
  - IS NULL / IS NOT NULL

QUERY CONSTRUCTION GUIDELINES:
- For "top N by X": use ORDER BY X DESC LIMIT N
- For grouping/aggregation: always include GROUP BY for non-aggregated columns
- For date trends: use DATE_TRUNC or strftime to bucket dates
- For averages: use AVG(col), for totals use SUM(col), for counts use COUNT(*)
- For comparisons across categories: GROUP BY the category column
- Always add ORDER BY to make results meaningful when possible
- Default LIMIT to 100 unless the question specifies a number or asks for ALL rows

Return JSON with exactly this structure:
{{
  "status": "ok",
  "sql": "SELECT ...",
  "message": "One sentence explaining what the query does.",
  "warnings": []
}}

If the question cannot be answered from the available columns:
{{
  "status": "error",
  "sql": "",
  "message": "Explain specifically which column or data is missing.",
  "warnings": []
}}

Dataset name: {dataset_name}

Available columns (USE ONLY THESE — do not invent new column names):
{columns_text}

Sample rows (shows real data values and formats):
{sample_rows_text}

Numeric column stats (min/max/avg for context):
{numeric_stats_text}

Categorical column values (actual values in the data):
{categorical_values_text}

User question: {question}
""".strip()

    return prompt


# ============================================================
# SUGGESTED QUESTIONS PROMPT BUILDER
# ------------------------------------------------------------
# This prompt asks the model to propose a small set of useful,
# grounded analytical questions based on the dataset itself.
#
# IMPORTANT:
# We are not asking the model to invent random questions.
# We want realistic, useful, schema-grounded suggestions.
# ============================================================
def build_suggest_questions_prompt(
    *,
    dataset_name: str,
    dataset_source_path_fn,
    max_questions: int = 8,
) -> str:
    """
    Build the prompt used to generate suggested questions for
    a selected dataset.
    """
    context = build_context(
        dataset_name=dataset_name,
        dataset_source_path_fn=dataset_source_path_fn,
    )

    columns_text = _format_columns(context.get("columns", []))
    sample_rows_text = _format_sample_rows(context.get("sample_rows", []))
    numeric_stats_text = _format_numeric_stats(context.get("numeric_stats", []))
    categorical_values_text = _format_categorical_values(
        context.get("categorical_values", [])
    )

    prompt = f"""
You are helping users explore a dataset in an analytics tool.

Your task is to suggest {max_questions} useful, realistic, grounded analytical questions
that a user could ask about this dataset.

Rules:
- Return ONLY valid JSON.
- Do not wrap the response in markdown or code fences.
- The questions must be grounded in the actual schema and values shown below.
- Prefer business-useful questions such as:
  - top categories by volume
  - totals by month
  - averages
  - rankings
  - trends
  - provider / code / region / category breakdowns
- Avoid vague or generic questions.
- Avoid asking about columns that do not exist.
- Keep each question concise and clickable in a UI.

Return JSON with exactly this structure:
{{
  "questions": [
    "question 1",
    "question 2",
    "question 3"
  ]
}}

Dataset name:
{dataset_name}

Available columns:
{columns_text}

Sample rows:
{sample_rows_text}

Numeric column stats:
{numeric_stats_text}

Categorical values:
{categorical_values_text}
""".strip()

    return prompt


# ============================================================
# OPENAI RAW CALL
# ============================================================
def generate_sql_response(prompt: str) -> str:
    """
    Send a prompt to OpenAI and return the raw output text.
    """
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY") or _read_key_from_env_file()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable not set")

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model=model,
        input=prompt,
    )

    return response.output_text.strip()


# ============================================================
# SQL GENERATION WRAPPER
# ============================================================
def generate_sql_for_dataset(
    *,
    dataset_name: str,
    question: str,
    dataset_source_path_fn,
) -> str:
    """
    High-level helper for question -> raw SQL response text.
    """
    prompt = build_sql_prompt(
        dataset_name=dataset_name,
        question=question,
        dataset_source_path_fn=dataset_source_path_fn,
    )
    return generate_sql_response(prompt)


# ============================================================
# SUGGESTED QUESTIONS PARSER
# ------------------------------------------------------------
# This turns the model's JSON output into a safe list[str].
# If the model response is malformed, we return an empty list
# instead of crashing the app.
# ============================================================
def parse_suggested_questions(raw_text: str) -> list[str]:
    """
    Parse the model output for suggested questions.

    Expected JSON:
    {
      "questions": ["...", "..."]
    }
    """
    try:
        cleaned = raw_text.strip()

        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()

        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start == -1 or end == -1 or end < start:
            return []

        data = json.loads(cleaned[start:end + 1])
        questions = data.get("questions", [])

        if not isinstance(questions, list):
            return []

        normalized = []
        seen = set()

        for q in questions:
            s = str(q).strip()
            if not s:
                continue
            if s in seen:
                continue
            seen.add(s)
            normalized.append(s)

        return normalized

    except Exception:
        return []


# ============================================================
# SUGGESTED QUESTIONS WRAPPER
# ------------------------------------------------------------
# High-level helper used by the route:
#
#   dataset -> prompt -> OpenAI -> parsed questions
# ============================================================
def suggest_questions_for_dataset(
    *,
    dataset_name: str,
    dataset_source_path_fn,
    max_questions: int = 8,
) -> list[str]:
    """
    Generate a list of dataset-aware suggested questions.
    """
    prompt = build_suggest_questions_prompt(
        dataset_name=dataset_name,
        dataset_source_path_fn=dataset_source_path_fn,
        max_questions=max_questions,
    )

    raw_text = generate_sql_response(prompt)
    return parse_suggested_questions(raw_text)