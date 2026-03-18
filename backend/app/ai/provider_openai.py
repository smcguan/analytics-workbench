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


# ============================================================
# EXPLAIN PROMPT BUILDER
# ------------------------------------------------------------
# Given a SQL query and its result rows, asks the model to
# explain in plain English what the query does and what the
# results mean in business terms.
#
# Returns plain text (not JSON) — 2–4 sentences.
# ============================================================
def build_explain_prompt(
    *,
    sql: str,
    columns: list[str],
    rows: list[dict],
    dataset_name: str,
) -> str:
    """
    Build the prompt used to explain a SQL query and its results
    in plain-English business terms.
    """
    sample = rows[:10]

    # Format rows as a simple readable table
    rows_text = "(no results)"
    if sample and columns:
        header = " | ".join(columns)
        separator = "-" * len(header)
        data_lines = []
        for row in sample:
            data_lines.append(" | ".join(str(row.get(c, "")) for c in columns))
        rows_text = header + "\n" + separator + "\n" + "\n".join(data_lines)

    prompt = f"""
You are a data analyst explaining a query result to a business user who is not technical.

Write 2 to 4 sentences. The first sentence should describe what the query does in plain English.
The remaining sentences should explain what the results mean in business terms — highlight
anything notable, such as which items are largest, smallest, or most significant.

Rules:
- Write for a business audience. No SQL syntax, no technical jargon.
- Be specific — reference actual values from the results when they are meaningful.
- Do NOT restate the column names or row count mechanically.
- Do NOT use bullet points or headers — plain flowing text only.
- Keep it concise: 2 to 4 sentences maximum.

Dataset: {dataset_name}

SQL query:
{sql}

Result columns: {", ".join(columns) if columns else "(none)"}

Result rows (up to 10):
{rows_text}
""".strip()

    return prompt


def generate_explanation(
    *,
    sql: str,
    columns: list[str],
    rows: list[dict],
    dataset_name: str,
) -> str:
    """
    Generate a plain-English explanation of a SQL query and its results.
    """
    prompt = build_explain_prompt(
        sql=sql,
        columns=columns,
        rows=rows,
        dataset_name=dataset_name,
    )
    return generate_sql_response(prompt)


# ============================================================
# INSIGHTS PROMPT BUILDER
# ------------------------------------------------------------
# Asks the model to surface 3–5 non-obvious findings based on
# the dataset's actual structure, stats, and sample values.
#
# Priority order mirrors CLAUDE.md:
#   concentration > outliers > trend > skew > missing > correlation
# ============================================================
def build_insights_prompt(
    *,
    dataset_name: str,
    dataset_source_path_fn,
    max_insights: int = 5,
) -> str:
    """
    Build the prompt used to generate AI insights for a dataset.
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
You are a data analyst identifying non-obvious insights in a dataset.

Your task is to produce up to {max_insights} high-value analytical insights that a
business user would find genuinely interesting — things they would NOT have thought
to ask about on their own.

CRITICAL RULES — FOLLOW EXACTLY:
- Return ONLY valid JSON. No markdown. No code fences. No explanation outside the JSON.
- Each insight must include a working DuckDB SQL query using the table name "dataset".
- Use ONLY the column names listed under "Available columns". Never invent column names.
- SQL must use DuckDB syntax. No semicolons at end of SQL.
- Do not qualify column names with a table prefix.
- Headlines must be plain English with specific numbers when possible (e.g. "Top 5 drugs account for 42% of spending").
- Do NOT restate the obvious (e.g. "This dataset has 734 rows").
- Do NOT generate more than {max_insights} insights.
- Write for business users — no technical jargon in headlines or explanations.

INSIGHT TYPES (use in this priority order — pick whichever apply):
1. concentration — a small number of items drives a disproportionate share of the total (Pareto)
2. outliers — values far outside the norm for their group
3. trend — fastest-growing or fastest-declining items (only if a date/time column exists)
4. skew — a numeric column is heavily skewed (mean >> median or vice versa)
5. missing — columns with high null rates, zero-heavy distributions, or formatting anomalies
6. correlation — two numeric columns that move together in an interesting way

DUCKDB SYNTAX REMINDERS:
- strftime('%Y-%m', col) — format string FIRST
- DATE_TRUNC('month', col) for date bucketing
- col::INTEGER for type casting
- ILIKE for case-insensitive string matching
- No semicolons at end of SQL

For chart_type: use "bar" when the SQL returns exactly 2 columns where one is
categorical and one is numeric (2–50 rows). Use "line" when one column is a
date/time and one is numeric. Use "" (empty string) otherwise.

Return JSON with exactly this structure:
{{
  "synopsis": "2-3 sentences describing what this dataset contains, what it measures, and who or what each row represents. Be specific — mention the domain, key columns, and scale (e.g. number of rows, time range, or geography if visible).",
  "insights": [
    {{
      "type": "concentration",
      "headline": "Top 5 drugs account for 42% of total spending",
      "explanation": "Medicare Part B spending is highly concentrated in a small number of drugs. The top 5 account for 42% of total spending despite representing less than 1% of all drugs billed.",
      "sql": "SELECT Brnd_Name, SUM(Tot_Spndng) AS total FROM dataset GROUP BY Brnd_Name ORDER BY total DESC LIMIT 10",
      "chart_type": "bar",
      "priority": 1
    }}
  ]
}}

Dataset name: {dataset_name}

Available columns (USE ONLY THESE):
{columns_text}

Sample rows (shows real data values and formats):
{sample_rows_text}

Numeric column stats (min/max/avg):
{numeric_stats_text}

Categorical column values:
{categorical_values_text}
""".strip()

    return prompt


# ============================================================
# INSIGHTS RESPONSE PARSER
# ------------------------------------------------------------
# Turns the model's JSON output into a safe list[dict].
# Skips malformed items so one bad insight doesn't crash the
# endpoint. Returns [] on any parse failure.
# ============================================================
def parse_insights_response(raw_text: str) -> dict:
    """
    Parse the model output for insights.

    Expected JSON:
    {
      "synopsis": "2-3 sentence dataset description.",
      "insights": [ { type, headline, explanation, sql, chart_type, priority }, ... ]
    }

    Returns:
        {"synopsis": str, "insights": list[dict]}
    """
    required_keys = {"type", "headline", "explanation", "sql"}

    try:
        cleaned = raw_text.strip()

        # Strip markdown code fences if present
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
            return {"synopsis": "", "insights": []}

        data = json.loads(cleaned[start:end + 1])

        synopsis = data.get("synopsis", "")
        if not isinstance(synopsis, str):
            synopsis = ""

        raw_insights = data.get("insights", [])
        if not isinstance(raw_insights, list):
            return {"synopsis": synopsis, "insights": []}

        result = []
        for item in raw_insights:
            if not isinstance(item, dict):
                continue
            # Skip items missing required fields
            if not required_keys.issubset(item.keys()):
                continue
            # Clamp priority to 1–5
            try:
                item["priority"] = max(1, min(5, int(item.get("priority", 1))))
            except (TypeError, ValueError):
                item["priority"] = 1
            # Ensure chart_type is a string
            if not isinstance(item.get("chart_type"), str):
                item["chart_type"] = ""
            result.append(item)

        return {"synopsis": synopsis, "insights": result}

    except Exception:
        return {"synopsis": "", "insights": []}


# ============================================================
# INSIGHTS WRAPPER
# ------------------------------------------------------------
# High-level helper used by the route:
#
#   dataset -> prompt -> OpenAI -> parsed insights
# ============================================================
def generate_insights_for_dataset(
    *,
    dataset_name: str,
    dataset_source_path_fn,
    max_insights: int = 5,
) -> dict:
    """
    Generate AI-powered insights for a dataset.

    Returns:
        {"synopsis": str, "insights": list[dict]}
    """
    prompt = build_insights_prompt(
        dataset_name=dataset_name,
        dataset_source_path_fn=dataset_source_path_fn,
        max_insights=max_insights,
    )

    raw_text = generate_sql_response(prompt)
    return parse_insights_response(raw_text)


# ============================================================
# GRAIN DESCRIPTION PROMPT BUILDER
# ------------------------------------------------------------
# Given a dataset schema and sample values, asks the model to
# write 1-2 sentences describing what one row represents and
# any aggregation guidance.
# ============================================================

def _build_grain_description_prompt(
    *,
    dataset_name: str,
    schema: list[dict],
) -> str:
    """
    Build the prompt used to generate a dataset grain description.

    The grain description answers: what does one row represent?
    """
    # Cap at 30 columns to keep the prompt concise
    lines = []
    for col in schema[:30]:
        samples = col.get("sample_values", [])
        sample_str = ", ".join(str(s) for s in samples[:3]) if samples else "N/A"
        lines.append(f"- {col['column_name']} ({col['data_type']}): e.g. {sample_str}")
    schema_text = "\n".join(lines)

    return f"""You are a data analyst writing concise documentation for a dataset.

Given the column names, types, and sample values below, write 1-2 sentences that describe:
1. What one row in this dataset represents (the unit of observation / grain)
2. Any important aggregation guidance (e.g. GROUP BY before summing, or "already aggregated by drug and year")

Rules:
- Return ONLY the plain text description. No JSON. No markdown. No bullet points.
- Maximum 2 sentences.
- Be specific — use actual column names if helpful.
- Focus on what ONE ROW represents, not the entire dataset.

Dataset name: {dataset_name}

Columns and sample values:
{schema_text}

Grain description:""".strip()


# ============================================================
# GRAIN DESCRIPTION WRAPPER
# ============================================================

def generate_grain_description_for_dataset(
    *,
    dataset_name: str,
    schema: list[dict],
) -> str:
    """
    Generate a 1-2 sentence grain description for a dataset.

    Called by the Export Passport endpoint in main.py.
    The result is cached in dataset_context.json so only one
    OpenAI call is needed per dataset.
    """
    import re as _re

    prompt = _build_grain_description_prompt(
        dataset_name=dataset_name,
        schema=schema,
    )
    raw = generate_sql_response(prompt)
    # Strip stray quotes or markdown the model might have emitted
    result = raw.strip().strip('"').strip("'")
    # Collapse multiple newlines to a single space
    result = _re.sub(r"\n+", " ", result).strip()
    return result