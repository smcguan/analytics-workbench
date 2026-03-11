from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from .context_builder import build_context


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


def build_sql_prompt(
    *,
    dataset_name: str,
    question: str,
    dataset_source_path_fn,
) -> str:
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
You are a SQL generation assistant for DuckDB.

Your job is to generate a single safe SQL query for the user's question.

Rules:
- Return ONLY valid JSON.
- Do not wrap the response in markdown or code fences.
- The SQL must be a single SELECT or WITH ... SELECT query.
- Do not use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, COPY, ATTACH, or DETACH.
- Use only the available dataset columns.
- The dataset should be queried as a table named dataset.
- If the request cannot be answered from the dataset, return a JSON response with status="error".
- Prefer clear, correct SQL over complex SQL.

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

User question:
{question}
""".strip()

    return prompt


def generate_sql_response(prompt: str) -> str:
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


def generate_sql_for_dataset(
    *,
    dataset_name: str,
    question: str,
    dataset_source_path_fn,
) -> str:
    prompt = build_sql_prompt(
        dataset_name=dataset_name,
        question=question,
        dataset_source_path_fn=dataset_source_path_fn,
    )
    return generate_sql_response(prompt)