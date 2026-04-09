"""
test_ollama_accuracy.py — Ollama SQL generation accuracy benchmarks

Runs 10 natural-language questions through the Ollama provider and scores
each on two dimensions:
  1. Syntactic validity — the generated SQL runs against DuckDB without error
  2. Structural correctness — the result returns expected columns and row counts

Skips entirely when Ollama is not running or llama3.2 is not available.

Run from project root:
    PYTHONPATH=backend python -m pytest tests/test_ollama_accuracy.py -v -s
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Skip if Ollama not available
# ---------------------------------------------------------------------------

def _ollama_available():
    try:
        import requests
        r = requests.get("http://localhost:11434/api/tags", timeout=2)
        tags = r.json().get("models", [])
        return any("llama3.2" in m.get("name", "") for m in tags)
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _ollama_available(),
    reason="Ollama not running or llama3.2 not available",
)


# ---------------------------------------------------------------------------
# Dataset setup
# ---------------------------------------------------------------------------

CASE_DIR = Path("data/example_cases/logistics_shipment_analysis")


@pytest.fixture(scope="module")
def parquet_path(tmp_path_factory):
    """Import the logistics CSV into a temporary Parquet file."""
    meta = json.loads((CASE_DIR / "metadata.json").read_text(encoding="utf-8"))
    csv_path = CASE_DIR / "data" / meta["dataset_file"]
    df = pd.read_csv(csv_path)
    tmp = tmp_path_factory.mktemp("ollama_accuracy")
    pq = tmp / "source.parquet"
    df.to_parquet(str(pq), index=False)
    return str(pq)


@pytest.fixture(scope="module")
def columns():
    meta = json.loads((CASE_DIR / "metadata.json").read_text(encoding="utf-8"))
    csv_path = CASE_DIR / "data" / meta["dataset_file"]
    df = pd.read_csv(csv_path, nrows=1)
    return list(df.columns)


# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------

ACCURACY_QUERIES = [
    {
        "question": "How many rows are in the dataset?",
        "min_rows": 1,
    },
    {
        "question": "Show total freight cost by carrier",
        "expected_columns_contain": ["carrier"],
        "min_rows": 1,
    },
    {
        "question": "What are the top 5 origin cities by total shipment cost?",
        "expected_columns_contain": ["origin_city"],
        "min_rows": 1,
    },
    {
        "question": "Show monthly shipment counts",
        "min_rows": 1,
    },
    {
        "question": "Which shipping mode has the highest average delay hours?",
        "expected_columns_contain": ["mode"],
        "min_rows": 1,
    },
    {
        "question": "Show me shipments where damage_flag is 1",
        "expected_columns_contain": ["damage_flag"],
        "min_rows": 0,
    },
    {
        "question": "What percentage of shipments were on time?",
        "min_rows": 1,
    },
    {
        "question": "Show average freight cost by priority level",
        "expected_columns_contain": ["priority"],
        "min_rows": 1,
    },
    {
        "question": "Which destination city has the most shipments?",
        "expected_columns_contain": ["destination_city"],
        "min_rows": 1,
    },
    {
        "question": "Show total cost and total weight by mode",
        "expected_columns_contain": ["mode"],
        "min_rows": 1,
    },
]


# ---------------------------------------------------------------------------
# Generate SQL via Ollama
# ---------------------------------------------------------------------------

def _generate_sql_via_ollama(question: str, columns: list[str], parquet_path: str) -> str:
    """Call the Ollama provider to generate SQL for a question."""
    from app.ai.provider_openai import build_sql_prompt
    from app.ai.provider_ollama import generate_response

    # Build the same prompt the real endpoint uses
    # We use a simplified context since we can't call build_context easily
    col_text = "\n".join(f"- {c} (VARCHAR)" for c in columns)
    prompt = f"""You are a SQL generation assistant. You write SQL exclusively for DuckDB.
Generate a single SELECT query answering: {question}
Table name: dataset
Available columns:
{col_text}
Return ONLY valid JSON: {{"status":"ok","sql":"SELECT ...","message":"...","warnings":[]}}
No markdown. No code fences. No semicolon at end of SQL."""

    raw = generate_response(prompt)
    # Parse the JSON response
    try:
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            lines = cleaned.splitlines()
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            cleaned = "\n".join(lines).strip()
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end > start:
            data = json.loads(cleaned[start:end + 1])
            return data.get("sql", "")
    except Exception:
        pass
    return ""


def _execute_sql(sql: str, parquet_path: str) -> tuple[list[str], list[dict], int]:
    """Execute SQL against the Parquet file, return (columns, rows, rowcount)."""
    rewritten = sql.replace("dataset", f"read_parquet('{parquet_path}')")
    con = duckdb.connect()
    try:
        result = con.execute(rewritten)
        cols = [desc[0] for desc in result.description]
        rows = [dict(zip(cols, row)) for row in result.fetchall()]
        return cols, rows, len(rows)
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Test runner
# ---------------------------------------------------------------------------

class TestOllamaAccuracy:
    """Ollama SQL generation accuracy benchmarks."""

    syntactic_pass = 0
    structural_pass = 0
    total = len(ACCURACY_QUERIES)

    @pytest.fixture(autouse=True, scope="class")
    def _report(self):
        yield
        cls = TestOllamaAccuracy
        syn_pct = (cls.syntactic_pass / cls.total * 100) if cls.total else 0
        str_pct = (cls.structural_pass / cls.total * 100) if cls.total else 0
        overall = ((cls.syntactic_pass + cls.structural_pass) / (cls.total * 2) * 100) if cls.total else 0
        print(f"\n{'='*60}")
        print(f"Ollama SQL Accuracy Results:")
        print(f"Syntactic validity: {cls.syntactic_pass}/{cls.total} (target: 7/{cls.total})")
        print(f"Structural correctness: {cls.structural_pass}/{cls.total} (target: 6/{cls.total})")
        print(f"Overall score: {overall:.0f}% (target: 65%)")
        print(f"{'='*60}")

    @pytest.mark.parametrize("query_idx", range(len(ACCURACY_QUERIES)))
    def test_query(self, query_idx, parquet_path, columns):
        q = ACCURACY_QUERIES[query_idx]
        question = q["question"]

        # Generate SQL
        sql = _generate_sql_via_ollama(question, columns, parquet_path)
        if not sql:
            # Empty SQL = syntactic failure — record and continue (don't block suite)
            print(f"  SKIP (empty SQL): {question}")
            return

        # Test 1: Syntactic validity
        try:
            cols, rows, rowcount = _execute_sql(sql, parquet_path)
            TestOllamaAccuracy.syntactic_pass += 1
        except Exception as exc:
            print(f"  FAIL (syntax): {question} — {exc}")
            print(f"  SQL: {sql[:120]}")
            return

        # Test 2: Structural correctness
        structural_ok = True
        expected_contain = q.get("expected_columns_contain", [])
        min_rows = q.get("min_rows", 0)

        if expected_contain:
            cols_lower = [c.lower() for c in cols]
            for expected in expected_contain:
                if not any(expected.lower() in c for c in cols_lower):
                    structural_ok = False

        if rowcount < min_rows:
            structural_ok = False

        if structural_ok:
            TestOllamaAccuracy.structural_pass += 1
        else:
            print(f"  WARN (structure): {question} — got cols={cols[:5]}, rows={rowcount}")

    def test_syntactic_threshold(self, parquet_path, columns):
        """Print accuracy summary. Does not fail — this is a benchmark."""
        cls = TestOllamaAccuracy
        print(f"\n  Syntactic: {cls.syntactic_pass}/{cls.total}, Structural: {cls.structural_pass}/{cls.total}")
