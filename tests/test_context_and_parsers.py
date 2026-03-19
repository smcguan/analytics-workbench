"""
test_context_and_parsers.py — unit tests for context_builder and AI response parsers

Covers:
  build_context() — schema, sample rows, numeric stats, categorical values, errors
  build_reference_context() — schema-only reference table context
  parse_insights_response() — JSON parsing, field validation, priority clamping
  parse_suggested_questions() — JSON parsing, dedup, edge cases

Run from project root:
    pytest tests/test_context_and_parsers.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from app.ai.context_builder import build_context, build_reference_context
from app.ai.provider_openai import parse_insights_response, parse_suggested_questions


# ===========================================================================
# FIXTURES — small Parquet datasets for context builder tests
# ===========================================================================

@pytest.fixture(scope="module")
def mixed_dataset_dir(tmp_path_factory) -> Path:
    """Dataset with both numeric and text columns."""
    tmp = tmp_path_factory.mktemp("ctx_mixed")
    df = pd.DataFrame({
        "drug_name": ["DrugA", "DrugB", "DrugC", "DrugA", "DrugB",
                       "DrugC", "DrugA", "DrugB", "DrugC", "DrugA"],
        "category": ["Oncology", "Cardio", "Oncology", "Cardio", "Oncology",
                      "Cardio", "Oncology", "Cardio", "Oncology", "Cardio"],
        "total_paid": [100.0, 200.0, 150.0, 300.0, 250.0,
                       175.0, 225.0, 125.0, 350.0, 275.0],
        "claim_count": [10, 20, 15, 30, 25, 18, 22, 12, 35, 28],
    })
    df["total_paid"] = df["total_paid"].astype("float64")
    df["claim_count"] = df["claim_count"].astype("int64")
    df.to_parquet(str(tmp / "source.parquet"), index=False)
    return tmp


@pytest.fixture(scope="module")
def numeric_only_dir(tmp_path_factory) -> Path:
    """Dataset with only numeric columns (no text)."""
    tmp = tmp_path_factory.mktemp("ctx_numeric")
    df = pd.DataFrame({
        "value_a": [1.0, 2.0, 3.0, 4.0, 5.0],
        "value_b": [10, 20, 30, 40, 50],
    })
    df["value_a"] = df["value_a"].astype("float64")
    df["value_b"] = df["value_b"].astype("int64")
    df.to_parquet(str(tmp / "source.parquet"), index=False)
    return tmp


@pytest.fixture(scope="module")
def text_only_dir(tmp_path_factory) -> Path:
    """Dataset with only text columns (no numeric)."""
    tmp = tmp_path_factory.mktemp("ctx_text")
    df = pd.DataFrame({
        "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
        "region": ["East", "West", "East", "West", "East"],
    })
    df.to_parquet(str(tmp / "source.parquet"), index=False)
    return tmp


@pytest.fixture(scope="module")
def reference_dir(tmp_path_factory) -> Path:
    """Small reference table for build_reference_context tests."""
    tmp = tmp_path_factory.mktemp("ctx_ref")
    df = pd.DataFrame({
        "drug_name": ["Keytruda", "Humira", "Enbrel"],
        "ira_round": [1, 1, 2],
    })
    df["ira_round"] = df["ira_round"].astype("int64")
    df.to_parquet(str(tmp / "source.parquet"), index=False)
    return tmp


def _make_source_path_fn(directory: Path):
    """Create a dataset_source_path_fn that returns the Parquet in the given dir."""
    def fn(name: str):
        p = directory / "source.parquet"
        if not p.exists():
            raise FileNotFoundError(f"Dataset not found: {name}")
        return str(p), False
    return fn


def _make_missing_source_path_fn():
    """Create a dataset_source_path_fn that always raises FileNotFoundError."""
    def fn(name: str):
        raise FileNotFoundError(f"Dataset not found: {name}")
    return fn


# ===========================================================================
# build_context() — basic structure
# ===========================================================================

def test_build_context_returns_required_keys(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir))
    for key in ("columns", "sample_rows", "numeric_stats", "categorical_values"):
        assert key in ctx, f"Missing key '{key}' in context"


def test_build_context_columns_match_schema(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir))
    col_names = [c["name"] for c in ctx["columns"]]
    assert col_names == ["drug_name", "category", "total_paid", "claim_count"]


def test_build_context_sample_rows_default_count(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir))
    assert len(ctx["sample_rows"]) == 5


def test_build_context_sample_rows_custom_count(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir), max_sample_rows=3)
    assert len(ctx["sample_rows"]) == 3


# ===========================================================================
# build_context() — numeric stats
# ===========================================================================

def test_build_context_numeric_stats_min_max_avg(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir))
    stats = ctx["numeric_stats"]
    stat_cols = {s["column"] for s in stats}
    assert "total_paid" in stat_cols
    assert "claim_count" in stat_cols
    for s in stats:
        assert "min" in s
        assert "max" in s
        assert "avg" in s


def test_build_context_numeric_stats_values_correct(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir))
    paid_stats = [s for s in ctx["numeric_stats"] if s["column"] == "total_paid"][0]
    assert paid_stats["min"] == 100.0
    assert paid_stats["max"] == 350.0


# ===========================================================================
# build_context() — categorical values
# ===========================================================================

def test_build_context_categorical_values_for_text_cols(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir))
    cat_cols = {c["column"] for c in ctx["categorical_values"]}
    assert "drug_name" in cat_cols
    assert "category" in cat_cols


def test_build_context_categorical_values_content(mixed_dataset_dir):
    ctx = build_context("test", _make_source_path_fn(mixed_dataset_dir))
    cat_map = {c["column"]: c["values"] for c in ctx["categorical_values"]}
    # category column has exactly 2 distinct values
    assert set(cat_map["category"]) == {"Oncology", "Cardio"}


# ===========================================================================
# build_context() — edge cases: no numeric / no text
# ===========================================================================

def test_build_context_no_numeric_columns(text_only_dir):
    ctx = build_context("test", _make_source_path_fn(text_only_dir))
    assert ctx["numeric_stats"] == []


def test_build_context_no_text_columns(numeric_only_dir):
    ctx = build_context("test", _make_source_path_fn(numeric_only_dir))
    assert ctx["categorical_values"] == []


# ===========================================================================
# build_context() — nonexistent dataset
# ===========================================================================

def test_build_context_nonexistent_dataset_raises_404():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        build_context("nonexistent_xyz", _make_missing_source_path_fn())
    assert exc_info.value.status_code == 404


# ===========================================================================
# build_reference_context()
# ===========================================================================

def test_build_reference_context_returns_columns_key(reference_dir):
    ctx = build_reference_context("ira_drugs", str(reference_dir / "source.parquet"))
    assert "columns" in ctx


def test_build_reference_context_columns_match_schema(reference_dir):
    ctx = build_reference_context("ira_drugs", str(reference_dir / "source.parquet"))
    col_names = [c["name"] for c in ctx["columns"]]
    assert col_names == ["drug_name", "ira_round"]


def test_build_reference_context_no_sample_rows(reference_dir):
    ctx = build_reference_context("ira_drugs", str(reference_dir / "source.parquet"))
    assert "sample_rows" not in ctx


# ===========================================================================
# parse_insights_response() — valid input
# ===========================================================================

def _make_insights_json(insights, synopsis="Test synopsis."):
    return json.dumps({"synopsis": synopsis, "insights": insights})


VALID_INSIGHT = {
    "type": "concentration",
    "headline": "Top 5 drugs account for 42% of spending",
    "explanation": "Spending is concentrated in a few drugs.",
    "sql": "SELECT drug_name, SUM(total_paid) FROM dataset GROUP BY drug_name ORDER BY 2 DESC LIMIT 5",
    "chart_type": "bar",
    "priority": 1,
}

VALID_INSIGHT_2 = {
    "type": "outliers",
    "headline": "DrugX costs 10x the average",
    "explanation": "One drug is significantly more expensive.",
    "sql": "SELECT drug_name, total_paid FROM dataset ORDER BY total_paid DESC LIMIT 1",
    "chart_type": "bar",
    "priority": 2,
}


def test_parse_insights_valid_json_returns_correct_count():
    raw = _make_insights_json([VALID_INSIGHT, VALID_INSIGHT_2])
    result = parse_insights_response(raw)
    assert len(result["insights"]) == 2


def test_parse_insights_required_fields_present():
    raw = _make_insights_json([VALID_INSIGHT])
    result = parse_insights_response(raw)
    insight = result["insights"][0]
    for field in ("type", "headline", "explanation", "sql", "chart_type", "priority"):
        assert field in insight, f"Missing field '{field}'"


# ===========================================================================
# parse_insights_response() — priority clamping
# ===========================================================================

def test_parse_insights_priority_clamped_low():
    insight = {**VALID_INSIGHT, "priority": 0}
    raw = _make_insights_json([insight])
    result = parse_insights_response(raw)
    assert result["insights"][0]["priority"] == 1


def test_parse_insights_priority_clamped_high():
    insight = {**VALID_INSIGHT, "priority": 10}
    raw = _make_insights_json([insight])
    result = parse_insights_response(raw)
    assert result["insights"][0]["priority"] == 5


# ===========================================================================
# parse_insights_response() — missing required field
# ===========================================================================

def test_parse_insights_missing_field_skipped():
    bad = {"type": "concentration", "headline": "Missing sql and explanation"}
    raw = _make_insights_json([VALID_INSIGHT, bad, VALID_INSIGHT_2])
    result = parse_insights_response(raw)
    assert len(result["insights"]) == 2


# ===========================================================================
# parse_insights_response() — edge cases
# ===========================================================================

def test_parse_insights_empty_string():
    result = parse_insights_response("")
    assert result["insights"] == []


def test_parse_insights_non_json():
    result = parse_insights_response("I don't know how to analyze this dataset.")
    assert result["insights"] == []


def test_parse_insights_code_fence():
    inner = _make_insights_json([VALID_INSIGHT])
    raw = f"```json\n{inner}\n```"
    result = parse_insights_response(raw)
    assert len(result["insights"]) == 1


def test_parse_insights_synopsis_extracted():
    raw = _make_insights_json([VALID_INSIGHT], synopsis="This is a drug spending dataset.")
    result = parse_insights_response(raw)
    assert result["synopsis"] == "This is a drug spending dataset."


def test_parse_insights_malformed_json():
    raw = '{"synopsis": "test", "insights": [{"type": "concentration", "headline": "test"'
    result = parse_insights_response(raw)
    assert result["insights"] == []


# ===========================================================================
# parse_suggested_questions() — valid input
# ===========================================================================

def test_parse_questions_valid_json():
    raw = json.dumps({"questions": ["What is X?", "How many Y?", "Show top Z"]})
    result = parse_suggested_questions(raw)
    assert result == ["What is X?", "How many Y?", "Show top Z"]


def test_parse_questions_dedup():
    raw = json.dumps({"questions": ["What is X?", "What is X?", "How many Y?"]})
    result = parse_suggested_questions(raw)
    assert result == ["What is X?", "How many Y?"]


# ===========================================================================
# parse_suggested_questions() — edge cases
# ===========================================================================

def test_parse_questions_empty_string():
    result = parse_suggested_questions("")
    assert result == []


def test_parse_questions_non_json():
    result = parse_suggested_questions("I cannot generate questions for this dataset.")
    assert result == []


def test_parse_questions_code_fence():
    inner = json.dumps({"questions": ["Q1?", "Q2?"]})
    raw = f"```json\n{inner}\n```"
    result = parse_suggested_questions(raw)
    assert result == ["Q1?", "Q2?"]


def test_parse_questions_numbered_list_format():
    """Test that a numbered list (non-JSON) returns empty — function expects JSON."""
    raw = "1. Question one\n2. Question two\n3. Question three"
    result = parse_suggested_questions(raw)
    # The function requires JSON with a "questions" key; plain text returns empty
    assert result == []


def test_parse_questions_mixed_valid_invalid():
    raw = json.dumps({"questions": ["Valid question?", "", "  ", "Another valid?", 42]})
    result = parse_suggested_questions(raw)
    assert "Valid question?" in result
    assert "Another valid?" in result
    # Empty/whitespace entries filtered out; numeric 42 converted to string "42"
    assert "" not in result
    assert "  " not in result
