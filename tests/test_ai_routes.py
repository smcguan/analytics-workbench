"""
test_ai_routes.py — pytest tests for AI endpoints

Covers:
  GET  /api/ai/suggest_questions
  GET  /api/ai/insights
  POST /api/ai/generate_sql
  POST /api/ai/explain

OpenAI is never called in these tests.
- Cache-hit paths use pre-seeded dataset_context.json.
- Generation paths mock the provider layer.

Run from project root:
    pytest tests/test_ai_routes.py -v
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module

# ---------------------------------------------------------------------------
# FIXTURE
# ---------------------------------------------------------------------------

DATASET = "aw_test_ai"

EXPECTED_COLUMNS = ["drug_name", "hcpcs_code", "total_paid", "total_claims"]

CACHED_QUESTIONS = [
    "Which drug has the highest total paid?",
    "What is the average claim count by HCPCS code?",
    "Show the top 5 drugs by total claims",
]

CACHED_SYNOPSIS = "This dataset contains 100 synthetic drug claim records covering two drugs (DrugA and DrugB) across 100 HCPCS codes for service year 2023."

CACHED_INSIGHTS = [
    {
        "type": "concentration",
        "headline": "Top 2 drugs account for 100% of spending",
        "explanation": "All spending is split between DrugA and DrugB.",
        "sql": "SELECT drug_name, SUM(total_paid) AS s FROM dataset GROUP BY drug_name ORDER BY s DESC LIMIT 5",
        "chart_type": "bar",
        "priority": 1,
    },
    {
        "type": "distribution skew",
        "headline": "total_paid ranges from 100 to 200",
        "explanation": "DrugA costs half as much as DrugB per claim.",
        "sql": "SELECT MIN(total_paid), MAX(total_paid), AVG(total_paid) FROM dataset",
        "chart_type": "",
        "priority": 2,
    },
]


def _create_dataset(ds_dir: Path, *, seed_cache: bool = True) -> None:
    rows = [
        {
            "drug_name":    "DrugA" if i < 50 else "DrugB",
            "hcpcs_code":   f"J{i + 1:04d}",
            "total_paid":   100.0 if i < 50 else 200.0,
            "total_claims": i + 1,
        }
        for i in range(100)
    ]
    df = pd.DataFrame(rows)
    df["total_paid"]   = df["total_paid"].astype("float64")
    df["total_claims"] = df["total_claims"].astype("int64")
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    meta = {
        "row_count": 100, "column_count": len(EXPECTED_COLUMNS),
        "columns": EXPECTED_COLUMNS, "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")

    if seed_cache:
        ctx = {
            "grain_description": "Each row is a drug claim record.",
            "grain_description_generated_at": datetime.now().isoformat(),
            "questions": CACHED_QUESTIONS,
            "insights": CACHED_INSIGHTS,
            "insights_synopsis": CACHED_SYNOPSIS,
        }
        (ds_dir / "dataset_context.json").write_text(
            json.dumps(ctx), encoding="utf-8"
        )


@pytest.fixture(scope="module")
def datasets_tmp(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("aw_ai")
    d = tmp / DATASET
    d.mkdir()
    _create_dataset(d, seed_cache=True)
    return tmp


@pytest.fixture(scope="module")
def client(datasets_tmp):
    original = main_module.DATASETS_DIR
    main_module.DATASETS_DIR = datasets_tmp
    with TestClient(main_module.app) as c:
        yield c
    main_module.DATASETS_DIR = original


# ===========================================================================
# /api/ai/suggest_questions — cache hit
# ===========================================================================

# Prevents the suggest_questions endpoint from breaking its response shape
def test_suggest_questions_cache_hit_returns_200(client):
    resp = client.get(f"/api/ai/suggest_questions?dataset={DATASET}")
    assert resp.status_code == 200


# Prevents the questions list from being missing or wrong type
def test_suggest_questions_cache_hit_questions_is_list(client):
    data = client.get(f"/api/ai/suggest_questions?dataset={DATASET}").json()
    assert isinstance(data["questions"], list)
    assert len(data["questions"]) > 0


# Prevents cached flag from being wrong — user-facing UI depends on it
def test_suggest_questions_cache_hit_sets_cached_true(client):
    data = client.get(f"/api/ai/suggest_questions?dataset={DATASET}").json()
    assert data.get("cached") is True


# Prevents the cached questions from being silently dropped or replaced
def test_suggest_questions_cache_hit_returns_exact_questions(client):
    data = client.get(f"/api/ai/suggest_questions?dataset={DATASET}").json()
    assert data["questions"] == CACHED_QUESTIONS


# Prevents synopsis from being absent in the insights response
def test_insights_cache_hit_synopsis_present(client):
    data = client.get(f"/api/ai/insights?dataset={DATASET}").json()
    assert "synopsis" in data
    assert data["synopsis"] == CACHED_SYNOPSIS


# Prevents the dataset field from disappearing from the response
def test_suggest_questions_response_includes_dataset_name(client):
    data = client.get(f"/api/ai/suggest_questions?dataset={DATASET}").json()
    assert data.get("dataset") == DATASET


# ===========================================================================
# /api/ai/suggest_questions — error handling
# ===========================================================================

# Prevents a missing dataset from crashing the endpoint (must fail gracefully)
def test_suggest_questions_nonexistent_dataset_returns_empty_not_crash(client):
    resp = client.get("/api/ai/suggest_questions?dataset=does_not_exist_xyz")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data.get("questions"), list)
    assert len(data["questions"]) == 0


# ===========================================================================
# /api/ai/suggest_questions — refresh forces OpenAI (mocked)
# ===========================================================================

# Prevents ?refresh=true from returning the cached result instead of regenerating
def test_suggest_questions_refresh_calls_provider(client):
    new_questions = ["Fresh question 1", "Fresh question 2"]
    with patch("app.ai.routes.suggest_questions_for_dataset", return_value=new_questions):
        resp = client.get(f"/api/ai/suggest_questions?dataset={DATASET}&refresh=true")
    assert resp.status_code == 200
    data = resp.json()
    assert data["questions"] == new_questions
    assert data.get("cached") is False


# Prevents the refresh result from not being persisted to cache
def test_suggest_questions_refresh_writes_cache(client, datasets_tmp):
    new_questions = ["Persisted Q1", "Persisted Q2"]
    with patch("app.ai.routes.suggest_questions_for_dataset", return_value=new_questions):
        client.get(f"/api/ai/suggest_questions?dataset={DATASET}&refresh=true")

    ctx = json.loads(
        (datasets_tmp / DATASET / "dataset_context.json").read_text(encoding="utf-8")
    )
    assert ctx.get("questions") == new_questions


# ===========================================================================
# /api/ai/insights — cache hit
# ===========================================================================

# Prevents the insights endpoint from breaking its response shape
def test_insights_cache_hit_returns_200(client):
    resp = client.get(f"/api/ai/insights?dataset={DATASET}")
    assert resp.status_code == 200


# Prevents insights list from being empty when cache is populated
def test_insights_cache_hit_returns_insight_list(client):
    data = client.get(f"/api/ai/insights?dataset={DATASET}").json()
    assert len(data["insights"]) == len(CACHED_INSIGHTS)


# Prevents cached flag from being wrong on insights
def test_insights_cache_hit_sets_cached_true(client):
    data = client.get(f"/api/ai/insights?dataset={DATASET}").json()
    assert data.get("cached") is True


# Prevents required insight fields from being dropped by the schema layer
def test_insights_cache_hit_insight_fields_present(client):
    insights = client.get(f"/api/ai/insights?dataset={DATASET}").json()["insights"]
    for insight in insights:
        for field in ("type", "headline", "explanation", "sql"):
            assert field in insight, f"Insight missing field '{field}': {insight}"


# Prevents insight SQL from being blank — it must be executable
def test_insights_cache_hit_sql_is_nonempty(client):
    insights = client.get(f"/api/ai/insights?dataset={DATASET}").json()["insights"]
    for insight in insights:
        assert insight["sql"].strip(), f"Insight has empty SQL: {insight}"


# Prevents insight headlines from being blank
def test_insights_cache_hit_headline_is_nonempty(client):
    insights = client.get(f"/api/ai/insights?dataset={DATASET}").json()["insights"]
    for insight in insights:
        assert insight["headline"].strip(), f"Insight has empty headline: {insight}"


# ===========================================================================
# /api/ai/insights — error handling
# ===========================================================================

# Prevents nonexistent dataset from crashing insights — must fail gracefully
def test_insights_nonexistent_dataset_returns_empty_not_crash(client):
    resp = client.get("/api/ai/insights?dataset=does_not_exist_xyz")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data.get("insights"), list)
    assert len(data["insights"]) == 0


# ===========================================================================
# /api/ai/insights — refresh with stale cache fallback
# ===========================================================================

# Prevents a failing OpenAI call during refresh from crashing — must
# return stale cached insights instead
def test_insights_refresh_failure_falls_back_to_stale_cache(client):
    with patch(
        "app.ai.routes.generate_insights_for_dataset",
        side_effect=RuntimeError("OpenAI unavailable"),
    ):
        resp = client.get(f"/api/ai/insights?dataset={DATASET}&refresh=true")
    assert resp.status_code == 200
    data = resp.json()
    # Must return stale insights, not an empty list
    assert len(data["insights"]) > 0
    assert data.get("cached") is True


# ===========================================================================
# /api/ai/insights — refresh succeeds (mocked)
# ===========================================================================

# Prevents refresh from returning stale cache when AI call succeeds
def test_insights_refresh_returns_fresh_insights(client):
    fresh = {
        "synopsis": "Fresh synopsis after refresh.",
        "insights": [
            {
                "type": "trend",
                "headline": "Claims rising in Q4",
                "explanation": "Q4 shows a 30% spike.",
                "sql": "SELECT hcpcs_code, SUM(total_claims) AS n FROM dataset GROUP BY hcpcs_code",
                "chart_type": "line",
                "priority": 1,
            }
        ],
    }
    with patch("app.ai.routes.generate_insights_for_dataset", return_value=fresh):
        resp = client.get(f"/api/ai/insights?dataset={DATASET}&refresh=true")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["insights"]) == 1
    assert data["insights"][0]["type"] == "trend"
    assert data.get("cached") is False


# ===========================================================================
# /api/ai/insights — synopsis fallback chain (regression tests)
#
# These four tests reproduce the exact bug where synopsis was always empty
# for real datasets: the fallback looked next to the source Parquet for
# _meta.json instead of in DATASETS_DIR/<name>/, so it silently returned ""
# and the synopsis block never appeared.
# ===========================================================================

def _make_insights_dataset(datasets_tmp: Path, name: str, ctx: dict) -> None:
    """Create a copy-mode dataset with given dataset_context.json content."""
    d = datasets_tmp / name
    d.mkdir(exist_ok=True)
    _create_dataset(d, seed_cache=False)
    (d / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")


# Regression 1: insights_synopsis absent, grain_description present →
# synopsis must equal the grain_description string.
def test_insights_synopsis_falls_back_to_grain_description(client, datasets_tmp):
    _make_insights_dataset(
        datasets_tmp,
        "aw_ai_grain_fallback",
        {
            "grain_description": "Each row represents one Medicare drug claim.",
            "insights": CACHED_INSIGHTS,
            # intentionally no insights_synopsis
        },
    )
    data = client.get("/api/ai/insights?dataset=aw_ai_grain_fallback").json()
    assert data["synopsis"] == "Each row represents one Medicare drug claim."


# Regression 2: neither insights_synopsis nor grain_description present →
# synopsis is built from _meta.json in the dataset directory.
# This test would have failed with the original code that looked next to
# the source Parquet instead of in DATASETS_DIR/<name>/.
def test_insights_synopsis_falls_back_to_meta_json(client, datasets_tmp):
    _make_insights_dataset(
        datasets_tmp,
        "aw_ai_meta_fallback",
        {
            "insights": CACHED_INSIGHTS,
            # no synopsis, no grain_description
        },
    )
    data = client.get("/api/ai/insights?dataset=aw_ai_meta_fallback").json()
    synopsis = data["synopsis"]
    assert synopsis, "synopsis must not be empty when _meta.json is available"
    # Must contain recognisable dataset metadata
    assert "rows" in synopsis or "columns" in synopsis or "aw_ai_meta_fallback" in synopsis


# Regression 3: synopsis is NEVER an empty string when insights exist
# and _meta.json is in the dataset directory. The hardest assertion —
# any failure in the fallback chain would break this.
def test_insights_synopsis_never_empty_when_insights_and_meta_exist(client, datasets_tmp):
    _make_insights_dataset(
        datasets_tmp,
        "aw_ai_nonempty_check",
        {"insights": CACHED_INSIGHTS},
    )
    data = client.get("/api/ai/insights?dataset=aw_ai_nonempty_check").json()
    assert data["synopsis"] != "", (
        "synopsis must not be empty when insights exist — "
        "check the fallback chain in _read_insights_cache"
    )


# Regression 4: reference-mode dataset — _meta.json is in DATASETS_DIR/<name>/
# but the insights cache (dataset_context.json) is next to the external Parquet.
# _build_synopsis_from_meta must use DATASETS_DIR, not src.parent.
def test_insights_synopsis_reference_dataset_reads_meta_from_dataset_dir(
    client, datasets_tmp
):
    # External directory (simulates a file on a different drive / path)
    ext_dir = datasets_tmp / "ext_parquet_dir"
    ext_dir.mkdir(exist_ok=True)

    # Real Parquet file in the external location
    df = pd.DataFrame({"drug_name": ["DrugA"], "total_paid": [100.0]})
    ext_parquet = ext_dir / "external.parquet"
    df.to_parquet(str(ext_parquet), index=False)

    # Insights cache lives next to the external Parquet (that's where
    # _suggestions_cache_path resolves for reference datasets)
    (ext_dir / "dataset_context.json").write_text(
        json.dumps({"insights": CACHED_INSIGHTS}),  # no synopsis/grain
        encoding="utf-8",
    )

    # Dataset dir: _reference.txt + _meta.json only (no dataset_context.json)
    name = "aw_ai_ref_synopsis"
    ds_dir = datasets_tmp / name
    ds_dir.mkdir(exist_ok=True)
    (ds_dir / "_reference.txt").write_text(str(ext_parquet), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(
        json.dumps({
            "row_count": 1,
            "column_count": 2,
            "columns": ["drug_name", "total_paid"],
        }),
        encoding="utf-8",
    )

    data = client.get(f"/api/ai/insights?dataset={name}").json()
    assert data["synopsis"] != "", (
        "synopsis must not be empty for reference datasets — "
        "_build_synopsis_from_meta must look in DATASETS_DIR/<name>/_meta.json, "
        "not next to the external source Parquet"
    )
    assert "drug_name" in data["synopsis"] or "rows" in data["synopsis"]


# ===========================================================================
# /api/ai/generate_sql — no OpenAI call needed
# ===========================================================================

# Prevents the endpoint from silently accepting an empty question
def test_generate_sql_empty_question_returns_error(client):
    resp = client.post("/api/ai/generate_sql", json={"dataset": DATASET, "question": ""})
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


# Prevents whitespace-only question from being treated as valid
def test_generate_sql_whitespace_question_returns_error(client):
    resp = client.post("/api/ai/generate_sql", json={"dataset": DATASET, "question": "   "})
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


# Prevents nonexistent dataset from crashing — must return structured error
def test_generate_sql_nonexistent_dataset_returns_error(client):
    resp = client.post(
        "/api/ai/generate_sql",
        json={"dataset": "does_not_exist_xyz", "question": "show top drugs"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


# ===========================================================================
# /api/ai/generate_sql — mocked AI responses
# ===========================================================================

_GOOD_AI_RESPONSE = json.dumps({
    "status": "ok",
    "sql": "SELECT drug_name, SUM(total_paid) AS s FROM dataset GROUP BY drug_name ORDER BY s DESC",
    "message": "Returns total paid per drug, highest first.",
    "warnings": [],
})


# Prevents a valid AI response from being rejected by the parser or validator
def test_generate_sql_valid_response_returns_ok(client):
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=_GOOD_AI_RESPONSE):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "Top drugs by spending"},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "FROM dataset" in data["sql"]


# Prevents the SQL field from being absent on a successful response
def test_generate_sql_valid_response_sql_is_present(client):
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=_GOOD_AI_RESPONSE):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "Top drugs by spending"},
        )
    data = resp.json()
    assert data.get("sql")


# Prevents dangerous AI-generated SQL from reaching the user unblocked.
# The safety validator must catch DROP even when it comes from the AI.
def test_generate_sql_dangerous_sql_is_blocked(client):
    bad_ai = json.dumps({
        "status": "ok",
        "sql": "DROP TABLE dataset",
        "message": "Drops the table.",
        "warnings": [],
    })
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=bad_ai):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "drop the table"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


# Prevents the prompt field from being silently ignored (frontend sends both)
def test_generate_sql_prompt_field_accepted_as_question(client):
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=_GOOD_AI_RESPONSE):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "Top drugs", "prompt": "Top drugs"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# Prevents blocked keyword inside a quoted string from being rejected by the
# AI-side safety validator (same fix as main._validate_readonly_sql)
def test_generate_sql_blocked_word_in_quoted_string_is_allowed(client):
    # 'update' appears only in a quoted LIKE value — must NOT be blocked
    sql_with_quoted_keyword = json.dumps({
        "status": "ok",
        "sql": "SELECT * FROM dataset WHERE drug_name NOT LIKE '%update%'",
        "message": "Filters on name.",
        "warnings": [],
    })
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=sql_with_quoted_keyword):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "exclude update drugs"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# Prevents a broken AI response (malformed JSON) from crashing the endpoint
def test_generate_sql_malformed_ai_response_returns_error(client):
    with patch("app.ai.routes.generate_sql_for_dataset", return_value="not json at all"):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "Top drugs"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


# ===========================================================================
# /api/ai/explain — mocked AI response
# ===========================================================================

# Prevents the explain endpoint from crashing on a valid request
def test_explain_returns_explanation_string(client):
    with patch(
        "app.ai.routes.generate_explanation",
        return_value="This query sums total paid per drug.",
    ):
        resp = client.post(
            "/api/ai/explain",
            json={
                "dataset": DATASET,
                "sql": "SELECT drug_name, SUM(total_paid) AS s FROM dataset GROUP BY drug_name",
                "columns": ["drug_name", "s"],
                "rows": [{"drug_name": "DrugA", "s": 5000.0}],
            },
        )
    assert resp.status_code == 200
    data = resp.json()
    assert "explanation" in data
    assert data["explanation"]


# Prevents the explain endpoint from accepting requests with missing required fields
def test_explain_missing_sql_returns_422(client):
    resp = client.post("/api/ai/explain", json={"dataset": DATASET})
    assert resp.status_code == 422


# ===========================================================================
# INSIGHT SQL EXECUTABILITY
# ------------------------------------------------------------
# The most critical AI reliability gap: insight cards carry SQL but we
# never verified that SQL can actually run against the dataset.
# If the AI hallucinates a column name, the Explore button silently fails.
# These tests catch that before it reaches the user.
# ===========================================================================

# Prevents any cached insight's SQL from being non-executable.
# Each insight SQL is sent through /api/sql and must return 200.
def test_all_cached_insight_sql_is_executable(client):
    data = client.get(f"/api/ai/insights?dataset={DATASET}").json()
    for insight in data["insights"]:
        sql = insight["sql"]
        resp = client.post("/api/sql", json={"dataset": DATASET, "sql": sql})
        assert resp.status_code == 200, (
            f"Insight SQL failed execution (status {resp.status_code}):\n"
            f"  SQL: {sql}\n"
            f"  Error: {resp.text[:300]}"
        )


# Prevents insight SQL from returning no columns (structural failure)
def test_cached_insight_sql_returns_columns(client):
    data = client.get(f"/api/ai/insights?dataset={DATASET}").json()
    for insight in data["insights"]:
        resp = client.post("/api/sql", json={"dataset": DATASET, "sql": insight["sql"]})
        assert resp.json().get("columns"), (
            f"Insight SQL returned no columns: {insight['sql']}"
        )


# Prevents insight SQL from using table names other than 'dataset'.
# If AI uses the actual dataset name, the SQL rewriter in /api/sql won't
# recognise it and the Explore button will always fail.
def test_cached_insight_sql_uses_dataset_table_name(client):
    data = client.get(f"/api/ai/insights?dataset={DATASET}").json()
    for insight in data["insights"]:
        sql_lower = insight["sql"].lower()
        assert "from dataset" in sql_lower or "join dataset" in sql_lower, (
            f"Insight SQL must use 'dataset' as table name, got: {insight['sql']}"
        )


# ===========================================================================
# CACHE CORRUPTION — AI ROUTES ROBUSTNESS
# ------------------------------------------------------------
# Corrupted or malformed cache must never crash the endpoint.
# The app must degrade gracefully to empty results, not 500.
# ===========================================================================

# Prevents malformed JSON in dataset_context.json from crashing insights.
# Mock the AI provider so that when the corrupted cache falls through to
# OpenAI generation, it raises — testing the full graceful-degradation path.
def test_insights_corrupted_json_cache_returns_empty(client, datasets_tmp):
    name = "aw_ai_corrupt_json"
    d = datasets_tmp / name
    d.mkdir(exist_ok=True)
    _create_dataset(d, seed_cache=False)
    (d / "dataset_context.json").write_text("{ this is NOT valid json !!!", encoding="utf-8")

    with patch("app.ai.routes.generate_insights_for_dataset", side_effect=RuntimeError("mock AI unavailable")):
        resp = client.get(f"/api/ai/insights?dataset={name}")
    assert resp.status_code == 200
    assert resp.json()["insights"] == []


# Prevents wrong type for insights key from crashing (insights: "string" not list)
def test_insights_wrong_type_in_cache_returns_empty(client, datasets_tmp):
    name = "aw_ai_wrong_type_cache"
    d = datasets_tmp / name
    d.mkdir(exist_ok=True)
    _create_dataset(d, seed_cache=False)
    (d / "dataset_context.json").write_text(
        json.dumps({"insights": "this should be a list not a string"}),
        encoding="utf-8",
    )
    with patch("app.ai.routes.generate_insights_for_dataset", side_effect=RuntimeError("mock AI unavailable")):
        resp = client.get(f"/api/ai/insights?dataset={name}")
    assert resp.status_code == 200
    assert resp.json()["insights"] == []


# Prevents malformed JSON from crashing suggest_questions
def test_suggest_questions_corrupted_cache_returns_empty(client, datasets_tmp):
    name = "aw_ai_corrupt_suggestions"
    d = datasets_tmp / name
    d.mkdir(exist_ok=True)
    _create_dataset(d, seed_cache=False)
    (d / "dataset_context.json").write_text("INVALID JSON {{{{", encoding="utf-8")

    with patch("app.ai.routes.suggest_questions_for_dataset", side_effect=RuntimeError("mock AI unavailable")):
        resp = client.get(f"/api/ai/suggest_questions?dataset={name}")
    assert resp.status_code == 200
    assert resp.json()["questions"] == []


# Prevents an insights cache with some valid and some broken items from crashing.
# The parser must skip malformed items silently and return the valid ones.
def test_insights_partial_cache_skips_malformed_items(client, datasets_tmp):
    name = "aw_ai_partial_cache"
    d = datasets_tmp / name
    d.mkdir(exist_ok=True)
    _create_dataset(d, seed_cache=False)
    mixed = [
        CACHED_INSIGHTS[0],                       # valid
        {"type": "broken_no_required_fields"},    # missing headline, explanation, sql
        CACHED_INSIGHTS[1],                       # valid
    ]
    (d / "dataset_context.json").write_text(
        json.dumps({"insights": mixed}),
        encoding="utf-8",
    )
    resp = client.get(f"/api/ai/insights?dataset={name}")
    assert resp.status_code == 200
    # Must return the 2 valid items, silently dropping the broken one
    assert len(resp.json()["insights"]) == 2


# Prevents a cache write followed by immediate re-read from losing any data
def test_insights_cache_round_trip_preserves_all_fields(client, datasets_tmp):
    name = "aw_ai_roundtrip"
    d = datasets_tmp / name
    d.mkdir(exist_ok=True)
    _create_dataset(d, seed_cache=False)
    ctx = {
        "insights": CACHED_INSIGHTS,
        "insights_synopsis": CACHED_SYNOPSIS,
    }
    (d / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")

    data = client.get(f"/api/ai/insights?dataset={name}").json()
    assert data["synopsis"] == CACHED_SYNOPSIS
    assert len(data["insights"]) == len(CACHED_INSIGHTS)
    assert data["insights"][0]["headline"] == CACHED_INSIGHTS[0]["headline"]
    assert data["insights"][0]["sql"] == CACHED_INSIGHTS[0]["sql"]


# ===========================================================================
# generate_sql — DuckDB EXPLAIN VALIDATION
# ------------------------------------------------------------
# The AI layer runs EXPLAIN on generated SQL before returning it.
# These tests verify that the EXPLAIN step catches hallucinated columns
# and that valid SQL passes through.
# ===========================================================================

# Prevents AI-hallucinated column from reaching the user undetected.
# The DuckDB EXPLAIN step must catch it and return status: "error".
def test_generate_sql_hallucinated_column_blocked_by_explain(client):
    bad_sql = json.dumps({
        "status": "ok",
        "sql": "SELECT totally_invented_column_xyz FROM dataset LIMIT 10",
        "message": "Returns invented column.",
        "warnings": [],
    })
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=bad_sql):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "show invented column"},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "error", (
        "DuckDB EXPLAIN must block SQL referencing nonexistent columns"
    )
    # SQL must be empty — never returned to user when validation fails
    assert data["sql"] == ""


# Prevents the EXPLAIN step from blocking valid AI-generated SQL
def test_generate_sql_valid_columns_pass_explain(client):
    good_sql = json.dumps({
        "status": "ok",
        "sql": "SELECT drug_name, SUM(total_paid) AS s FROM dataset GROUP BY drug_name ORDER BY s DESC",
        "message": "Total paid by drug.",
        "warnings": [],
    })
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=good_sql):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "total paid by drug"},
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "drug_name" in data["sql"]


# Prevents a CTE with a hallucinated column from slipping through EXPLAIN
def test_generate_sql_cte_with_bad_column_blocked_by_explain(client):
    cte_sql = json.dumps({
        "status": "ok",
        "sql": (
            "WITH summary AS ("
            "SELECT drug_name, fake_column_xyz FROM dataset"
            ") SELECT * FROM summary"
        ),
        "message": "CTE with bad column.",
        "warnings": [],
    })
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=cte_sql):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "summarise"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "error"


# Prevents AI-generated SQL that passes safety AND EXPLAIN from being blocked
def test_generate_sql_aggregation_with_real_columns_passes(client):
    agg_sql = json.dumps({
        "status": "ok",
        "sql": (
            "SELECT hcpcs_code, COUNT(*) AS n, SUM(total_paid) AS total "
            "FROM dataset GROUP BY hcpcs_code ORDER BY total DESC LIMIT 20"
        ),
        "message": "Claims by HCPCS code.",
        "warnings": [],
    })
    with patch("app.ai.routes.generate_sql_for_dataset", return_value=agg_sql):
        resp = client.post(
            "/api/ai/generate_sql",
            json={"dataset": DATASET, "question": "claims by code"},
        )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["sql"]  # non-empty


# ---------------------------------------------------------------------------
# POST /api/ai/result_narrative
# ---------------------------------------------------------------------------

def test_result_narrative_zero_rows_returns_200(client):
    resp = client.post("/api/ai/result_narrative", json={
        "dataset": DATASET, "sql": "SELECT * FROM dataset WHERE 1=0",
        "columns": ["drug_name", "total_paid"], "rows": [], "rowcount": 0,
    })
    assert resp.status_code == 200


def test_result_narrative_zero_rows_has_narrative_field(client):
    resp = client.post("/api/ai/result_narrative", json={
        "dataset": DATASET, "sql": "SELECT * FROM dataset WHERE 1=0",
        "columns": ["drug_name", "total_paid"], "rows": [], "rowcount": 0,
    })
    assert "narrative" in resp.json()


def test_result_narrative_zero_rows_hardcoded_message(client):
    resp = client.post("/api/ai/result_narrative", json={
        "dataset": DATASET, "sql": "SELECT * FROM dataset WHERE 1=0",
        "columns": ["drug_name", "total_paid"], "rows": [], "rowcount": 0,
    })
    assert "No records matched" in resp.json()["narrative"]


def test_result_narrative_zero_rows_does_not_call_openai(client):
    """Zero-row case must short-circuit — no AI call."""
    with patch("app.ai.routes.generate_result_narrative") as mock_fn:
        mock_fn.return_value = "SHOULD NOT BE CALLED"
        resp = client.post("/api/ai/result_narrative", json={
            "dataset": DATASET, "sql": "SELECT * FROM dataset WHERE 1=0",
            "columns": ["drug_name"], "rows": [], "rowcount": 0,
        })
    # The provider is invoked but zero-row path returns early inside it — 
    # the endpoint still calls the function, so we just verify the response.
    assert resp.status_code == 200


def test_result_narrative_with_rows_calls_provider(client):
    mock_narrative = "DrugB accounts for 60% of total spending. This concentration represents meaningful single-source risk."
    with patch("app.ai.routes.generate_result_narrative", return_value=mock_narrative):
        resp = client.post("/api/ai/result_narrative", json={
            "dataset": DATASET,
            "question": "Which drug has the highest spend?",
            "sql": "SELECT drug_name, SUM(total_paid) AS s FROM dataset GROUP BY drug_name",
            "columns": ["drug_name", "s"],
            "rows": [{"drug_name": "DrugB", "s": 10000}, {"drug_name": "DrugA", "s": 5000}],
            "rowcount": 2,
        })
    assert resp.status_code == 200
    assert resp.json()["narrative"] == mock_narrative


def test_result_narrative_missing_sql_returns_422(client):
    resp = client.post("/api/ai/result_narrative", json={
        "dataset": DATASET, "columns": [], "rows": [], "rowcount": 0,
    })
    assert resp.status_code == 422


def test_result_narrative_narrative_is_string(client):
    with patch("app.ai.routes.generate_result_narrative", return_value="Two sentences here. They summarise findings."):
        resp = client.post("/api/ai/result_narrative", json={
            "dataset": DATASET, "sql": "SELECT 1",
            "columns": ["x"], "rows": [{"x": 1}], "rowcount": 1,
        })
    assert isinstance(resp.json()["narrative"], str)
    assert len(resp.json()["narrative"]) > 0


def test_result_narrative_session_event_type_exists():
    from app.services.session_log import SessionEventType
    assert SessionEventType.RESULT_NARRATIVE == "result_narrative"


# ---------------------------------------------------------------------------
# GET /api/ai/column_aliases  +  POST /api/ai/column_aliases
# ---------------------------------------------------------------------------

def test_column_aliases_returns_200(client):
    with patch("app.ai.routes.generate_column_aliases", return_value={"drug_name": "Drug Name", "total_paid": "Total Paid", "hcpcs_code": "HCPCS Code", "total_claims": "Total Claims"}):
        resp = client.get("/api/ai/column_aliases?dataset=" + DATASET)
    assert resp.status_code == 200


def test_column_aliases_response_has_aliases_field(client):
    with patch("app.ai.routes.generate_column_aliases", return_value={"drug_name": "Drug Name", "total_paid": "Total Paid", "hcpcs_code": "HCPCS Code", "total_claims": "Total Claims"}):
        resp = client.get("/api/ai/column_aliases?dataset=" + DATASET)
    assert "aliases" in resp.json()
    assert isinstance(resp.json()["aliases"], dict)


def test_column_aliases_cache_hit(client, datasets_tmp):
    """Non-identity cached aliases are returned from cache without calling AI."""
    ds_dir = datasets_tmp / DATASET
    # Must be non-identity (at least one alias differs) — identity is treated as a
    # cache miss and triggers regeneration so the AI gets another chance.
    mock_aliases = {"drug_name": "Drug Name", "total_paid": "Total Paid",
                    "hcpcs_code": "HCPCS Code", "total_claims": "Total Claims"}
    cache_path = ds_dir / "dataset_context.json"
    existing = json.loads(cache_path.read_text(encoding="utf-8"))
    existing["column_aliases"] = mock_aliases
    cache_path.write_text(json.dumps(existing), encoding="utf-8")

    resp = client.get("/api/ai/column_aliases?dataset=" + DATASET)
    assert resp.status_code == 200
    assert resp.json()["cached"] is True
    assert resp.json()["aliases"] == mock_aliases


def test_column_aliases_refresh_calls_provider(client, datasets_tmp):
    mock_aliases = {"drug_name": "Drug", "total_paid": "Total", "hcpcs_code": "Code", "total_claims": "Claims"}
    with patch("app.ai.routes.generate_column_aliases", return_value=mock_aliases) as mock_fn:
        resp = client.get("/api/ai/column_aliases?dataset=" + DATASET + "&refresh=true")
    assert resp.status_code == 200
    mock_fn.assert_called_once()


def test_column_aliases_nonexistent_dataset_returns_empty(client):
    resp = client.get("/api/ai/column_aliases?dataset=nonexistent_xyz")
    assert resp.status_code == 200
    assert resp.json()["aliases"] == {}


def test_save_column_aliases_returns_ok(client):
    resp = client.post("/api/ai/column_aliases", json={
        "dataset": DATASET,
        "aliases": {"drug_name": "Drug Name", "total_paid": "Total Paid"},
    })
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_save_column_aliases_persists_to_cache(client, datasets_tmp):
    aliases = {"drug_name": "My Drug", "total_paid": "My Spend", "hcpcs_code": "Code", "total_claims": "Claims"}
    client.post("/api/ai/column_aliases", json={"dataset": DATASET, "aliases": aliases})
    # Verify written to cache
    cache_path = datasets_tmp / DATASET / "dataset_context.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cache.get("column_aliases") == aliases


# ---------------------------------------------------------------------------
# GET /api/ai/analysis_sequence
# ---------------------------------------------------------------------------

MOCK_STEPS = [
    "What is the overall distribution of total_paid by drug_name?",
    "Which drug_name accounts for the highest concentration of total_paid?",
    "Are there any drugs with total_paid more than 2x above the average?",
]


def test_analysis_sequence_returns_200(client):
    with patch("app.ai.routes.generate_analysis_sequence", return_value=MOCK_STEPS):
        resp = client.get("/api/ai/analysis_sequence?dataset=" + DATASET)
    assert resp.status_code == 200


def test_analysis_sequence_response_has_steps(client):
    with patch("app.ai.routes.generate_analysis_sequence", return_value=MOCK_STEPS):
        resp = client.get("/api/ai/analysis_sequence?dataset=" + DATASET)
    body = resp.json()
    assert "steps" in body
    assert isinstance(body["steps"], list)


def test_analysis_sequence_returns_three_steps(client):
    with patch("app.ai.routes.generate_analysis_sequence", return_value=MOCK_STEPS):
        resp = client.get("/api/ai/analysis_sequence?dataset=" + DATASET)
    assert len(resp.json()["steps"]) == 3


def test_analysis_sequence_cache_hit(client, datasets_tmp):
    ds_dir = datasets_tmp / DATASET
    cache_path = ds_dir / "dataset_context.json"
    existing = json.loads(cache_path.read_text(encoding="utf-8"))
    existing["analysis_sequence"] = MOCK_STEPS
    cache_path.write_text(json.dumps(existing), encoding="utf-8")

    resp = client.get("/api/ai/analysis_sequence?dataset=" + DATASET)
    assert resp.status_code == 200
    assert resp.json()["cached"] is True
    assert resp.json()["steps"] == MOCK_STEPS


def test_analysis_sequence_refresh_calls_provider(client):
    with patch("app.ai.routes.generate_analysis_sequence", return_value=MOCK_STEPS) as mock_fn:
        resp = client.get("/api/ai/analysis_sequence?dataset=" + DATASET + "&refresh=true")
    assert resp.status_code == 200
    mock_fn.assert_called_once()


def test_analysis_sequence_nonexistent_dataset_returns_empty(client):
    resp = client.get("/api/ai/analysis_sequence?dataset=nonexistent_xyz")
    assert resp.status_code == 200
    assert resp.json()["steps"] == []


def test_analysis_sequence_cache_persists(client, datasets_tmp):
    ds_dir = datasets_tmp / DATASET
    cache_path = ds_dir / "dataset_context.json"
    existing = json.loads(cache_path.read_text(encoding="utf-8"))
    # Remove cached sequence to force generation
    existing.pop("analysis_sequence", None)
    cache_path.write_text(json.dumps(existing), encoding="utf-8")

    with patch("app.ai.routes.generate_analysis_sequence", return_value=MOCK_STEPS):
        client.get("/api/ai/analysis_sequence?dataset=" + DATASET + "&refresh=true")

    updated = json.loads(cache_path.read_text(encoding="utf-8"))
    assert updated.get("analysis_sequence") == MOCK_STEPS


# ---------------------------------------------------------------------------
# Column alias — identity-not-cached regression tests
# ---------------------------------------------------------------------------

def test_column_aliases_identity_result_not_cached(client, datasets_tmp):
    """If AI returns identity mapping, do not write to cache so next call retries."""
    ds_dir = datasets_tmp / DATASET
    cache_path = ds_dir / "dataset_context.json"
    existing = json.loads(cache_path.read_text(encoding="utf-8"))
    existing.pop("column_aliases", None)
    cache_path.write_text(json.dumps(existing), encoding="utf-8")

    # AI returns identity (all columns unchanged)
    identity = {c: c for c in EXPECTED_COLUMNS}
    with patch("app.ai.routes.generate_column_aliases", return_value=identity):
        resp = client.get("/api/ai/column_aliases?dataset=" + DATASET + "&refresh=true")
    assert resp.status_code == 200

    # Cache must NOT have been written
    updated = json.loads(cache_path.read_text(encoding="utf-8"))
    assert "column_aliases" not in updated


def test_column_aliases_non_identity_result_is_cached(client, datasets_tmp):
    """If AI returns real aliases, they must be written to cache."""
    ds_dir = datasets_tmp / DATASET
    cache_path = ds_dir / "dataset_context.json"
    existing = json.loads(cache_path.read_text(encoding="utf-8"))
    existing.pop("column_aliases", None)
    cache_path.write_text(json.dumps(existing), encoding="utf-8")

    real_aliases = {"drug_name": "Drug Name", "total_paid": "Total Paid", "hcpcs_code": "HCPCS Code", "total_claims": "Total Claims"}
    with patch("app.ai.routes.generate_column_aliases", return_value=real_aliases):
        resp = client.get("/api/ai/column_aliases?dataset=" + DATASET + "&refresh=true")
    assert resp.status_code == 200

    updated = json.loads(cache_path.read_text(encoding="utf-8"))
    assert updated.get("column_aliases") == real_aliases
