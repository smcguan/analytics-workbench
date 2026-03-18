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
