from __future__ import annotations

"""
============================================================
FILE: routes.py
LOCATION: backend/app/ai/routes.py
============================================================

PURPOSE
-------
This module defines the FastAPI endpoints for all AI-driven
features of the Analytics Workbench.

These endpoints connect the frontend UI with the AI system.

CURRENT AI FEATURES
-------------------

1) Generate SQL from a natural-language question

   POST /api/ai/generate_sql

   Flow:
       question
           ↓
       OpenAI SQL generation
           ↓
       response parsing
           ↓
       SQL safety validation
           ↓
       DuckDB semantic validation
           ↓
       structured API response


2) Suggest useful questions for a dataset

   GET /api/ai/suggest_questions

   Flow:
       dataset selected
           ↓
       build dataset context
           ↓
       OpenAI suggests questions
           ↓
       normalized question list
           ↓
       returned to frontend


DESIGN PRINCIPLE
----------------
AI never directly executes SQL.

The AI system only proposes SQL or questions.

Execution happens later through the SQL execution layer
in the main backend.

This separation improves:

• safety
• transparency
• debuggability

============================================================
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Query

logger = logging.getLogger("app")

from .provider_openai import (
    generate_sql_for_dataset,
    suggest_questions_for_dataset,
    generate_insights_for_dataset,
    generate_explanation,
)
from .context_builder import build_reference_context

from .response_parser import parse_generate_sql_response

from pydantic import ValidationError

from .schemas import (
    GenerateSQLRequest,
    GenerateSQLResponse,
    InsightItem,
    InsightsResponse,
    ExplainRequest,
    ExplainResponse,
)

from .sql_validator import (
    validate_generated_sql,
    validate_sql_with_duckdb,
)

# ============================================================
# ROUTER INITIALIZATION
# ------------------------------------------------------------
# All AI endpoints are grouped under /api/ai
#
# Example:
#   /api/ai/generate_sql
#   /api/ai/suggest_questions
# ============================================================

router = APIRouter(prefix="/api/ai", tags=["AI"])


# ============================================================
# DATASET PATH RESOLUTION
# ------------------------------------------------------------
# The AI layer does not directly know where datasets live.
#
# Instead we call the dataset resolver defined in main.py.
#
# We support both development mode and packaged EXE mode.
# ============================================================

def _get_dataset_source_path(dataset: str):
    """
    Resolve the dataset source path.

    This function bridges the AI module and the main
    application module where dataset configuration lives.
    """

    try:
        # Development mode import
        from app.main import dataset_source_path
    except Exception:
        # Packaged EXE fallback
        from main import dataset_source_path

    return dataset_source_path(dataset)


def _build_reference_context_if_loaded(reference_name: str | None) -> dict | None:
    """
    Build reference table context for AI prompts if a reference table is loaded.
    Returns None if no reference table is specified or found.
    """
    if not reference_name:
        return None
    try:
        try:
            from app.main import REFERENCES_DIR
        except Exception:
            from main import REFERENCES_DIR
        ref_pq = (REFERENCES_DIR / reference_name / "source.parquet").resolve()
        if not ref_pq.exists():
            return None
        return build_reference_context(
            reference_name=reference_name,
            reference_source_path=str(ref_pq),
        )
    except Exception:
        logger.warning("Failed to build reference context for %s", reference_name)
        return None


def _suggestions_cache_path(dataset: str) -> Path:
    """
    Return the path to the suggestions cache file for a dataset.

    We derive the dataset directory from the source path so we
    don't need to re-import DATASETS_DIR separately.
    """
    src, _ = _get_dataset_source_path(dataset)
    return Path(src).parent / "dataset_context.json"


def _read_suggestions_cache(dataset: str) -> list[str] | None:
    """Return cached questions list, or None if no valid cache exists."""
    try:
        cache = json.loads(_suggestions_cache_path(dataset).read_text(encoding="utf-8"))
        questions = cache.get("questions")
        if isinstance(questions, list) and questions:
            return questions
    except Exception:
        pass
    return None


def _write_suggestions_cache(dataset: str, questions: list[str]) -> None:
    """Persist questions to dataset_context.json inside the dataset directory.

    Reads then merges so an existing 'insights' key is preserved.
    """
    try:
        cache_path = _suggestions_cache_path(dataset)
        existing: dict = {}
        try:
            existing = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        existing["questions"] = questions
        existing["generated_at"] = datetime.now(timezone.utc).isoformat()
        cache_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Could not write suggestions cache for %s: %s", dataset, exc)


def _build_synopsis_from_meta(dataset: str) -> str:
    """
    Build a basic dataset synopsis from _meta.json without any AI call.

    _meta.json always lives in the dataset directory (DATASETS_DIR/<name>/),
    regardless of whether the dataset is copy-mode or reference-mode.

    Used as a fallback when neither insights_synopsis nor grain_description
    is present — ensures the synopsis block always shows something useful
    for existing cached insights without requiring a refresh.
    """
    try:
        try:
            from app.main import DATASETS_DIR
        except Exception:
            from main import DATASETS_DIR  # type: ignore[no-redef]
        meta_path = (DATASETS_DIR / dataset / "_meta.json").resolve()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        row_count = meta.get("row_count", 0)
        columns: list = meta.get("columns", [])
        col_count = meta.get("column_count", len(columns))
        shown = columns[:6]
        col_str = ", ".join(shown)
        if col_count > len(shown):
            col_str += f", and {col_count - len(shown)} more"
        return (
            f"{dataset} — {row_count:,} rows × {col_count} columns. "
            f"Columns: {col_str}."
        )
    except Exception:
        return ""


def _read_insights_cache(dataset: str) -> dict | None:
    """Return cached insights dict {"synopsis": str, "insights": list}, or None.

    Malformed items in the cached list are silently skipped so that a single
    bad entry doesn't discard the entire cache.
    """
    try:
        cache = json.loads(_suggestions_cache_path(dataset).read_text(encoding="utf-8"))
        raw_insights = cache.get("insights")
        if isinstance(raw_insights, list) and raw_insights:
            # Filter out items that fail InsightItem validation so a single
            # corrupt entry never breaks the whole response.
            valid: list[dict] = []
            for item in raw_insights:
                if not isinstance(item, dict):
                    continue
                try:
                    InsightItem(**item)
                    valid.append(item)
                except (ValidationError, TypeError):
                    logger.warning(
                        "skipping malformed insight cache item | dataset=%s | item=%s",
                        dataset,
                        str(item)[:120],
                    )

            if not valid:
                return None

            # Priority: AI-generated synopsis → grain description → metadata fallback.
            # This ensures the synopsis block is always populated, even for
            # pre-existing caches that predate the synopsis field.
            synopsis = (
                cache.get("insights_synopsis")
                or cache.get("grain_description")
                or _build_synopsis_from_meta(dataset)
            )
            return {"synopsis": synopsis, "insights": valid}
    except Exception:
        pass
    return None


def _write_insights_cache(dataset: str, synopsis: str, insights: list[dict]) -> None:
    """Persist insights and synopsis to dataset_context.json.

    Reads then merges so the 'questions' key is preserved.
    """
    try:
        cache_path = _suggestions_cache_path(dataset)
        existing: dict = {}
        try:
            existing = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        existing["insights"] = insights
        existing["insights_synopsis"] = synopsis
        existing["insights_generated_at"] = datetime.now(timezone.utc).isoformat()
        cache_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Could not write insights cache for %s: %s", dataset, exc)


# ============================================================
# AI FEATURE: SUGGESTED QUESTIONS
# ------------------------------------------------------------
# Endpoint:
#     GET /api/ai/suggest_questions
#
# PURPOSE
# -------
# Provide users with a small set of useful questions they
# could ask about the dataset.
#
# This helps with:
# • onboarding
# • dataset discovery
# • reducing blank-page friction
#
# Example output:
#
# {
#   "dataset": "claims",
#   "questions": [
#       "Top HCPCS codes by total claims",
#       "Total paid by month",
#       "Providers with highest total paid"
#   ]
# }
# ============================================================

@router.get("/suggest_questions")
def suggest_questions(
    dataset: str,
    max_questions: int = 8,
    refresh: bool = Query(False, description="Force regeneration, ignoring cache"),
):
    """
    Return AI-generated suggested questions for the dataset.

    Parameters
    ----------
    dataset : str
        Dataset name
    max_questions : int
        Maximum number of suggested questions
    refresh : bool
        When True, bypass the cache and call OpenAI even if cached
        questions exist. Overwrites the cache with fresh results.
    """

    try:

        # ----------------------------------------------------
        # STEP 1 — Return cached questions if available
        #
        # dataset_context.json in the dataset directory holds
        # the last set of AI-generated questions. This avoids
        # an OpenAI round-trip on every Suggestions click.
        # ----------------------------------------------------
        if not refresh:
            cached = _read_suggestions_cache(dataset)
            if cached is not None:
                logger.info("suggest_questions cache hit | dataset=%s", dataset)
                return {"dataset": dataset, "questions": cached, "cached": True}

        # ----------------------------------------------------
        # STEP 2 — Ask the provider layer for suggestions
        # ----------------------------------------------------
        logger.info(
            "suggest_questions calling OpenAI | dataset=%s | refresh=%s",
            dataset,
            refresh,
        )
        questions = suggest_questions_for_dataset(
            dataset_name=dataset,
            dataset_source_path_fn=_get_dataset_source_path,
            max_questions=max_questions,
        )

        # ----------------------------------------------------
        # STEP 3 — Persist to cache so next click is instant
        # ----------------------------------------------------
        if questions:
            _write_suggestions_cache(dataset, questions)

        # ----------------------------------------------------
        # STEP 4 — Return structured response
        # ----------------------------------------------------
        return {
            "dataset": dataset,
            "questions": questions,
            "cached": False,
        }

    except Exception as e:

        # ----------------------------------------------------
        # Fail safely — never crash the API
        # ----------------------------------------------------
        return {
            "dataset": dataset,
            "questions": [],
            "error": f"{type(e).__name__}: {e}",
        }


# ============================================================
# AI FEATURE: DATASET INSIGHTS
# ------------------------------------------------------------
# Endpoint:
#     GET /api/ai/insights
#
# PURPOSE
# -------
# Surface 3–5 non-obvious findings about the dataset so the
# user sees value immediately after loading a file — no query
# required.
#
# Follows the same caching pattern as suggest_questions:
# - First call: run AI analysis, cache in dataset_context.json
# - Subsequent calls: return from cache instantly
# - ?refresh=true: bypass cache, regenerate, fallback to stale
# ============================================================

@router.get("/insights", response_model=InsightsResponse)
def get_insights(
    dataset: str,
    max_insights: int = 5,
    refresh: bool = Query(False, description="Force regeneration, ignoring cache"),
):
    """
    Return AI-generated insights for the dataset.

    Parameters
    ----------
    dataset : str
        Dataset name
    max_insights : int
        Maximum number of insights to generate
    refresh : bool
        When True, bypass the cache and call OpenAI even if cached
        insights exist. Overwrites the cache with fresh results.
        If OpenAI fails during refresh, returns stale cache as fallback.
    """

    try:

        # ----------------------------------------------------
        # STEP 1 — Return cached insights if available
        # ----------------------------------------------------
        if not refresh:
            cached = _read_insights_cache(dataset)
            if cached is not None:
                logger.info("insights cache hit | dataset=%s", dataset)
                return InsightsResponse(
                    dataset=dataset,
                    synopsis=cached["synopsis"],
                    insights=[InsightItem(**i) for i in cached["insights"]],
                    cached=True,
                )

        # ----------------------------------------------------
        # STEP 2 — Ask the provider layer for insights
        # ----------------------------------------------------
        logger.info(
            "insights calling OpenAI | dataset=%s | refresh=%s",
            dataset,
            refresh,
        )

        result = generate_insights_for_dataset(
            dataset_name=dataset,
            dataset_source_path_fn=_get_dataset_source_path,
            max_insights=max_insights,
        )
        synopsis = result.get("synopsis", "")
        raw_insights = result.get("insights", [])

        # ----------------------------------------------------
        # STEP 3 — Persist to cache so next load is instant
        # ----------------------------------------------------
        if raw_insights:
            _write_insights_cache(dataset, synopsis, raw_insights)

        # ----------------------------------------------------
        # STEP 4 — Return structured response
        # ----------------------------------------------------
        return InsightsResponse(
            dataset=dataset,
            synopsis=synopsis,
            insights=[InsightItem(**i) for i in raw_insights],
            cached=False,
        )

    except Exception as e:

        # ----------------------------------------------------
        # On refresh failure — try to return stale cache
        # ----------------------------------------------------
        if refresh:
            stale = _read_insights_cache(dataset)
            if stale:
                logger.warning(
                    "insights refresh failed, returning stale cache | dataset=%s | error=%s",
                    dataset, e,
                )
                return InsightsResponse(
                    dataset=dataset,
                    synopsis=stale["synopsis"],
                    insights=[InsightItem(**i) for i in stale["insights"]],
                    cached=True,
                )

        # ----------------------------------------------------
        # Fail safely — never crash the API
        # ----------------------------------------------------
        logger.exception("insights failed | dataset=%s | error=%s", dataset, e)
        return InsightsResponse(
            dataset=dataset,
            insights=[],
            error=f"{type(e).__name__}: {e}",
        )


# ============================================================
# AI FEATURE: GENERATE SQL
# ------------------------------------------------------------
# Endpoint:
#     POST /api/ai/generate_sql
#
# PURPOSE
# -------
# Convert a natural-language question into SQL using AI.
#
# This endpoint DOES NOT execute the SQL.
#
# The workflow is intentionally:
#
# question
#     ↓
# generate SQL
#     ↓
# user reviews / edits SQL
#     ↓
# Run SQL endpoint executes it
#
# This keeps the system transparent and safe.
# ============================================================

@router.post("/generate_sql", response_model=GenerateSQLResponse)
def generate_sql(payload: GenerateSQLRequest) -> GenerateSQLResponse:

    try:

        # ----------------------------------------------------
        # STEP 1 — Resolve the question from either the
        # "question" or "prompt" field.
        #
        # The frontend sends four attempts using both keys:
        #   { dataset, question: prompt }
        #   { dataset, prompt: prompt }
        #
        # If GenerateSQLRequest only declares "question",
        # the prompt-keyed attempts arrive with question=None.
        # We fall back to payload.prompt if it exists so both
        # frontend key variants reach the AI provider.
        # ----------------------------------------------------
        question = (
            getattr(payload, "question", None)
            or getattr(payload, "prompt", None)
            or ""
        ).strip()

        if not question:
            return GenerateSQLResponse(
                status="error",
                dataset=payload.dataset,
                question="",
                sql="",
                message="No question or prompt provided.",
                warnings=[],
            )

        # ----------------------------------------------------
        # STEP 2 — Generate SQL using the AI provider
        # ----------------------------------------------------
        ref_ctx = _build_reference_context_if_loaded(
            getattr(payload, "reference", None)
        )
        model_output = generate_sql_for_dataset(
            dataset_name=payload.dataset,
            question=question,
            dataset_source_path_fn=_get_dataset_source_path,
            reference_context=ref_ctx,
        )

        # ----------------------------------------------------
        # STEP 2 — Parse the raw model output
        # ----------------------------------------------------
        parsed = parse_generate_sql_response(model_output)

        # ----------------------------------------------------
        # STEP 3 — Validate SQL safety
        #
        # This prevents dangerous SQL like:
        # DROP / DELETE / UPDATE etc.
        # ----------------------------------------------------
        if parsed["status"] == "ok":

            is_valid, validation_message = validate_generated_sql(parsed["sql"])

            if not is_valid:
                return GenerateSQLResponse(
                    status="error",
                    dataset=payload.dataset,
                    question=payload.question,
                    sql="",
                    message=validation_message,
                    warnings=parsed["warnings"],
                )

            # ------------------------------------------------
            # STEP 4 — Validate SQL semantics with DuckDB
            #
            # This catches issues like:
            # • nonexistent columns
            # • syntax errors
            # • invalid table references
            #
            # We use:
            #
            #     EXPLAIN <query>
            #
            # so the query is never actually executed.
            # ------------------------------------------------
            duck_ok, duck_message = validate_sql_with_duckdb(
                sql=parsed["sql"],
                dataset_name=payload.dataset,
                dataset_source_path_fn=_get_dataset_source_path,
            )

            if not duck_ok:
                return GenerateSQLResponse(
                    status="error",
                    dataset=payload.dataset,
                    question=payload.question,
                    sql="",
                    message=duck_message,
                    warnings=parsed["warnings"],
                )

        # ----------------------------------------------------
        # STEP 5 — Return final structured response
        # ----------------------------------------------------
        return GenerateSQLResponse(
            status=parsed["status"],
            dataset=payload.dataset,
            question=payload.question,
            sql=parsed["sql"],
            message=parsed["message"],
            warnings=parsed["warnings"],
        )

    except Exception as e:

        # ----------------------------------------------------
        # FINAL SAFETY NET
        #
        # If anything unexpected happens we return a clean
        # structured error instead of crashing the API.
        #
        # The full error text is returned in "message" so
        # the frontend toast can surface it for diagnosis.
        # ----------------------------------------------------
        import logging as _logging
        _logging.getLogger("app").exception(
            "generate_sql failed | dataset=%s | error=%s",
            getattr(payload, "dataset", "unknown"),
            e,
        )
        return GenerateSQLResponse(
            status="error",
            dataset=payload.dataset,
            question=getattr(payload, "question", "") or "",
            sql="",
            message=f"{type(e).__name__}: {e}",
            warnings=[],
        )


# ============================================================
# AI FEATURE: EXPLAIN SQL RESULTS
# ------------------------------------------------------------
# Endpoint:
#     POST /api/ai/explain
#
# PURPOSE
# -------
# Explain what a SQL query does and what its results mean
# in plain-English business terms.
#
# The frontend sends the SQL from the editor plus the column
# names and result rows from the most recent /api/sql run.
# The AI returns 2–4 sentences of plain text (not JSON).
# ============================================================

@router.post("/explain", response_model=ExplainResponse)
def explain_sql(payload: ExplainRequest) -> ExplainResponse:
    try:
        explanation = generate_explanation(
            sql=payload.sql,
            columns=payload.columns,
            rows=payload.rows,
            dataset_name=payload.dataset,
        )
        return ExplainResponse(explanation=explanation)
    except Exception as e:
        logger.exception("explain failed | dataset=%s | error=%s", payload.dataset, e)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")