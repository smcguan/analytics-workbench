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
    generate_result_narrative,
    generate_column_aliases,
    generate_analysis_sequence,
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
    ResultNarrativeRequest,
    ResultNarrativeResponse,
    ColumnAliasResponse,
    UpdateColumnAliasRequest,
    AnalysisSequenceResponse,
)

from .sql_validator import (
    validate_generated_sql,
    validate_sql_with_duckdb,
)

from app.services.session_log import log_event, SessionEventType

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


def _resolve_reference_parquet_path(reference_name: str | None) -> str | None:
    """
    Resolve the reference table's Parquet path on disk.
    Returns None if no reference is specified or the file doesn't exist.
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
        return str(ref_pq)
    except Exception:
        return None


def _build_reference_context_if_loaded(reference_name: str | None) -> dict | None:
    """
    Build reference table context for AI prompts if a reference table is loaded.
    Returns None if no reference table is specified or found.
    """
    if not reference_name:
        return None
    try:
        ref_pq_path = _resolve_reference_parquet_path(reference_name)
        if not ref_pq_path:
            return None
        return build_reference_context(
            reference_name=reference_name,
            reference_source_path=ref_pq_path,
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


def _read_aliases_cache(dataset: str) -> dict[str, str] | None:
    """Return cached column alias dict, or None if no valid cache exists."""
    try:
        cache = json.loads(_suggestions_cache_path(dataset).read_text(encoding="utf-8"))
        aliases = cache.get("column_aliases")
        if isinstance(aliases, dict):
            return aliases
    except Exception:
        pass
    return None


def _write_aliases_cache(dataset: str, aliases: dict[str, str]) -> None:
    """Persist column aliases to dataset_context.json. Merges with existing keys."""
    try:
        cache_path = _suggestions_cache_path(dataset)
        existing: dict = {}
        try:
            existing = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        existing["column_aliases"] = aliases
        cache_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Could not write aliases cache for %s: %s", dataset, exc)


def _get_dataset_columns(dataset: str) -> list[str]:
    """Read column names from _meta.json without an AI call."""
    try:
        try:
            from app.main import DATASETS_DIR
        except Exception:
            from main import DATASETS_DIR  # type: ignore[no-redef]
        meta_path = (DATASETS_DIR / dataset / "_meta.json").resolve()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        columns = meta.get("columns", [])
        return [str(c) for c in columns]
    except Exception:
        return []


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
        # STEP 0 — Check AI consent before doing anything
        # ----------------------------------------------------
        try:
            try:
                from app.main import DATASETS_DIR as _DS_DIR
            except Exception:
                from main import DATASETS_DIR as _DS_DIR  # type: ignore[no-redef]
            _consent_meta_path = (_DS_DIR / dataset / "_meta.json").resolve()
            if _consent_meta_path.exists():
                _consent_meta = json.loads(_consent_meta_path.read_text(encoding="utf-8"))
                if _consent_meta.get("ai_consent") is False:
                    return {
                        "dataset": dataset,
                        "questions": [],
                        "cached": False,
                        "error": "AI features disabled for this dataset",
                    }
        except Exception:
            pass  # If we can't read meta, allow suggestions (backward compat)

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
                try:
                    log_event(SessionEventType.SUGGESTIONS_GENERATED, {"dataset": dataset, "cached": True})
                except Exception:
                    logger.warning("Failed to log session event", exc_info=True)
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
        try:
            log_event(SessionEventType.SUGGESTIONS_GENERATED, {"dataset": dataset, "cached": False})
        except Exception:
            logger.warning("Failed to log session event", exc_info=True)

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
        # STEP 0 — Check AI consent before doing anything
        # ----------------------------------------------------
        try:
            try:
                from app.main import DATASETS_DIR
            except Exception:
                from main import DATASETS_DIR  # type: ignore[no-redef]
            _consent_meta_path = (DATASETS_DIR / dataset / "_meta.json").resolve()
            if _consent_meta_path.exists():
                _consent_meta = json.loads(_consent_meta_path.read_text(encoding="utf-8"))
                if _consent_meta.get("ai_consent") is False:
                    return InsightsResponse(
                        dataset=dataset,
                        insights=[],
                        cached=False,
                        synopsis="",
                        error="AI features disabled for this dataset",
                    )
        except Exception:
            pass  # If we can't read meta, allow insights (backward compat)

        # ----------------------------------------------------
        # STEP 1 — Return cached insights if available
        # ----------------------------------------------------
        if not refresh:
            cached = _read_insights_cache(dataset)
            if cached is not None:
                logger.info("insights cache hit | dataset=%s", dataset)
                try:
                    log_event(SessionEventType.INSIGHTS_GENERATED, {
                        "dataset": dataset,
                        "insight_count": len(cached["insights"]),
                        "cached": True,
                    })
                except Exception:
                    logger.warning("Failed to log session event", exc_info=True)
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
        try:
            log_event(SessionEventType.INSIGHTS_GENERATED, {
                "dataset": dataset,
                "insight_count": len(raw_insights),
                "cached": False,
            })
        except Exception:
            logger.warning("Failed to log session event", exc_info=True)

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
                reference_parquet_path=_resolve_reference_parquet_path(
                    getattr(payload, "reference", None)
                ),
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
        try:
            log_event(SessionEventType.AI_SQL_GENERATED, {
                "dataset": payload.dataset,
                "question": question,
            })
        except Exception:
            logger.warning("Failed to log session event", exc_info=True)

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


@router.post("/result_narrative", response_model=ResultNarrativeResponse)
def get_result_narrative(payload: ResultNarrativeRequest) -> ResultNarrativeResponse:
    try:
        narrative = generate_result_narrative(
            question=payload.question,
            sql=payload.sql,
            columns=payload.columns,
            rows=payload.rows,
            rowcount=payload.rowcount,
            dataset_name=payload.dataset,
        )
        log_event(SessionEventType.RESULT_NARRATIVE, {
            "dataset": payload.dataset,
            "narrative": narrative,
        })
        return ResultNarrativeResponse(narrative=narrative)
    except Exception as e:
        logger.exception("result_narrative failed | dataset=%s | error=%s", payload.dataset, e)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


@router.get("/column_aliases", response_model=ColumnAliasResponse)
def get_column_aliases(
    dataset: str,
    refresh: bool = Query(False, description="Force regeneration, ignoring cache"),
) -> ColumnAliasResponse:
    """Return human-readable display aliases for a dataset's column names.

    Cache hit: returns instantly from dataset_context.json.
    Cache miss or refresh=true: calls the AI, caches, and returns.
    """
    if not refresh:
        cached = _read_aliases_cache(dataset)
        if cached is not None:
            return ColumnAliasResponse(dataset=dataset, aliases=cached, cached=True)

    columns = _get_dataset_columns(dataset)
    if not columns:
        return ColumnAliasResponse(dataset=dataset, aliases={}, cached=False)

    try:
        aliases = generate_column_aliases(columns=columns, dataset_name=dataset)
        # Only cache if the AI actually renamed at least one column.
        # If every alias equals its original (identity fallback), the AI call
        # likely failed silently — do not write to cache so the next open retries.
        is_identity = all(aliases.get(c, c) == c for c in columns)
        if not is_identity:
            _write_aliases_cache(dataset, aliases)
        else:
            logger.warning(
                "column_aliases returned identity for %s — AI may have failed; not caching",
                dataset,
            )
        return ColumnAliasResponse(dataset=dataset, aliases=aliases, cached=False)
    except Exception as e:
        logger.exception("column_aliases failed | dataset=%s | error=%s", dataset, e)
        identity = {c: c for c in columns}
        return ColumnAliasResponse(dataset=dataset, aliases=identity, cached=False)


@router.post("/column_aliases")
def save_column_aliases(payload: UpdateColumnAliasRequest) -> dict:
    """Persist user-edited column aliases back to the cache."""
    try:
        _write_aliases_cache(payload.dataset, payload.aliases)
        return {"status": "ok"}
    except Exception as e:
        logger.exception("save_column_aliases failed | dataset=%s | error=%s", payload.dataset, e)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")


# ============================================================
# ANALYSIS SEQUENCE — cache helpers + endpoint
# ============================================================

def _read_sequence_cache(dataset: str) -> list[str] | None:
    """Return cached analysis sequence (list of 3 strings), or None."""
    try:
        cache = json.loads(_suggestions_cache_path(dataset).read_text(encoding="utf-8"))
        steps = cache.get("analysis_sequence")
        if isinstance(steps, list) and len(steps) >= 3:
            return steps[:3]
    except Exception:
        pass
    return None


def _write_sequence_cache(dataset: str, steps: list[str]) -> None:
    try:
        cache_path = _suggestions_cache_path(dataset)
        existing: dict = {}
        try:
            existing = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            pass
        existing["analysis_sequence"] = steps
        cache_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("Could not write sequence cache for %s: %s", dataset, exc)


@router.get("/analysis_sequence", response_model=AnalysisSequenceResponse)
def get_analysis_sequence(
    dataset: str,
    refresh: bool = Query(False, description="Force regeneration, ignoring cache"),
) -> AnalysisSequenceResponse:
    """Return a 3-step analytical sequence for the dataset.

    Cache hit: returns instantly from dataset_context.json.
    Cache miss or refresh=true: calls the AI, caches, and returns.
    """
    if not refresh:
        cached = _read_sequence_cache(dataset)
        if cached is not None:
            return AnalysisSequenceResponse(dataset=dataset, steps=cached, cached=True)

    columns = _get_dataset_columns(dataset)
    if not columns:
        return AnalysisSequenceResponse(dataset=dataset, steps=[], cached=False)

    # Use insights synopsis as additional context if available
    synopsis = ""
    try:
        ctx = json.loads(_suggestions_cache_path(dataset).read_text(encoding="utf-8"))
        synopsis = ctx.get("insights_synopsis") or ctx.get("grain_description") or ""
    except Exception:
        pass

    try:
        steps = generate_analysis_sequence(
            dataset_name=dataset,
            columns=columns,
            synopsis=synopsis,
        )
        if steps:
            _write_sequence_cache(dataset, steps)
        return AnalysisSequenceResponse(dataset=dataset, steps=steps, cached=False)
    except Exception as e:
        logger.exception("analysis_sequence failed | dataset=%s | error=%s", dataset, e)
        return AnalysisSequenceResponse(dataset=dataset, steps=[], cached=False)