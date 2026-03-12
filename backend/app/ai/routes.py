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

from fastapi import APIRouter

from .provider_openai import (
    generate_sql_for_dataset,
    suggest_questions_for_dataset,
)

from .response_parser import parse_generate_sql_response

from .schemas import (
    GenerateSQLRequest,
    GenerateSQLResponse,
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
def suggest_questions(dataset: str, max_questions: int = 8):
    """
    Return AI-generated suggested questions for the dataset.

    Parameters
    ----------
    dataset : str
        Dataset name

    max_questions : int
        Maximum number of suggested questions
    """

    try:

        # ----------------------------------------------------
        # STEP 1 — Ask the provider layer for suggestions
        # ----------------------------------------------------
        questions = suggest_questions_for_dataset(
            dataset_name=dataset,
            dataset_source_path_fn=_get_dataset_source_path,
            max_questions=max_questions,
        )

        # ----------------------------------------------------
        # STEP 2 — Return structured response
        # ----------------------------------------------------
        return {
            "dataset": dataset,
            "questions": questions,
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
        # STEP 1 — Generate SQL using the AI provider
        # ----------------------------------------------------
        model_output = generate_sql_for_dataset(
            dataset_name=payload.dataset,
            question=payload.question,
            dataset_source_path_fn=_get_dataset_source_path,
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
        # ----------------------------------------------------
        return GenerateSQLResponse(
            status="error",
            dataset=payload.dataset,
            question=payload.question,
            sql="",
            message=f"DEBUG ERROR: {type(e).__name__}: {e}",
            warnings=[],
        )