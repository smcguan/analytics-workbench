"""
============================================================
FILE: schemas.py
LOCATION: backend/app/ai/schemas.py
============================================================

PURPOSE
-------
This module defines the API request and response schemas used
by the AI endpoints.

FastAPI uses Pydantic models to:

• Validate incoming request data
• Guarantee consistent response structure
• Generate automatic API documentation (Swagger)

These schemas are used by the two AI endpoints:

    POST /api/ai/generate_sql
    POST /api/ai/run_query


PIPELINE OVERVIEW
-----------------

User Question
      ↓
GenerateSQLRequest
      ↓
AI SQL generation
      ↓
GenerateSQLResponse
      ↓
(optional execution step)
      ↓
RunQueryResponse


WHY THIS FILE EXISTS
--------------------

Separating schemas into their own module keeps the system
clean and predictable.

Instead of constructing JSON responses manually throughout
the code, the API always returns objects that match these
schemas.

This ensures:

• consistent API structure
• automatic validation
• better documentation
• fewer runtime bugs

============================================================
"""

from typing import List, Literal
from pydantic import BaseModel


# ============================================================
# REQUEST MODEL
# ============================================================
# Used when the frontend asks the AI to generate SQL or
# execute a natural language query.
#
# Example request body:
#
# {
#   "dataset": "demo",
#   "question": "show the first 10 rows"
# }
# ============================================================

class GenerateSQLRequest(BaseModel):

    # Name of the dataset selected by the user.
    # This is NOT used as a SQL table name.
    dataset: str

    # Natural language question the user asks the AI.
    question: str


# ============================================================
# RESPONSE MODEL — SQL GENERATION
# ============================================================
# Returned by:
#
#     POST /api/ai/generate_sql
#
# This endpoint generates SQL but does NOT execute it.
#
# Example response:
#
# {
#   "status": "ok",
#   "dataset": "demo",
#   "question": "...",
#   "sql": "SELECT * FROM dataset LIMIT 10",
#   "message": "Returns the first 10 rows.",
#   "warnings": []
# }
# ============================================================

class GenerateSQLResponse(BaseModel):

    # Whether the AI successfully generated SQL
    status: Literal["ok", "error"]

    # Dataset name from the request
    dataset: str

    # Original user question
    question: str

    # Generated SQL query
    sql: str = ""

    # Human-readable explanation of the query
    message: str

    # Optional warnings (for example partial assumptions)
    warnings: List[str] = []


# ============================================================
# RESPONSE MODEL — QUERY EXECUTION
# ============================================================
# Returned by:
#
#     POST /api/ai/run_query
#
# This extends GenerateSQLResponse and adds query results.
#
# Example response:
#
# {
#   "status": "ok",
#   "dataset": "demo",
#   "question": "...",
#   "sql": "SELECT * FROM dataset LIMIT 10",
#   "columns": ["id","name","revenue"],
#   "rows": [
#       {"id":1,"name":"A","revenue":10},
#       {"id":2,"name":"B","revenue":20}
#   ],
#   "rowcount": 120
# }
# ============================================================

class RunQueryResponse(GenerateSQLResponse):

    # Column names returned by the query
    columns: list[str] = []

    # Preview rows returned from DuckDB
    rows: list[dict] = []

    # Total number of rows in the full result set
    # (not just the preview rows)
    rowcount: int | None = None