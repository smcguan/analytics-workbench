from __future__ import annotations

"""
============================================================
FILE: response_parser.py
LOCATION: backend/app/ai/response_parser.py
============================================================

PURPOSE
-------
This module cleans and parses the raw text returned by the
AI model after SQL generation.

The AI is instructed to return JSON, but language models do
not always follow instructions perfectly. They may return:

- clean JSON
- JSON wrapped in markdown code fences
- extra explanatory text before or after the JSON
- malformed output

This file makes the system more resilient by:

1. stripping markdown fences if present
2. extracting the JSON object from surrounding text
3. parsing the JSON safely
4. normalizing the response into a predictable structure

WHY THIS FILE EXISTS
--------------------
The rest of the AI pipeline expects a clean structure like:

{
  "status": "ok",
  "sql": "SELECT ...",
  "message": "...",
  "warnings": []
}

If we let raw model text flow directly into validation and
execution, the system would be fragile and error-prone.

This parser acts as a cleanup and normalization layer between:

    OpenAI raw output
            ↓
    response_parser.py
            ↓
    SQL validator / route response

AI PIPELINE POSITION
--------------------

Natural language question
        ↓
OpenAI model
        ↓
raw text response
        ↓
parse_generate_sql_response(...)   <-- this file
        ↓
normalized dict
        ↓
SQL validation
        ↓
API response

DESIGN PRINCIPLE
----------------
This parser should fail safely.

If the model returns invalid output, we do not want the
system to crash. Instead, we convert the failure into a
structured error response that the rest of the system can
handle cleanly.

============================================================
"""

import json
from typing import Any


# ============================================================
# HELPER — STRIP MARKDOWN CODE FENCES
# ------------------------------------------------------------
# Language models often wrap JSON in markdown like:
#
# ```json
# { ... }
# ```
#
# or
#
# ```
# { ... }
# ```
#
# This helper removes the outer code fences so the JSON
# parser sees only the JSON content.
# ============================================================
def _strip_code_fences(text: str) -> str:
    """
    Remove outer triple-backtick code fences if present.

    Example:
        ```json
        {"status":"ok", ...}
        ```

    becomes:
        {"status":"ok", ...}
    """
    s = (text or "").strip()

    if s.startswith("```"):
        lines = s.splitlines()

        # Remove opening fence line
        if lines and lines[0].startswith("```"):
            lines = lines[1:]

        # Remove closing fence line
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]

        s = "\n".join(lines).strip()

    return s


# ============================================================
# HELPER — EXTRACT JSON OBJECT
# ------------------------------------------------------------
# Even after stripping markdown, the model may still return
# extra text before or after the JSON object.
#
# Example:
#   "Here is the SQL:\n{...}"
#
# This helper finds the first '{' and the last '}' and
# returns the substring in between.
#
# This is intentionally simple because the expected response
# format is a single JSON object.
# ============================================================
def _extract_json_object(text: str) -> str:
    """
    Extract the first full JSON object found in the text.

    Raises:
        ValueError if no valid JSON object boundaries are found.
    """
    s = _strip_code_fences(text)

    start = s.find("{")
    end = s.rfind("}")

    if start == -1 or end == -1 or end < start:
        raise ValueError("No JSON object found in model response.")

    return s[start:end + 1]


# ============================================================
# MAIN PARSER
# ------------------------------------------------------------
# This is the main function used by the AI routes.
#
# It:
# 1. extracts the JSON object
# 2. parses JSON text into Python data
# 3. normalizes fields
# 4. returns a predictable dict structure
#
# IMPORTANT:
# This function always returns a dictionary with:
#   status
#   sql
#   message
#   warnings
#
# Even if parsing fails.
# ============================================================
def parse_generate_sql_response(raw_text: str) -> dict[str, Any]:
    """
    Parse and normalize the AI model's SQL-generation response.

    Expected model shape:
    {
      "status": "ok" or "error",
      "sql": "...",
      "message": "...",
      "warnings": []
    }

    Returned dict is always normalized to:
    {
      "status": "ok" or "error",
      "sql": str,
      "message": str,
      "warnings": list[str],
    }
    """
    # --------------------------------------------------------
    # STEP 1 — Try to extract and parse JSON
    # --------------------------------------------------------
    # If anything goes wrong here, we convert the failure into
    # a structured error result instead of throwing.
    #
    try:
        json_text = _extract_json_object(raw_text)
        data = json.loads(json_text)

    except Exception as e:
        return {
            "status": "error",
            "sql": "",
            "message": f"AI returned an invalid response format: {e}",
            "warnings": [],
        }

    # --------------------------------------------------------
    # STEP 2 — Normalize status
    # --------------------------------------------------------
    # Only "ok" and "error" are allowed system states.
    # Anything else is coerced to "error".
    #
    status = str(data.get("status", "error")).strip().lower()
    if status not in {"ok", "error"}:
        status = "error"

    # --------------------------------------------------------
    # STEP 3 — Normalize SQL and message fields
    # --------------------------------------------------------
    # Ensure these are always strings, even if the model
    # returns unexpected types.
    #
    sql = str(data.get("sql", "") or "").strip()
    message = str(data.get("message", "") or "").strip()

    # --------------------------------------------------------
    # STEP 4 — Normalize warnings
    # --------------------------------------------------------
    # warnings should ideally be a list, but the model may
    # return a string or another value type.
    #
    warnings = data.get("warnings", [])

    if not isinstance(warnings, list):
        warnings = [str(warnings)]

    warnings = [str(w).strip() for w in warnings if str(w).strip()]

    # --------------------------------------------------------
    # STEP 5 — Safety consistency check
    # --------------------------------------------------------
    # If the model says status="ok" but provides no SQL,
    # that is not really a success.
    #
    if status == "ok" and not sql:
        status = "error"
        message = message or "AI reported success but did not return SQL."

    # --------------------------------------------------------
    # STEP 6 — Provide default message if missing
    # --------------------------------------------------------
    # This guarantees the UI and routes always have something
    # meaningful to display.
    #
    if not message:
        message = "SQL generated successfully." if status == "ok" else "SQL generation failed."

    # --------------------------------------------------------
    # STEP 7 — Return normalized response object
    # --------------------------------------------------------
    return {
        "status": status,
        "sql": sql,
        "message": message,
        "warnings": warnings,
    }