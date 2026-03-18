"""
test_response_parser.py — unit tests for response_parser.parse_generate_sql_response

Covers:
  Clean JSON parsing
  Markdown code fence stripping
  Extra text around JSON
  Malformed / missing JSON
  Field normalization (status, sql, message, warnings)
  Safety: status=ok with empty SQL forced to error

Run from project root:
    pytest tests/test_response_parser.py -v
"""
from __future__ import annotations

import json

import pytest

from app.ai.response_parser import parse_generate_sql_response


# ===========================================================================
# CLEAN JSON — happy path
# ===========================================================================

def test_clean_json_ok():
    raw = json.dumps({
        "status": "ok",
        "sql": "SELECT * FROM dataset LIMIT 10",
        "message": "Here are the first 10 rows.",
        "warnings": [],
    })
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"
    assert result["sql"] == "SELECT * FROM dataset LIMIT 10"
    assert result["message"] == "Here are the first 10 rows."
    assert result["warnings"] == []


def test_clean_json_error():
    raw = json.dumps({
        "status": "error",
        "sql": "",
        "message": "I could not generate SQL for that question.",
        "warnings": [],
    })
    result = parse_generate_sql_response(raw)
    assert result["status"] == "error"
    assert result["sql"] == ""


# ===========================================================================
# MARKDOWN CODE FENCES
# ===========================================================================

def test_json_wrapped_in_code_fence():
    raw = '```json\n{"status":"ok","sql":"SELECT 1","message":"ok","warnings":[]}\n```'
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"
    assert result["sql"] == "SELECT 1"


def test_json_wrapped_in_plain_code_fence():
    raw = '```\n{"status":"ok","sql":"SELECT 1","message":"done","warnings":[]}\n```'
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"
    assert result["sql"] == "SELECT 1"


def test_nested_code_fence_with_language_tag():
    raw = '```JSON\n{"status":"ok","sql":"SELECT * FROM dataset","message":"","warnings":[]}\n```'
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"


# ===========================================================================
# EXTRA TEXT AROUND JSON
# ===========================================================================

def test_text_before_json():
    raw = 'Here is your SQL query:\n{"status":"ok","sql":"SELECT 1","message":"done","warnings":[]}'
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"
    assert result["sql"] == "SELECT 1"


def test_text_after_json():
    raw = '{"status":"ok","sql":"SELECT 1","message":"done","warnings":[]}\nI hope this helps!'
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"
    assert result["sql"] == "SELECT 1"


def test_text_both_sides():
    raw = 'Sure!\n{"status":"ok","sql":"SELECT 1","message":"","warnings":[]}\nLet me know.'
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"


# ===========================================================================
# MALFORMED / MISSING JSON
# ===========================================================================

def test_empty_string_returns_error():
    result = parse_generate_sql_response("")
    assert result["status"] == "error"
    assert result["sql"] == ""
    assert result["warnings"] == []


def test_none_input_returns_error():
    result = parse_generate_sql_response(None)
    assert result["status"] == "error"
    assert result["sql"] == ""


def test_plain_text_no_json_returns_error():
    result = parse_generate_sql_response("I cannot help with that request.")
    assert result["status"] == "error"
    assert result["sql"] == ""


def test_invalid_json_returns_error():
    result = parse_generate_sql_response("{this is not valid json!!!}")
    assert result["status"] == "error"
    assert result["sql"] == ""


def test_json_array_instead_of_object():
    """AI returns a JSON array instead of an object."""
    raw = '[{"sql": "SELECT 1"}]'
    # _extract_json_object uses first { and last }, so it should
    # extract the inner object. But json.loads on the extracted
    # substring may produce unexpected results.
    result = parse_generate_sql_response(raw)
    # Should not crash — returns error or parses partial
    assert result["status"] in ("ok", "error")
    assert isinstance(result["sql"], str)


# ===========================================================================
# FIELD NORMALIZATION
# ===========================================================================

def test_missing_status_defaults_to_error():
    raw = json.dumps({"sql": "SELECT 1", "message": "ok"})
    result = parse_generate_sql_response(raw)
    # status missing → defaults to "error", but sql is present
    # Actually: status defaults to "error", but sql exists
    assert result["status"] == "error"


def test_unknown_status_coerced_to_error():
    raw = json.dumps({"status": "maybe", "sql": "SELECT 1", "message": "unsure"})
    result = parse_generate_sql_response(raw)
    assert result["status"] == "error"


def test_status_ok_uppercase_normalized():
    raw = json.dumps({"status": "OK", "sql": "SELECT 1", "message": "done"})
    result = parse_generate_sql_response(raw)
    assert result["status"] == "ok"


def test_sql_field_none_becomes_empty_string():
    raw = json.dumps({"status": "error", "sql": None, "message": "failed"})
    result = parse_generate_sql_response(raw)
    assert result["sql"] == ""


def test_message_field_none_gets_default():
    raw = json.dumps({"status": "ok", "sql": "SELECT 1", "message": None})
    result = parse_generate_sql_response(raw)
    assert result["message"]  # non-empty


def test_warnings_string_becomes_list():
    raw = json.dumps({
        "status": "ok", "sql": "SELECT 1",
        "message": "done", "warnings": "watch out",
    })
    result = parse_generate_sql_response(raw)
    assert isinstance(result["warnings"], list)
    assert "watch out" in result["warnings"]


def test_warnings_missing_becomes_empty_list():
    raw = json.dumps({"status": "ok", "sql": "SELECT 1", "message": "done"})
    result = parse_generate_sql_response(raw)
    assert result["warnings"] == []


def test_warnings_empty_strings_filtered():
    raw = json.dumps({
        "status": "ok", "sql": "SELECT 1",
        "message": "done", "warnings": ["real warning", "", "  "],
    })
    result = parse_generate_sql_response(raw)
    assert result["warnings"] == ["real warning"]


# ===========================================================================
# SAFETY: status=ok but no SQL
# ===========================================================================

def test_ok_with_empty_sql_forced_to_error():
    raw = json.dumps({"status": "ok", "sql": "", "message": "success"})
    result = parse_generate_sql_response(raw)
    assert result["status"] == "error"
    assert result["sql"] == ""


def test_ok_with_whitespace_sql_forced_to_error():
    raw = json.dumps({"status": "ok", "sql": "   ", "message": "success"})
    result = parse_generate_sql_response(raw)
    assert result["status"] == "error"


def test_ok_with_missing_sql_forced_to_error():
    raw = json.dumps({"status": "ok", "message": "I generated the query."})
    result = parse_generate_sql_response(raw)
    assert result["status"] == "error"


# ===========================================================================
# DEFAULT MESSAGES
# ===========================================================================

def test_default_message_on_ok():
    raw = json.dumps({"status": "ok", "sql": "SELECT 1"})
    result = parse_generate_sql_response(raw)
    assert result["message"]  # non-empty default


def test_default_message_on_error():
    raw = json.dumps({"status": "error"})
    result = parse_generate_sql_response(raw)
    assert result["message"]  # non-empty default


# ===========================================================================
# RESPONSE STRUCTURE GUARANTEE
# ===========================================================================

def test_response_always_has_four_keys():
    """Every return path must include status, sql, message, warnings."""
    inputs = [
        "",
        None,
        "random text",
        '{"status":"ok","sql":"SELECT 1","message":"ok","warnings":[]}',
        "```json\n{}\n```",
    ]
    for raw in inputs:
        result = parse_generate_sql_response(raw)
        for key in ("status", "sql", "message", "warnings"):
            assert key in result, f"Missing key '{key}' for input: {raw!r}"
        assert isinstance(result["status"], str)
        assert isinstance(result["sql"], str)
        assert isinstance(result["message"], str)
        assert isinstance(result["warnings"], list)
