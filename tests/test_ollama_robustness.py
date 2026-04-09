"""
test_ollama_robustness.py — Ollama error handling and graceful failure tests

These tests do NOT require Ollama to be running. All Ollama interactions
are mocked. Tests verify error handling, provider routing, and graceful
failure across all AI endpoints.

Run from project root:
    PYTHONPATH=backend python -m pytest tests/test_ollama_robustness.py -v
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.key_manager import set_ai_mode, get_ai_mode, save_key
from app.services.session_log import (
    _reset_session,
    start_session,
    SessionEventType,
)


# ---------------------------------------------------------------------------
# FIXTURES
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _isolated_config(tmp_path, monkeypatch):
    """Redirect config.enc to a temp directory."""
    fake_appdata = str(tmp_path / "appdata")
    monkeypatch.setenv("APPDATA", fake_appdata)


@pytest.fixture(autouse=True)
def _clean_session():
    """Ensure a clean session for each test."""
    import app.services.session_log as _sl
    prev_session = _sl._current_session
    prev_dir = _sl._sessions_dir
    _reset_session()
    yield
    _sl._current_session = prev_session
    _sl._sessions_dir = prev_dir


@pytest.fixture
def client():
    with TestClient(main_module.app) as c:
        yield c


# ---------------------------------------------------------------------------
# Test 1 — Ollama not running returns 503
# ---------------------------------------------------------------------------

class TestOllamaNotRunning:
    def test_generate_sql_returns_503_when_ollama_down(self, client):
        """HTTP 503 with clear message when Ollama is not reachable."""
        set_ai_mode("local")
        start_session()
        with patch("app.ai.routes._get_ai_mode", return_value="local"), \
             patch("app.ai.provider_ollama.check_ollama_available", return_value=False):
            resp = client.post("/api/ai/generate_sql", json={
                "dataset": "test", "question": "count rows"
            })
            assert resp.status_code == 503
            assert "Ollama" in resp.json()["detail"]

    def test_no_500_or_hang(self, client):
        """Must not return 500 or hang — clean 503."""
        set_ai_mode("local")
        start_session()
        with patch("app.ai.routes._get_ai_mode", return_value="local"), \
             patch("app.ai.provider_ollama.check_ollama_available", return_value=False):
            resp = client.post("/api/ai/generate_sql", json={
                "dataset": "test", "question": "anything"
            })
            assert resp.status_code != 500
            assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Test 2 — Model not found returns clear error
# ---------------------------------------------------------------------------

class TestModelNotFound:
    def test_model_not_found_surfaces_readable_error(self):
        """When Ollama returns 'model not found', error is readable."""
        from app.ai.provider_ollama import generate_response

        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.text = '{"error": "model \'llama3.2\' not found, try pulling it first"}'

        with patch("app.ai.provider_ollama.requests") as mock_requests:
            mock_requests.post.return_value = mock_resp
            mock_requests.ConnectionError = ConnectionError
            mock_requests.Timeout = TimeoutError
            with pytest.raises(ConnectionError, match="Ollama error"):
                generate_response("test prompt")


# ---------------------------------------------------------------------------
# Test 3 — Malformed JSON from Ollama handled gracefully
# ---------------------------------------------------------------------------

class TestMalformedResponse:
    def test_plain_text_response_does_not_crash(self):
        """If Ollama returns plain text instead of JSON, parsing fails gracefully."""
        from app.ai.provider_openai import generate_sql_response
        from app.ai.response_parser import parse_generate_sql_response

        set_ai_mode("local")

        plain_text = "I'm sorry, I can't generate SQL for that question."
        with patch("app.ai.provider_ollama.generate_response", return_value=plain_text):
            raw = generate_sql_response("test prompt")
            # The response parser should handle non-JSON gracefully
            parsed = parse_generate_sql_response(raw)
            assert parsed["status"] == "error"

    def test_partial_json_does_not_crash(self):
        """Partial JSON from Ollama doesn't cause a traceback."""
        from app.ai.provider_openai import generate_sql_response
        from app.ai.response_parser import parse_generate_sql_response

        set_ai_mode("local")

        partial = '{"status": "ok", "sql": "SELECT'
        with patch("app.ai.provider_ollama.generate_response", return_value=partial):
            raw = generate_sql_response("test prompt")
            parsed = parse_generate_sql_response(raw)
            # Should return error status, not crash
            assert parsed["status"] == "error"


# ---------------------------------------------------------------------------
# Test 4 — Markdown code fences cleaned
# ---------------------------------------------------------------------------

class TestCodeFenceCleaning:
    def test_json_in_code_fences_parses_correctly(self):
        """Ollama often wraps JSON in ```json ... ``` — verify cleaning works."""
        from app.ai.provider_openai import generate_sql_response
        from app.ai.response_parser import parse_generate_sql_response

        set_ai_mode("local")

        fenced = '```json\n{"status":"ok","sql":"SELECT COUNT(*) FROM dataset","message":"counts rows","warnings":[]}\n```'
        with patch("app.ai.provider_ollama.generate_response", return_value=fenced):
            raw = generate_sql_response("test prompt")
            parsed = parse_generate_sql_response(raw)
            assert parsed["status"] == "ok"
            assert "SELECT COUNT" in parsed["sql"]


# ---------------------------------------------------------------------------
# Test 5 — Switching local→cloud mid-session works
# ---------------------------------------------------------------------------

class TestMidSessionSwitch:
    def test_switch_local_to_cloud_routes_correctly(self):
        """After switching from local to cloud, calls route to OpenAI."""
        from app.ai.provider_openai import generate_sql_response

        set_ai_mode("local")
        with patch("app.ai.provider_ollama.generate_response", return_value="local result"):
            result1 = generate_sql_response("prompt1")
            assert result1 == "local result"

        set_ai_mode("cloud")
        save_key("sk-test-key")
        with patch("app.ai.provider_openai._call_openai", return_value="cloud result"):
            result2 = generate_sql_response("prompt2")
            assert result2 == "cloud result"


# ---------------------------------------------------------------------------
# Test 6 — BUG-012 regression: suggest_questions routes to Ollama
# ---------------------------------------------------------------------------

class TestBug012Regression:
    def test_suggest_questions_routes_to_ollama_in_local_mode(self):
        """BUG-012: suggest_questions calls generate_sql_response which must
        route to Ollama in local mode. We test the dispatch layer directly
        since suggest_questions_for_dataset requires a real dataset."""
        set_ai_mode("local")

        from app.ai.provider_openai import generate_sql_response
        with patch("app.ai.provider_ollama.generate_response", return_value="ollama called") as mock_ollama, \
             patch("app.ai.provider_openai._call_openai") as mock_openai:
            result = generate_sql_response("suggest questions prompt")
            assert mock_ollama.called, "Ollama provider was NOT called — dispatch bypassed local mode"
            assert not mock_openai.called, "OpenAI was called despite local mode being active"
            assert result == "ollama called"


# ---------------------------------------------------------------------------
# Test 7 — All 8 AI functions route to correct provider
# ---------------------------------------------------------------------------

class TestAllEndpointsRoute:
    """Verify generate_sql_response dispatches correctly for both modes.

    Since all 8 AI functions call generate_sql_response() as their single
    chokepoint, testing the dispatch function covers all endpoints.
    """

    def test_cloud_mode_calls_openai(self):
        set_ai_mode("cloud")
        save_key("sk-test")
        from app.ai.provider_openai import generate_sql_response

        with patch("app.ai.provider_openai._call_openai", return_value="openai result") as mock:
            result = generate_sql_response("test")
            assert mock.called
            assert result == "openai result"

    def test_local_mode_calls_ollama(self):
        set_ai_mode("local")
        from app.ai.provider_openai import generate_sql_response

        with patch("app.ai.provider_ollama.generate_response", return_value="ollama result") as mock:
            result = generate_sql_response("test")
            assert mock.called
            assert result == "ollama result"

    def test_all_high_level_functions_use_chokepoint(self):
        """Verify that all 8 high-level functions call generate_sql_response."""
        from app.ai import provider_openai as po
        import inspect

        # These are the 8 functions that call generate_sql_response
        callers = [
            po.generate_sql_for_dataset,
            po.suggest_questions_for_dataset,
            po.generate_insights_for_dataset,
            po.generate_explanation,
            po.generate_result_narrative,
            po.generate_column_aliases,
            po.generate_analysis_sequence,
            po.generate_analysis_summary,
        ]

        for fn in callers:
            src = inspect.getsource(fn)
            assert "generate_sql_response" in src or "generate_response" in src, \
                f"{fn.__name__} does not call generate_sql_response — may bypass provider routing"
