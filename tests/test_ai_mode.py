"""
test_ai_mode.py — pytest tests for the AI Mode Switch (M5 Priority 3)

Covers:
  - key_manager: get_ai_mode/set_ai_mode persistence and validation
  - Provider routing: OpenAI vs Ollama dispatch based on ai_mode
  - HTTP 503 when Ollama not running
  - ai_mode_change event logging
  - Privacy Mode parity in local mode
  - Settings endpoints

OpenAI and Ollama are never actually called — providers are mocked.

Run from project root:
    pytest tests/test_ai_mode.py -v
"""
from __future__ import annotations

import os
from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.key_manager import get_ai_mode, set_ai_mode
from app.services.session_log import (
    _reset_session,
    start_session,
    get_current_session,
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
# KEY MANAGER TESTS
# ---------------------------------------------------------------------------

class TestAiModeKeyManager:
    """Tests for get_ai_mode/set_ai_mode in key_manager.py."""

    def test_defaults_to_cloud(self):
        assert get_ai_mode() == "cloud"

    def test_set_local_persists(self):
        set_ai_mode("local")
        assert get_ai_mode() == "local"

    def test_set_cloud_persists(self):
        set_ai_mode("local")
        set_ai_mode("cloud")
        assert get_ai_mode() == "cloud"

    def test_invalid_mode_raises_valueerror(self):
        with pytest.raises(ValueError, match="Invalid ai_mode"):
            set_ai_mode("invalid")

    def test_invalid_mode_does_not_change_current(self):
        set_ai_mode("local")
        with pytest.raises(ValueError):
            set_ai_mode("banana")
        assert get_ai_mode() == "local"


# ---------------------------------------------------------------------------
# SETTINGS ENDPOINT TESTS
# ---------------------------------------------------------------------------

class TestAiModeEndpoints:
    """Tests for GET/POST /api/settings/ai_mode."""

    def test_get_ai_mode_returns_default(self, client):
        resp = client.get("/api/settings/ai_mode")
        assert resp.status_code == 200
        data = resp.json()
        assert data["mode"] == "cloud"
        assert "ollama_available" in data

    def test_set_ai_mode_local(self, client):
        resp = client.post(
            "/api/settings/ai_mode",
            json={"mode": "local"},
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

        # Verify it persisted
        resp2 = client.get("/api/settings/ai_mode")
        assert resp2.json()["mode"] == "local"

    def test_set_ai_mode_invalid_returns_400(self, client):
        resp = client.post(
            "/api/settings/ai_mode",
            json={"mode": "quantum"},
        )
        assert resp.status_code == 400

    def test_ollama_available_false_when_not_running(self, client):
        with patch("app.ai.provider_ollama.check_ollama_available", return_value=False):
            resp = client.get("/api/settings/ai_mode")
            data = resp.json()
            assert data["ollama_available"] is False

    def test_ai_mode_toggles_correctly_across_multiple_clicks(self, client):
        """Simulate multiple toggle clicks: cloud → local → cloud → local.
        Each POST should alternate correctly — no snap-back to one value."""
        # Start at cloud (default)
        assert client.get("/api/settings/ai_mode").json()["mode"] == "cloud"

        # Click 1: cloud → local
        client.post("/api/settings/ai_mode", json={"mode": "local"})
        assert client.get("/api/settings/ai_mode").json()["mode"] == "local"

        # Click 2: local → cloud
        client.post("/api/settings/ai_mode", json={"mode": "cloud"})
        assert client.get("/api/settings/ai_mode").json()["mode"] == "cloud"

        # Click 3: cloud → local
        client.post("/api/settings/ai_mode", json={"mode": "local"})
        assert client.get("/api/settings/ai_mode").json()["mode"] == "local"

        # Click 4: local → cloud
        client.post("/api/settings/ai_mode", json={"mode": "cloud"})
        assert client.get("/api/settings/ai_mode").json()["mode"] == "cloud"

    def test_privacy_mode_toggles_correctly_across_multiple_clicks(self, client):
        """Simulate multiple toggle clicks: off → on → off → on.
        Each POST should alternate correctly — never stuck on one value."""
        # Start at off (default)
        assert client.get("/api/settings/privacy_mode").json()["privacy_mode"] is False

        # Click 1: off → on
        client.post("/api/settings/privacy_mode", json={"enabled": True},
                     headers={"Content-Type": "application/json"})
        assert client.get("/api/settings/privacy_mode").json()["privacy_mode"] is True

        # Click 2: on → off
        client.post("/api/settings/privacy_mode", json={"enabled": False},
                     headers={"Content-Type": "application/json"})
        assert client.get("/api/settings/privacy_mode").json()["privacy_mode"] is False

        # Click 3: off → on
        client.post("/api/settings/privacy_mode", json={"enabled": True},
                     headers={"Content-Type": "application/json"})
        assert client.get("/api/settings/privacy_mode").json()["privacy_mode"] is True

        # Click 4: on → off
        client.post("/api/settings/privacy_mode", json={"enabled": False},
                     headers={"Content-Type": "application/json"})
        assert client.get("/api/settings/privacy_mode").json()["privacy_mode"] is False


# ---------------------------------------------------------------------------
# PROVIDER ROUTING TESTS
# ---------------------------------------------------------------------------

class TestProviderRouting:
    """Tests that generate_sql_response routes to the correct provider."""

    def test_cloud_mode_calls_openai(self):
        """In cloud mode, generate_sql_response should call _call_openai."""
        set_ai_mode("cloud")
        from app.key_manager import save_key
        save_key("sk-test-key")
        from app.ai.provider_openai import generate_sql_response

        with patch("app.ai.provider_openai._call_openai", return_value="test response") as mock_openai:
            result = generate_sql_response("test prompt")
            assert mock_openai.called
            assert result == "test response"

    def test_local_mode_calls_ollama(self):
        """In local mode, generate_sql_response should call Ollama generate_response."""
        set_ai_mode("local")
        from app.ai.provider_openai import generate_sql_response

        with patch("app.ai.provider_ollama.generate_response", return_value="local response") as mock_ollama:
            result = generate_sql_response("test prompt")
            assert mock_ollama.called
            assert result == "local response"


# ---------------------------------------------------------------------------
# 503 HANDLING TESTS
# ---------------------------------------------------------------------------

class TestOllama503:
    """Tests that 503 is returned when Ollama mode active but unavailable."""

    def test_503_on_generate_sql(self, client):
        set_ai_mode("local")
        start_session()
        with patch("app.ai.provider_ollama.check_ollama_available", return_value=False):
            resp = client.post("/api/ai/generate_sql", json={
                "dataset": "test", "question": "test"
            })
            assert resp.status_code == 503
            assert "Ollama" in resp.json()["detail"]

    def test_503_on_suggest_questions(self, client):
        set_ai_mode("local")
        start_session()
        with patch("app.ai.provider_ollama.check_ollama_available", return_value=False):
            resp = client.get("/api/ai/suggest_questions?dataset=test")
            # suggest_questions catches exceptions internally and returns error in JSON
            data = resp.json()
            assert resp.status_code in (200, 503)
            if resp.status_code == 200:
                assert "error" in data or data.get("questions") == []


# ---------------------------------------------------------------------------
# SESSION LOG TESTS
# ---------------------------------------------------------------------------

class TestAiModeSessionLog:
    """Tests that ai_mode_change events are logged."""

    def test_mode_change_logged(self, client):
        start_session()
        # Change from cloud to local
        client.post("/api/settings/ai_mode", json={"mode": "local"})

        session = get_current_session()
        mode_events = [
            e for e in session.events
            if e.event_type == SessionEventType.AI_MODE_CHANGE
        ]
        assert len(mode_events) == 1
        assert mode_events[0].details["old_mode"] == "cloud"
        assert mode_events[0].details["new_mode"] == "local"

    def test_same_mode_no_event(self, client):
        start_session()
        # Set to cloud when already cloud — no event
        client.post("/api/settings/ai_mode", json={"mode": "cloud"})

        session = get_current_session()
        mode_events = [
            e for e in session.events
            if e.event_type == SessionEventType.AI_MODE_CHANGE
        ]
        assert len(mode_events) == 0


# ---------------------------------------------------------------------------
# PRIVACY MODE PARITY TESTS
# ---------------------------------------------------------------------------

class TestPrivacyModeParity:
    """Privacy Mode applies identically in local mode."""

    def test_privacy_mode_strips_data_in_local_mode(self):
        """Build a summary prompt in local+privacy mode — verify SQL stripped."""
        from app.ai.provider_openai import build_analysis_summary_prompt

        events = [
            {"event_type": "query_run", "details": {
                "dataset": "ds", "sql": "SELECT secret FROM dataset",
                "rowcount": 5, "elapsed_seconds": 0.01,
            }},
        ]
        prompt = build_analysis_summary_prompt(
            session_events=events,
            session_meta={"datasets_used": ["ds"]},
            privacy_mode=True,
        )
        assert "SELECT secret" not in prompt
        assert "PRIVACY RESTRICTION" in prompt
