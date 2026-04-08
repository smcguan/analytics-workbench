"""
test_analysis_summary.py — pytest tests for the Analysis Summary Artifact

Covers:
  - build_analysis_summary_prompt: event formatting, privacy mode
  - parse_analysis_summary: section extraction from AI output
  - POST /api/session/analysis_summary: endpoint behavior

OpenAI is never called — the provider is mocked.

Run from project root:
    pytest tests/test_analysis_summary.py -v
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.ai.provider_openai import (
    build_analysis_summary_prompt,
    parse_analysis_summary,
)
from app.services.session_log import (
    _reset_session,
    start_session,
    log_event,
    SessionEventType,
    get_current_session,
)


# ---------------------------------------------------------------------------
# FIXTURES
# ---------------------------------------------------------------------------

MOCK_AI_RESPONSE = """## Findings

- **TX Medicaid** has the highest claims volume at 5,000 rows
- OH reimbursement rates are 16 percentage points below TX and FL
- Lone Star Health Plan accounts for 47.8% of TX managed care claims

## Methodology

Three state Medicaid datasets were loaded (TX, FL, OH) totaling 13,000 rows. A schema mapping reference table was joined to normalize column names across states. MCO lookup and audit risk flag reference tables were applied for enrichment.

## Limitations

- Tot_Mftr is a proxy measure for single-source status and may over-count
- OH ZIP codes were stored as numeric and required VARCHAR casting
- Reimbursement rate analysis used AVG(paid/billed), which may be skewed by outlier claims

## Open Items

- Confirm Skilled Nursing cross-state comparison methodology with Farragut
- Apply orphan drug and MFN flags to Part D candidate list
- Validate FL audit risk flags against current CMS guidelines
"""


@pytest.fixture(autouse=True)
def _clean_session():
    """Ensure a clean session singleton for each test.

    Saves and restores the previous session to avoid breaking other test
    modules that depend on the startup session singleton.
    """
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
# PROMPT BUILDER TESTS
# ---------------------------------------------------------------------------

class TestBuildPrompt:
    """Tests for build_analysis_summary_prompt."""

    def _sample_events(self):
        return [
            {
                "event_type": "dataset_import",
                "timestamp": "2026-04-08T10:00:00Z",
                "details": {"dataset": "tx_medicaid", "row_count": 5000, "column_count": 12},
            },
            {
                "event_type": "reference_load",
                "timestamp": "2026-04-08T10:01:00Z",
                "details": {"reference_name": "mco_lookup", "source": "library"},
            },
            {
                "event_type": "ai_sql_generated",
                "timestamp": "2026-04-08T10:02:00Z",
                "details": {"question": "What are the top MCOs by total claims in TX?"},
            },
            {
                "event_type": "query_run",
                "timestamp": "2026-04-08T10:02:30Z",
                "details": {
                    "dataset": "tx_medicaid",
                    "sql": "SELECT mco_name, COUNT(*) as claims FROM dataset GROUP BY mco_name ORDER BY claims DESC",
                    "rowcount": 3,
                    "elapsed_seconds": 0.042,
                },
            },
            {
                "event_type": "result_narrative",
                "timestamp": "2026-04-08T10:02:35Z",
                "details": {"narrative": "Lone Star Health Plan leads with 47.8% of claims."},
            },
        ]

    def _sample_meta(self):
        return {
            "name": "Medicaid Analysis",
            "datasets_used": ["tx_medicaid"],
            "queries_run": 1,
            "duration_seconds": 300,
        }

    def test_prompt_includes_session_metadata(self):
        prompt = build_analysis_summary_prompt(
            session_events=self._sample_events(),
            session_meta=self._sample_meta(),
        )
        assert "Medicaid Analysis" in prompt
        assert "tx_medicaid" in prompt

    def test_prompt_includes_events(self):
        prompt = build_analysis_summary_prompt(
            session_events=self._sample_events(),
            session_meta=self._sample_meta(),
        )
        assert "Imported dataset: tx_medicaid" in prompt
        assert "Loaded reference table: mco_lookup" in prompt
        assert 'Asked: "What are the top MCOs' in prompt

    def test_prompt_includes_sql_in_normal_mode(self):
        prompt = build_analysis_summary_prompt(
            session_events=self._sample_events(),
            session_meta=self._sample_meta(),
            privacy_mode=False,
        )
        assert "SELECT mco_name" in prompt

    def test_prompt_strips_sql_in_privacy_mode(self):
        prompt = build_analysis_summary_prompt(
            session_events=self._sample_events(),
            session_meta=self._sample_meta(),
            privacy_mode=True,
        )
        assert "SELECT mco_name" not in prompt
        assert "Ran query on tx_medicaid" in prompt

    def test_prompt_strips_narratives_in_privacy_mode(self):
        prompt = build_analysis_summary_prompt(
            session_events=self._sample_events(),
            session_meta=self._sample_meta(),
            privacy_mode=True,
        )
        assert "Lone Star" not in prompt

    def test_prompt_includes_privacy_restriction_text(self):
        prompt = build_analysis_summary_prompt(
            session_events=self._sample_events(),
            session_meta=self._sample_meta(),
            privacy_mode=True,
        )
        assert "PRIVACY RESTRICTION" in prompt

    def test_prompt_empty_events(self):
        prompt = build_analysis_summary_prompt(
            session_events=[],
            session_meta={},
        )
        assert "no analytical events recorded" in prompt

    def test_prompt_includes_four_section_headers(self):
        prompt = build_analysis_summary_prompt(
            session_events=self._sample_events(),
            session_meta=self._sample_meta(),
        )
        assert "## Findings" in prompt
        assert "## Methodology" in prompt
        assert "## Limitations" in prompt
        assert "## Open Items" in prompt


# ---------------------------------------------------------------------------
# PARSER TESTS
# ---------------------------------------------------------------------------

class TestParseResponse:
    """Tests for parse_analysis_summary."""

    def test_parses_four_sections(self):
        result = parse_analysis_summary(MOCK_AI_RESPONSE)
        assert result["findings"] != ""
        assert result["methodology"] != ""
        assert result["limitations"] != ""
        assert result["open_items"] != ""

    def test_findings_content(self):
        result = parse_analysis_summary(MOCK_AI_RESPONSE)
        assert "TX Medicaid" in result["findings"]
        assert "Lone Star" in result["findings"]

    def test_raw_text_preserved(self):
        result = parse_analysis_summary(MOCK_AI_RESPONSE)
        assert "## Findings" in result["raw_text"]

    def test_handles_markdown_fences(self):
        fenced = "```markdown\n" + MOCK_AI_RESPONSE + "\n```"
        result = parse_analysis_summary(fenced)
        assert result["findings"] != ""
        assert result["methodology"] != ""

    def test_handles_empty_response(self):
        result = parse_analysis_summary("")
        assert result["findings"] == ""
        assert result["methodology"] == ""
        assert result["limitations"] == ""
        assert result["open_items"] == ""
        assert result["raw_text"] == ""

    def test_handles_partial_response(self):
        partial = "## Findings\n\nSome finding here.\n\n## Methodology\n\nSome method."
        result = parse_analysis_summary(partial)
        assert "Some finding" in result["findings"]
        assert "Some method" in result["methodology"]
        assert result["limitations"] == ""
        assert result["open_items"] == ""


# ---------------------------------------------------------------------------
# ENDPOINT TESTS
# ---------------------------------------------------------------------------

class TestEndpoint:
    """Tests for POST /api/session/analysis_summary."""

    def test_404_when_no_session(self, client):
        resp = client.post("/api/session/analysis_summary")
        assert resp.status_code == 404

    def test_400_when_no_events(self, client):
        start_session()
        # Session has only session_start event — needs analytical events
        # Clear events to simulate truly empty session
        session = get_current_session()
        session.events.clear()
        resp = client.post("/api/session/analysis_summary")
        assert resp.status_code == 400

    def test_success_with_mocked_ai(self, client):
        start_session()
        log_event(SessionEventType.DATASET_IMPORT, {
            "dataset": "test_ds", "row_count": 100, "column_count": 5
        })
        log_event(SessionEventType.QUERY_RUN, {
            "dataset": "test_ds", "sql": "SELECT * FROM dataset LIMIT 10",
            "rowcount": 10, "elapsed_seconds": 0.01,
        })

        with patch(
            "app.ai.provider_openai.generate_sql_response",
            return_value=MOCK_AI_RESPONSE,
        ):
            resp = client.post("/api/session/analysis_summary")
            assert resp.status_code == 200
            data = resp.json()
            assert "findings" in data
            assert "methodology" in data
            assert "limitations" in data
            assert "open_items" in data
            assert "raw_text" in data
            assert "TX Medicaid" in data["findings"]

    def test_privacy_mode_passes_through(self, client):
        start_session()
        log_event(SessionEventType.DATASET_IMPORT, {
            "dataset": "test_ds", "row_count": 100, "column_count": 5
        })

        with patch("app.main._get_privacy_mode", return_value=True), \
             patch(
                 "app.ai.provider_openai.generate_sql_response",
                 return_value=MOCK_AI_RESPONSE,
             ) as mock_call:
            resp = client.post("/api/session/analysis_summary")
            assert resp.status_code == 200
            # Verify the prompt sent to AI includes privacy restriction
            prompt_sent = mock_call.call_args[0][0]
            assert "PRIVACY RESTRICTION" in prompt_sent

    def test_402_when_no_api_key(self, client):
        start_session()
        log_event(SessionEventType.DATASET_IMPORT, {"dataset": "x"})
        with patch("app.main._has_key", return_value=False):
            resp = client.post("/api/session/analysis_summary")
            assert resp.status_code == 402
