"""Global test configuration for Analytics Workbench."""

from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _bypass_api_key_check():
    """Bypass the API key requirement in all tests.

    Tests mock the AI provider functions individually — they never make
    real OpenAI calls. The _require_api_key guard would block those
    tests with a 402 before the mock even runs, so we disable it here.
    """
    with patch("app.ai.routes._has_api_key", return_value=True):
        yield
