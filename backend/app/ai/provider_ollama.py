"""
============================================================
FILE: provider_ollama.py
LOCATION: backend/app/ai/provider_ollama.py
============================================================

PURPOSE
-------
Ollama provider for local AI execution. Drop-in replacement for
generate_sql_response() from provider_openai.py.

Uses the Ollama HTTP API at localhost:11434. The model name is
read from config at request time via key_manager.get_ollama_model()
so changes in Settings take effect immediately without restart.

============================================================
"""

from __future__ import annotations

import json
import logging
import os

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

logger = logging.getLogger("app")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")


def _get_model() -> str:
    """Read the configured model name from config at request time."""
    from app.key_manager import get_ollama_model
    return get_ollama_model()


def check_ollama_available() -> bool:
    """Return True if Ollama is running and responding at the configured URL."""
    if requests is None:
        logger.warning("check_ollama_available: requests module not installed")
        return False
    url = f"{OLLAMA_BASE_URL}/api/tags"
    try:
        resp = requests.get(url, timeout=2)
        logger.info("check_ollama_available: GET %s → %s", url, resp.status_code)
        return resp.status_code == 200
    except Exception as exc:
        logger.warning("check_ollama_available: GET %s → %s: %s", url, type(exc).__name__, exc)
        return False


def get_ollama_model() -> str:
    """Return the configured Ollama model name (delegates to key_manager)."""
    return _get_model()


def generate_response(prompt: str) -> str:
    """Send a prompt to Ollama and return the raw output text.

    Raises ConnectionError if Ollama is not reachable, which routes.py
    translates to HTTP 503.
    """
    if requests is None:
        raise ConnectionError(
            "Ollama support requires the 'requests' package. Install with: pip install requests"
        )
    model = _get_model()
    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
            },
            timeout=120,
        )
    except requests.ConnectionError:
        raise ConnectionError(
            "Ollama is not running. Start Ollama and try again."
        )
    except requests.Timeout:
        raise ConnectionError(
            "Ollama request timed out. Check that the model is loaded."
        )

    if resp.status_code != 200:
        error_detail = resp.text[:200] if resp.text else f"HTTP {resp.status_code}"
        raise ConnectionError(f"Ollama error: {error_detail}")

    data = resp.json()
    return data.get("response", "").strip()
