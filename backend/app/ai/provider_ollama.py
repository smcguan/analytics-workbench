"""
============================================================
FILE: provider_ollama.py
LOCATION: backend/app/ai/provider_ollama.py
============================================================

PURPOSE
-------
Ollama provider for local AI execution. Drop-in replacement for
generate_sql_response() from provider_openai.py.

Uses the Ollama HTTP API at localhost:11434 with the llama3.2 model.
All prompts are identical to the OpenAI provider — only the
transport and model name differ.

This provider exists for compliance demonstration: Farragut's
legal team needs a verified air-gap option. Quality is lower
than GPT-4o-mini for complex SQL but sufficient for the
procurement unlock.

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
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")


def check_ollama_available() -> bool:
    """Return True if Ollama is running and responding at the configured URL."""
    if requests is None:
        return False
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=2)
        return resp.status_code == 200
    except Exception:
        return False


def get_ollama_model() -> str:
    """Return the configured Ollama model name."""
    return OLLAMA_MODEL


def generate_response(prompt: str) -> str:
    """Send a prompt to Ollama and return the raw output text.

    Raises ConnectionError if Ollama is not reachable, which routes.py
    translates to HTTP 503.
    """
    if requests is None:
        raise ConnectionError(
            "Ollama support requires the 'requests' package. Install with: pip install requests"
        )
    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
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
