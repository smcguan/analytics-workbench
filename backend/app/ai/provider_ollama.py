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

Uses only Python standard library (urllib) — no third-party packages
required. This ensures it works in both dev mode and the packaged
PyInstaller .exe where requests may not be bundled.

============================================================
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
import urllib.error

logger = logging.getLogger("app")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")


def _get_model() -> str:
    """Read the configured model name from config at request time."""
    from app.key_manager import get_ollama_model
    return get_ollama_model()


def check_ollama_available() -> bool:
    """Return True if Ollama is running and responding at the configured URL."""
    url = f"{OLLAMA_BASE_URL}/api/tags"
    try:
        with urllib.request.urlopen(url, timeout=2) as resp:
            logger.info("check_ollama_available: GET %s → %s", url, resp.status)
            return resp.status == 200
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
    model = _get_model()
    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{OLLAMA_BASE_URL}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
            if resp.status != 200:
                raise ConnectionError(f"Ollama error: HTTP {resp.status} — {body[:200]}")
            data = json.loads(body)
            return data.get("response", "").strip()
    except urllib.error.URLError as exc:
        raise ConnectionError(
            "Ollama is not running. Start Ollama and try again."
        ) from exc
    except TimeoutError:
        raise ConnectionError(
            "Ollama request timed out. Check that the model is loaded."
        )
