"""
============================================================
FILE: key_manager.py
LOCATION: backend/app/key_manager.py
============================================================

PURPOSE
-------
Manages the customer-supplied OpenAI API key and application
settings (e.g. privacy_mode). All values are stored as a JSON
object encrypted with Fernet symmetric encryption using a
machine-specific derivation key (COMPUTERNAME + USERNAME
hashed with SHA-256).

Storage location: %APPDATA%/JetWareAI/config.enc

Config structure (encrypted JSON):
    {"key": "sk-...", "privacy_mode": false, "ai_mode": "cloud"}

This module is the ONLY place the config file is read or
written. No other module should access it directly.

BACKWARD COMPATIBILITY
----------------------
Older installs stored only the raw API key string (not JSON).
_read_config() detects this and migrates transparently.
============================================================
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger("app")

# ============================================================
# ENCRYPTION KEY DERIVATION
# ============================================================

def _derive_fernet_key() -> bytes:
    """Derive a Fernet key from machine-specific values.

    Uses COMPUTERNAME + USERNAME as seed, hashed with SHA-256,
    then base64-encoded to produce a valid 32-byte Fernet key.
    """
    computer = os.getenv("COMPUTERNAME", "UNKNOWN")
    user = os.getenv("USERNAME", "UNKNOWN")
    seed = f"{computer}{user}".encode("utf-8")
    digest = hashlib.sha256(seed).digest()
    return base64.urlsafe_b64encode(digest)


def _get_fernet() -> Fernet:
    return Fernet(_derive_fernet_key())


# ============================================================
# CONFIG FILE PATH
# ============================================================

def _config_path() -> Path:
    appdata = os.getenv("APPDATA")
    if not appdata:
        appdata = str(Path.home() / ".config")
    return Path(appdata) / "JetWareAI" / "config.enc"


def _ensure_config_dir() -> None:
    _config_path().parent.mkdir(parents=True, exist_ok=True)


# ============================================================
# INTERNAL CONFIG READ / WRITE
# ============================================================

def _read_config() -> dict:
    """Decrypt and return the config dict. Returns {} if missing or corrupt.

    Handles backward compatibility: if the decrypted content is a plain
    string (old format), it migrates to {"key": "<value>"} and re-saves.
    """
    path = _config_path()
    if not path.exists():
        return {}
    try:
        decrypted = _get_fernet().decrypt(path.read_bytes()).decode("utf-8")
    except Exception:
        logger.warning(
            "config.enc decryption failed (wrong machine or corrupted) — deleting %s",
            path,
        )
        try:
            path.unlink()
        except OSError as exc:
            logger.warning("Could not delete bad config.enc: %s", exc)
        return {}

    # Try JSON first (new format)
    try:
        cfg = json.loads(decrypted)
        if isinstance(cfg, dict):
            return cfg
    except (json.JSONDecodeError, ValueError):
        pass

    # Backward compat: plain string = raw API key from v1.20.0
    if decrypted.startswith("sk-"):
        cfg = {"key": decrypted}
        _write_config(cfg)  # migrate to JSON format
        return cfg

    return {}


def _write_config(cfg: dict) -> None:
    """Encrypt and write the config dict to disk."""
    _ensure_config_dir()
    payload = json.dumps(cfg).encode("utf-8")
    encrypted = _get_fernet().encrypt(payload)
    _config_path().write_bytes(encrypted)


# ============================================================
# PUBLIC API — API KEY
# ============================================================

def has_key() -> bool:
    """Return True if a valid API key exists in the config."""
    cfg = _read_config()
    return bool(cfg.get("key"))


def get_key() -> str:
    """Return the API key. Raises RuntimeError if not found."""
    cfg = _read_config()
    key = cfg.get("key")
    if not key:
        raise RuntimeError("No API key configured. Add your key in Settings.")
    return key


def save_key(key: str) -> None:
    """Save the API key, preserving other config fields."""
    cfg = _read_config()
    cfg["key"] = key
    _write_config(cfg)
    logger.info("API key saved to %s", _config_path())


def clear_key() -> None:
    """Delete the entire config file."""
    path = _config_path()
    if path.exists():
        path.unlink()
        logger.info("API key cleared from %s", path)


def mask_key(key: str) -> str:
    """Return masked format: sk-...XXXX (last 4 chars)."""
    if len(key) <= 7:
        return "sk-...****"
    return f"sk-...{key[-4:]}"


# ============================================================
# PUBLIC API — PRIVACY MODE
# ============================================================

def get_privacy_mode() -> bool:
    """Return the current privacy_mode setting. Defaults to False."""
    cfg = _read_config()
    return bool(cfg.get("privacy_mode", False))


def set_privacy_mode(enabled: bool) -> None:
    """Save the privacy_mode setting, preserving other config fields."""
    cfg = _read_config()
    cfg["privacy_mode"] = enabled
    _write_config(cfg)
    logger.info("Privacy mode set to %s", enabled)


# ============================================================
# PUBLIC API — AI MODE
# ============================================================

_VALID_AI_MODES = ("cloud", "local")


def get_ai_mode() -> str:
    """Return the current ai_mode setting. Defaults to 'cloud'."""
    cfg = _read_config()
    mode = cfg.get("ai_mode", "cloud")
    return mode if mode in _VALID_AI_MODES else "cloud"


def set_ai_mode(mode: str) -> None:
    """Save the ai_mode setting. Validates value is 'cloud' or 'local'."""
    if mode not in _VALID_AI_MODES:
        raise ValueError(f"Invalid ai_mode: {mode!r}. Must be 'cloud' or 'local'.")
    cfg = _read_config()
    cfg["ai_mode"] = mode
    _write_config(cfg)
    logger.info("AI mode set to %s", mode)


# ============================================================
# PUBLIC API — OLLAMA MODEL
# ============================================================

_DEFAULT_OLLAMA_MODEL = "llama3.1:8b"


def get_ollama_model() -> str:
    """Return the configured Ollama model name. Defaults to 'llama3.1:8b'."""
    cfg = _read_config()
    model = cfg.get("ollama_model", "")
    return model if model else _DEFAULT_OLLAMA_MODEL


def set_ollama_model(model: str) -> None:
    """Save the Ollama model name. Validates it's a non-empty string."""
    if not model or not isinstance(model, str) or not model.strip():
        raise ValueError("Ollama model name must be a non-empty string.")
    cfg = _read_config()
    cfg["ollama_model"] = model.strip()
    _write_config(cfg)
    logger.info("Ollama model set to %s", model.strip())
