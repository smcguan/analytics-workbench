"""
============================================================
FILE: key_manager.py
LOCATION: src/key_manager.py
============================================================

PURPOSE
-------
Manages the customer-supplied OpenAI API key. The key is
encrypted at rest using Fernet symmetric encryption with a
machine-specific derivation key (COMPUTERNAME + USERNAME
hashed with SHA-256).

Storage location: %APPDATA%/JetWareAI/config.enc

This module is the ONLY place the API key is read or written.
No other module should access the key file directly.
============================================================
"""

from __future__ import annotations

import base64
import hashlib
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
        # Fallback for non-Windows or missing APPDATA
        appdata = str(Path.home() / ".config")
    return Path(appdata) / "JetWareAI" / "config.enc"


def _ensure_config_dir() -> None:
    _config_path().parent.mkdir(parents=True, exist_ok=True)


# ============================================================
# PUBLIC API
# ============================================================

def has_key() -> bool:
    """Return True if a valid encrypted key exists on disk."""
    path = _config_path()
    if not path.exists():
        return False
    try:
        _get_fernet().decrypt(path.read_bytes())
        return True
    except (InvalidToken, Exception):
        return False


def get_key() -> str:
    """Decrypt and return the API key. Raises RuntimeError if not found."""
    path = _config_path()
    if not path.exists():
        raise RuntimeError("No API key configured. Add your key in Settings.")
    try:
        decrypted = _get_fernet().decrypt(path.read_bytes())
        return decrypted.decode("utf-8")
    except InvalidToken:
        raise RuntimeError("API key file is corrupted. Please re-enter your key in Settings.")
    except Exception as exc:
        raise RuntimeError(f"Failed to read API key: {exc}")


def save_key(key: str) -> None:
    """Encrypt and write the API key to disk."""
    _ensure_config_dir()
    encrypted = _get_fernet().encrypt(key.encode("utf-8"))
    _config_path().write_bytes(encrypted)
    logger.info("API key saved to %s", _config_path())


def clear_key() -> None:
    """Delete the config file."""
    path = _config_path()
    if path.exists():
        path.unlink()
        logger.info("API key cleared from %s", path)


def mask_key(key: str) -> str:
    """Return masked format: sk-...XXXX (last 4 chars)."""
    if len(key) <= 7:
        return "sk-...****"
    return f"sk-...{key[-4:]}"
