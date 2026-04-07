"""Tests for backend/app/key_manager.py — encryption, corruption, wrong-machine handling."""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from app.key_manager import (
    has_key,
    get_key,
    save_key,
    clear_key,
    mask_key,
    _config_path,
    _ensure_config_dir,
)


@pytest.fixture(autouse=True)
def _isolated_config(tmp_path, monkeypatch):
    """Redirect config.enc to a temp directory so tests never touch real key."""
    fake_appdata = str(tmp_path / "appdata")
    monkeypatch.setenv("APPDATA", fake_appdata)


class TestKeyManagerBasics:
    """Round-trip save / get / clear / mask."""

    def test_no_key_initially(self):
        assert has_key() is False

    def test_save_and_retrieve(self):
        save_key("sk-test-abc123")
        assert has_key() is True
        assert get_key() == "sk-test-abc123"

    def test_clear_key(self):
        save_key("sk-test-abc123")
        clear_key()
        assert has_key() is False

    def test_get_key_raises_when_missing(self):
        with pytest.raises(RuntimeError, match="No API key configured"):
            get_key()

    def test_mask_key_normal(self):
        assert mask_key("sk-proj-abcdefghijklmnop") == "sk-...mnop"

    def test_mask_key_short(self):
        assert mask_key("sk-") == "sk-...****"


class TestCorruptedConfig:
    """BUG-011: corrupted or wrong-machine config.enc must be auto-deleted."""

    def test_has_key_returns_false_on_corrupted_file(self):
        """A corrupted config.enc should be deleted and has_key() returns False."""
        _ensure_config_dir()
        _config_path().write_bytes(b"this is not valid fernet data")
        assert has_key() is False
        assert not _config_path().exists(), "corrupted file should be deleted"

    def test_has_key_returns_false_on_wrong_machine_key(self):
        """config.enc encrypted on a different machine (different seed) should be
        detected as invalid, deleted, and has_key() returns False."""
        # Save a valid key with current machine identity
        save_key("sk-test-valid")
        assert has_key() is True

        # Simulate a different machine by changing COMPUTERNAME
        with patch.dict(os.environ, {"COMPUTERNAME": "OTHER_MACHINE_XYZ"}):
            assert has_key() is False
            assert not _config_path().exists(), "wrong-machine file should be deleted"

    def test_get_key_deletes_corrupted_and_raises(self):
        """get_key() on a corrupted file should delete it and raise RuntimeError."""
        _ensure_config_dir()
        _config_path().write_bytes(b"corrupted-garbage-data")
        with pytest.raises(RuntimeError, match="corrupted or from another machine"):
            get_key()
        assert not _config_path().exists(), "corrupted file should be deleted"

    def test_get_key_deletes_wrong_machine_and_raises(self):
        """get_key() with a wrong-machine file should delete it and raise."""
        save_key("sk-test-for-machine-a")
        with patch.dict(os.environ, {"COMPUTERNAME": "DIFFERENT_BOX"}):
            with pytest.raises(RuntimeError, match="corrupted or from another machine"):
                get_key()
            assert not _config_path().exists()

    def test_fresh_key_works_after_corrupted_cleanup(self):
        """After auto-deleting a corrupted file, a fresh save/get cycle works."""
        _ensure_config_dir()
        _config_path().write_bytes(b"bad-data")
        assert has_key() is False
        save_key("sk-fresh-key-999")
        assert has_key() is True
        assert get_key() == "sk-fresh-key-999"
