"""Tests for backend/app/key_manager.py — encryption, corruption, wrong-machine handling, privacy mode."""

import os
from unittest.mock import patch

import pytest

from app.key_manager import (
    has_key,
    get_key,
    save_key,
    clear_key,
    mask_key,
    get_privacy_mode,
    set_privacy_mode,
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
        _ensure_config_dir()
        _config_path().write_bytes(b"this is not valid fernet data")
        assert has_key() is False
        assert not _config_path().exists(), "corrupted file should be deleted"

    def test_has_key_returns_false_on_wrong_machine_key(self):
        save_key("sk-test-valid")
        assert has_key() is True
        with patch.dict(os.environ, {"COMPUTERNAME": "OTHER_MACHINE_XYZ"}):
            assert has_key() is False
            assert not _config_path().exists(), "wrong-machine file should be deleted"

    def test_get_key_deletes_corrupted_and_raises(self):
        _ensure_config_dir()
        _config_path().write_bytes(b"corrupted-garbage-data")
        with pytest.raises(RuntimeError, match="No API key configured"):
            get_key()
        assert not _config_path().exists(), "corrupted file should be deleted"

    def test_get_key_deletes_wrong_machine_and_raises(self):
        save_key("sk-test-for-machine-a")
        with patch.dict(os.environ, {"COMPUTERNAME": "DIFFERENT_BOX"}):
            with pytest.raises(RuntimeError, match="No API key configured"):
                get_key()
            assert not _config_path().exists()

    def test_fresh_key_works_after_corrupted_cleanup(self):
        _ensure_config_dir()
        _config_path().write_bytes(b"bad-data")
        assert has_key() is False
        save_key("sk-fresh-key-999")
        assert has_key() is True
        assert get_key() == "sk-fresh-key-999"


class TestPrivacyMode:
    """Privacy mode toggle — persisted alongside API key in config.enc."""

    def test_default_is_false(self):
        assert get_privacy_mode() is False

    def test_default_false_even_with_key(self):
        save_key("sk-test-key")
        assert get_privacy_mode() is False

    def test_set_true_persists(self):
        save_key("sk-test-key")
        set_privacy_mode(True)
        assert get_privacy_mode() is True

    def test_set_false_persists(self):
        save_key("sk-test-key")
        set_privacy_mode(True)
        set_privacy_mode(False)
        assert get_privacy_mode() is False

    def test_toggle_does_not_affect_key(self):
        save_key("sk-test-key")
        set_privacy_mode(True)
        assert get_key() == "sk-test-key"

    def test_key_save_does_not_affect_privacy(self):
        save_key("sk-test-key")
        set_privacy_mode(True)
        save_key("sk-new-key")
        assert get_privacy_mode() is True
        assert get_key() == "sk-new-key"

    def test_clear_key_resets_privacy(self):
        save_key("sk-test-key")
        set_privacy_mode(True)
        clear_key()
        assert get_privacy_mode() is False

    def test_privacy_mode_without_key(self):
        """Privacy mode can be set even without a key configured."""
        set_privacy_mode(True)
        assert get_privacy_mode() is True
        assert has_key() is False
