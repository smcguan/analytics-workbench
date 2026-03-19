"""
test_reference_tables.py — tests for reference table import, list, delete, and AI consent

Covers:
  POST /api/references/import — CSV import, lightweight pipeline
  GET  /api/references — list loaded reference tables
  POST /api/references/{name}/delete — remove reference table
  POST /api/datasets/{name}/ai_consent — store and persist consent
  import_reference_table() service function

Run from project root:
    pytest tests/test_reference_tables.py -v
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.services.dataset_import import (
    import_reference_table,
    ReferenceImportResult,
    DatasetImportError,
)


# ===========================================================================
# FIXTURES
# ===========================================================================

@pytest.fixture()
def ref_tmp(tmp_path):
    """Provide a temporary REFERENCES_DIR and restore the original after."""
    original = main_module.REFERENCES_DIR
    main_module.REFERENCES_DIR = tmp_path / "references"
    main_module.REFERENCES_DIR.mkdir()
    yield main_module.REFERENCES_DIR
    main_module.REFERENCES_DIR = original


@pytest.fixture()
def ds_tmp(tmp_path):
    """Provide a temporary DATASETS_DIR with a test dataset for AI consent tests."""
    original = main_module.DATASETS_DIR
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    # Create a minimal test dataset
    test_ds = ds_dir / "test_consent"
    test_ds.mkdir()
    df = pd.DataFrame({"x": [1, 2, 3]})
    df.to_parquet(str(test_ds / "source.parquet"), index=False)
    meta = {"row_count": 3, "column_count": 1}
    (test_ds / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    main_module.DATASETS_DIR = ds_dir
    yield ds_dir
    main_module.DATASETS_DIR = original


@pytest.fixture()
def client(ref_tmp, ds_tmp):
    with TestClient(main_module.app) as c:
        yield c


def _make_csv_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


# ===========================================================================
# import_reference_table() SERVICE FUNCTION
# ===========================================================================

def test_import_reference_table_csv(tmp_path):
    csv_data = _make_csv_bytes([
        {"drug": "Keytruda", "excluded": 1},
        {"drug": "Opdivo", "excluded": 1},
    ])
    result = import_reference_table(
        uploaded_file=io.BytesIO(csv_data),
        original_filename="ira_exclusions.csv",
        registered_root=tmp_path,
    )
    assert isinstance(result, ReferenceImportResult)
    assert result.row_count == 2
    assert len(result.columns) == 2
    assert Path(result.parquet_path).exists()
    # _meta.json should exist
    meta_path = Path(result.reference_dir) / "_meta.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["row_count"] == 2
    assert meta["reference_name"] == result.reference_name


def test_import_reference_table_overwrites(tmp_path):
    csv1 = _make_csv_bytes([{"drug": "A"}])
    csv2 = _make_csv_bytes([{"drug": "B"}, {"drug": "C"}])

    r1 = import_reference_table(
        uploaded_file=io.BytesIO(csv1),
        original_filename="ref.csv",
        registered_root=tmp_path,
    )
    assert r1.row_count == 1

    r2 = import_reference_table(
        uploaded_file=io.BytesIO(csv2),
        original_filename="ref.csv",
        registered_root=tmp_path,
        overwrite=True,
    )
    assert r2.row_count == 2


def test_import_reference_table_empty_raises():
    csv_data = b"drug\n"  # header only, no rows
    with pytest.raises(DatasetImportError):
        import_reference_table(
            uploaded_file=io.BytesIO(csv_data),
            original_filename="empty.csv",
            registered_root="/tmp/aw_test_ref",
        )


def test_import_reference_table_no_metadata_json(tmp_path):
    """Reference tables should NOT create metadata.json (only _meta.json)."""
    csv_data = _make_csv_bytes([{"x": 1}])
    result = import_reference_table(
        uploaded_file=io.BytesIO(csv_data),
        original_filename="small.csv",
        registered_root=tmp_path,
    )
    ref_dir = Path(result.reference_dir)
    assert (ref_dir / "_meta.json").exists()
    assert not (ref_dir / "metadata.json").exists()


# ===========================================================================
# POST /api/references/import ENDPOINT
# ===========================================================================

def test_import_reference_endpoint(client, ref_tmp):
    csv_data = _make_csv_bytes([
        {"drug": "Stelara", "ira_round": 1},
        {"drug": "Imbruvica", "ira_round": 2},
    ])
    resp = client.post(
        "/api/references/import",
        files={"file": ("ira_drugs.csv", io.BytesIO(csv_data), "text/csv")},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["reference"] is not None
    assert data["row_count"] == 2
    assert len(data["columns"]) == 2


# ===========================================================================
# GET /api/references ENDPOINT
# ===========================================================================

def test_list_references_empty(client, ref_tmp):
    resp = client.get("/api/references")
    assert resp.status_code == 200
    assert resp.json()["references"] == []


def test_list_references_after_import(client, ref_tmp):
    csv_data = _make_csv_bytes([{"drug": "A"}])
    client.post(
        "/api/references/import",
        files={"file": ("test_ref.csv", io.BytesIO(csv_data), "text/csv")},
    )
    resp = client.get("/api/references")
    refs = resp.json()["references"]
    assert len(refs) == 1
    assert refs[0]["name"] is not None


# ===========================================================================
# POST /api/references/{name}/delete ENDPOINT
# ===========================================================================

def test_delete_reference(client, ref_tmp):
    csv_data = _make_csv_bytes([{"drug": "A"}])
    import_resp = client.post(
        "/api/references/import",
        files={"file": ("to_delete.csv", io.BytesIO(csv_data), "text/csv")},
    )
    name = import_resp.json()["reference"]

    del_resp = client.post(f"/api/references/{name}/delete")
    assert del_resp.status_code == 200

    # Verify it's gone
    list_resp = client.get("/api/references")
    assert len(list_resp.json()["references"]) == 0


def test_delete_nonexistent_reference(client, ref_tmp):
    """Deleting a reference that doesn't exist should not error."""
    resp = client.post("/api/references/no_such_ref/delete")
    assert resp.status_code == 200


# ===========================================================================
# AI CONSENT — POST /api/datasets/{name}/ai_consent
# ===========================================================================

def test_set_ai_consent_true(client, ds_tmp):
    resp = client.post("/api/datasets/test_consent/ai_consent", json={"ai_consent": True})
    assert resp.status_code == 200
    assert resp.json()["ai_consent"] is True
    # Verify persisted
    meta = json.loads((ds_tmp / "test_consent" / "_meta.json").read_text())
    assert meta["ai_consent"] is True


def test_set_ai_consent_false(client, ds_tmp):
    resp = client.post("/api/datasets/test_consent/ai_consent", json={"ai_consent": False})
    assert resp.status_code == 200
    assert resp.json()["ai_consent"] is False
    meta = json.loads((ds_tmp / "test_consent" / "_meta.json").read_text())
    assert meta["ai_consent"] is False


def test_ai_consent_preserves_existing_meta(client, ds_tmp):
    """Setting consent should not clobber other _meta.json fields."""
    meta_path = ds_tmp / "test_consent" / "_meta.json"
    meta = json.loads(meta_path.read_text())
    assert "row_count" in meta  # pre-existing field

    client.post("/api/datasets/test_consent/ai_consent", json={"ai_consent": True})

    updated = json.loads(meta_path.read_text())
    assert updated["ai_consent"] is True
    assert updated["row_count"] == 3  # preserved


def test_ai_consent_nonexistent_dataset(client, ds_tmp):
    resp = client.post("/api/datasets/no_such_dataset/ai_consent", json={"ai_consent": True})
    assert resp.status_code == 404


def test_ai_consent_exposed_in_meta(client, ds_tmp):
    """ai_consent should appear in GET /api/datasets/{name}/meta response."""
    client.post("/api/datasets/test_consent/ai_consent", json={"ai_consent": False})
    resp = client.get("/api/datasets/test_consent/meta")
    assert resp.status_code == 200
    assert resp.json()["ai_consent"] is False
