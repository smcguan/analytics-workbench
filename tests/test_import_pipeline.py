"""
test_import_pipeline.py — tests for dataset import pipeline and AI consent enforcement

Covers:
  import_dataset() — CSV, TSV, Excel, Parquet end-to-end
  detect_file_type() — extension mapping
  make_registered_name() — name sanitisation
  _strip_trailing_special_chars_from_df() — footnote marker removal
  _title_case_string_columns() — reference table normalisation
  POST /api/datasets/import — HTTP endpoint
  AI consent enforcement on /api/ai/insights and /api/ai/suggest_questions

Run from project root:
    pytest tests/test_import_pipeline.py -v
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.services.dataset_import import (
    DatasetImportError,
    DatasetValidationError,
    UnsupportedDatasetTypeError,
    detect_file_type,
    import_dataset,
    make_registered_name,
    _strip_trailing_special_chars_from_df,
    _title_case_string_columns,
)


# ===========================================================================
# HELPERS
# ===========================================================================

def _make_csv_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def _make_tsv_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_csv(buf, index=False, sep="\t")
    return buf.getvalue()


def _make_xlsx_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _make_parquet_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


SAMPLE_ROWS = [
    {"drug_name": "Keytruda", "hcpcs_code": "J9271", "total_paid": 1500.0, "claims": 10},
    {"drug_name": "Opdivo", "hcpcs_code": "J9299", "total_paid": 2200.0, "claims": 8},
    {"drug_name": "Avastin", "hcpcs_code": "J9035", "total_paid": 900.0, "claims": 15},
]


# ===========================================================================
# 1. CSV import end-to-end
# ===========================================================================

def test_csv_import_end_to_end(tmp_path):
    csv_data = _make_csv_bytes(SAMPLE_ROWS)
    result = import_dataset(
        uploaded_file=io.BytesIO(csv_data),
        original_filename="drugs.csv",
        registered_root=tmp_path,
    )
    ds_dir = Path(result.dataset_dir)
    assert (ds_dir / "source.parquet").exists()
    assert (ds_dir / "metadata.json").exists()
    assert (ds_dir / "_meta.json").exists()
    assert result.metadata.row_count == 3
    col_names = [c.name for c in result.metadata.columns]
    assert col_names == ["drug_name", "hcpcs_code", "total_paid", "claims"]


# ===========================================================================
# 2. TSV import end-to-end
# ===========================================================================

def test_tsv_import_end_to_end(tmp_path):
    tsv_data = _make_tsv_bytes(SAMPLE_ROWS)
    result = import_dataset(
        uploaded_file=io.BytesIO(tsv_data),
        original_filename="drugs.tsv",
        registered_root=tmp_path,
    )
    ds_dir = Path(result.dataset_dir)
    assert (ds_dir / "source.parquet").exists()
    assert (ds_dir / "metadata.json").exists()
    assert (ds_dir / "_meta.json").exists()
    assert result.metadata.row_count == 3
    col_names = [c.name for c in result.metadata.columns]
    assert col_names == ["drug_name", "hcpcs_code", "total_paid", "claims"]


# ===========================================================================
# 3. Excel (.xlsx) import end-to-end
# ===========================================================================

def test_xlsx_import_end_to_end(tmp_path):
    xlsx_data = _make_xlsx_bytes(SAMPLE_ROWS)
    result = import_dataset(
        uploaded_file=io.BytesIO(xlsx_data),
        original_filename="drugs.xlsx",
        registered_root=tmp_path,
    )
    ds_dir = Path(result.dataset_dir)
    assert (ds_dir / "source.parquet").exists()
    assert (ds_dir / "metadata.json").exists()
    assert (ds_dir / "_meta.json").exists()
    assert result.metadata.row_count == 3
    col_names = [c.name for c in result.metadata.columns]
    assert col_names == ["drug_name", "hcpcs_code", "total_paid", "claims"]


# ===========================================================================
# 4. Parquet import end-to-end (fast-copy path)
# ===========================================================================

def test_parquet_import_end_to_end(tmp_path):
    parquet_data = _make_parquet_bytes(SAMPLE_ROWS)
    result = import_dataset(
        uploaded_file=io.BytesIO(parquet_data),
        original_filename="drugs.parquet",
        registered_root=tmp_path,
    )
    ds_dir = Path(result.dataset_dir)
    assert (ds_dir / "source.parquet").exists()
    assert (ds_dir / "metadata.json").exists()
    assert result.metadata.row_count == 3
    # Parquet fast-copy: the source.parquet should be a copy, not a conversion
    # Verify we can read it back correctly
    df = pd.read_parquet(ds_dir / "source.parquet")
    assert len(df) == 3
    assert list(df.columns) == ["drug_name", "hcpcs_code", "total_paid", "claims"]


# ===========================================================================
# 5. Import with overwrite=True
# ===========================================================================

def test_import_overwrite_true(tmp_path):
    csv1 = _make_csv_bytes([{"x": 1}])
    csv2 = _make_csv_bytes([{"x": 2}, {"x": 3}])

    r1 = import_dataset(
        uploaded_file=io.BytesIO(csv1),
        original_filename="data.csv",
        registered_root=tmp_path,
    )
    assert r1.metadata.row_count == 1

    r2 = import_dataset(
        uploaded_file=io.BytesIO(csv2),
        original_filename="data.csv",
        registered_root=tmp_path,
        overwrite=True,
    )
    assert r2.metadata.row_count == 2

    # Verify the old data is gone
    df = pd.read_parquet(Path(r2.dataset_dir) / "source.parquet")
    assert list(df["x"]) == [2, 3]


# ===========================================================================
# 6. Import with overwrite=False (default) — duplicate raises error
# ===========================================================================

def test_import_overwrite_false_raises(tmp_path):
    csv_data = _make_csv_bytes([{"x": 1}])

    import_dataset(
        uploaded_file=io.BytesIO(csv_data),
        original_filename="dup.csv",
        registered_root=tmp_path,
    )

    with pytest.raises(DatasetValidationError, match="already exists"):
        import_dataset(
            uploaded_file=io.BytesIO(csv_data),
            original_filename="dup.csv",
            registered_root=tmp_path,
            overwrite=False,
        )


# ===========================================================================
# 7. Import empty CSV (headers only, no data rows)
# ===========================================================================

def test_import_empty_csv_raises(tmp_path):
    empty_csv = b"col_a,col_b\n"
    with pytest.raises(DatasetValidationError):
        import_dataset(
            uploaded_file=io.BytesIO(empty_csv),
            original_filename="empty.csv",
            registered_root=tmp_path,
        )


# ===========================================================================
# 8. Import file with zero bytes
# ===========================================================================

def test_import_zero_bytes_raises(tmp_path):
    with pytest.raises(DatasetValidationError):
        import_dataset(
            uploaded_file=io.BytesIO(b""),
            original_filename="nothing.csv",
            registered_root=tmp_path,
        )


# ===========================================================================
# 9. detect_file_type()
# ===========================================================================

def test_detect_file_type_csv():
    assert detect_file_type("data.csv") == "csv"

def test_detect_file_type_tsv():
    assert detect_file_type("data.tsv") == "tsv"

def test_detect_file_type_xlsx():
    assert detect_file_type("data.xlsx") == "xlsx"

def test_detect_file_type_xls():
    assert detect_file_type("data.xls") == "xlsx"

def test_detect_file_type_parquet():
    assert detect_file_type("data.parquet") == "parquet"

def test_detect_file_type_unknown_raises():
    with pytest.raises(UnsupportedDatasetTypeError):
        detect_file_type("data.json")


# ===========================================================================
# 10. make_registered_name()
# ===========================================================================

def test_make_registered_name_spaces():
    assert make_registered_name("My Dataset") == "my_dataset"

def test_make_registered_name_leading_number():
    result = make_registered_name("2023 Sales")
    assert result.startswith("dataset_")
    assert "2023" in result

def test_make_registered_name_special_chars():
    result = make_registered_name("drugs (Part B) & more!")
    # Should strip non-alphanumeric chars except underscores
    assert "(" not in result
    assert ")" not in result
    assert "&" not in result
    assert "!" not in result
    assert len(result) > 0

def test_make_registered_name_unicode():
    result = make_registered_name("Données médicales")
    # Unicode accented chars are stripped by the regex [^a-z0-9_\s-]
    assert len(result) > 0
    # Should still produce a valid identifier
    assert result.replace("_", "").isalnum() or result == "dataset"

def test_make_registered_name_very_long():
    long_name = "a" * 500
    result = make_registered_name(long_name)
    assert len(result) > 0  # should not crash

def test_make_registered_name_empty_string():
    assert make_registered_name("") == "dataset"

def test_make_registered_name_only_special_chars():
    assert make_registered_name("!!!@@@###") == "dataset"


# ===========================================================================
# 11. strip_trailing_special_chars
# ===========================================================================

def test_strip_trailing_special_chars_enabled(tmp_path):
    """Strip trailing special chars from string columns during CSV import.

    This test documents a real bug: _strip_trailing_special_chars_from_df
    checks `df[col].dtype == object` but pandas >= 2.0 returns StringDtype
    for string columns, so the stripping never fires.
    """
    rows = [
        {"drug": "Stelara*", "code": "J9035", "paid": 100.0},
        {"drug": "Humira**", "code": "J0135", "paid": 200.0},
        {"drug": "Enbrel\u2020", "code": "J1438", "paid": 150.0},  # dagger
    ]
    csv_data = _make_csv_bytes(rows)
    result = import_dataset(
        uploaded_file=io.BytesIO(csv_data),
        original_filename="cms_drugs.csv",
        registered_root=tmp_path,
        strip_trailing_special_chars=True,
    )
    df = pd.read_parquet(Path(result.dataset_dir) / "source.parquet")
    assert list(df["drug"]) == ["Stelara", "Humira", "Enbrel"]
    # Numeric column should be untouched
    assert list(df["paid"]) == [100.0, 200.0, 150.0]


def test_strip_trailing_special_chars_disabled(tmp_path):
    rows = [{"drug": "Stelara*", "paid": 100.0}]
    csv_data = _make_csv_bytes(rows)
    result = import_dataset(
        uploaded_file=io.BytesIO(csv_data),
        original_filename="cms_no_strip.csv",
        registered_root=tmp_path,
        strip_trailing_special_chars=False,
    )
    df = pd.read_parquet(Path(result.dataset_dir) / "source.parquet")
    assert df["drug"].iloc[0] == "Stelara*"


# ===========================================================================
# 12. _title_case_string_columns()
# ===========================================================================

def test_title_case_string_columns(tmp_path):
    df = pd.DataFrame({
        "drug_name": ["keytruda", "OPDIVO", "avastin"],
        "amount": [100.0, 200.0, 300.0],
        "count": [1, 2, 3],
    })
    pq_path = tmp_path / "test.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, pq_path)

    _title_case_string_columns(pq_path)

    result_df = pd.read_parquet(pq_path)
    # String column should be title-cased
    assert list(result_df["drug_name"]) == ["Keytruda", "Opdivo", "Avastin"]
    # Numeric columns should be unchanged
    assert list(result_df["amount"]) == [100.0, 200.0, 300.0]
    assert list(result_df["count"]) == [1, 2, 3]


# ===========================================================================
# 13. Import via HTTP endpoint (POST /api/datasets/import)
# ===========================================================================

@pytest.fixture()
def http_tmp(tmp_path):
    """Provide temporary DATASETS_DIR for HTTP endpoint tests."""
    original_ds = main_module.DATASETS_DIR
    original_ref = main_module.REFERENCES_DIR
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    ref_dir = tmp_path / "references"
    ref_dir.mkdir()
    main_module.DATASETS_DIR = ds_dir
    main_module.REFERENCES_DIR = ref_dir
    yield ds_dir
    main_module.DATASETS_DIR = original_ds
    main_module.REFERENCES_DIR = original_ref


@pytest.fixture()
def http_client(http_tmp):
    with TestClient(main_module.app) as c:
        yield c


def test_import_endpoint_csv(http_client, http_tmp):
    csv_data = _make_csv_bytes(SAMPLE_ROWS)
    resp = http_client.post(
        "/api/datasets/import",
        files={"file": ("test_drugs.csv", io.BytesIO(csv_data), "text/csv")},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] == 3
    assert data["column_count"] == 4
    assert data["original_type"] == "csv"
    assert len(data["columns"]) == 4
    # Verify files on disk
    ds_dir = http_tmp / data["dataset"]
    assert (ds_dir / "source.parquet").exists()
    assert (ds_dir / "metadata.json").exists()


# ===========================================================================
# 14-17. AI CONSENT ENFORCEMENT
# ===========================================================================

@pytest.fixture()
def consent_tmp(tmp_path):
    """Set up temporary dirs with a dataset that has cached insights and suggestions."""
    original_ds = main_module.DATASETS_DIR
    original_ref = main_module.REFERENCES_DIR
    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    ref_dir = tmp_path / "references"
    ref_dir.mkdir()

    # Create a minimal dataset with cached insights and suggestions
    test_ds = ds_dir / "consent_test"
    test_ds.mkdir()
    df = pd.DataFrame({"drug": ["A", "B", "C"], "paid": [100, 200, 300]})
    df.to_parquet(str(test_ds / "source.parquet"), index=False)

    meta = {
        "row_count": 3,
        "column_count": 2,
        "ai_consent": True,
    }
    (test_ds / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")

    # Pre-cache insights and suggestions so we don't need OpenAI
    ctx = {
        "suggested_questions": ["What drug has highest paid?", "Show top drugs"],
        "insights_synopsis": "Test synopsis",
        "insights": [
            {
                "type": "concentration",
                "headline": "Drug B dominates spending",
                "explanation": "Drug B accounts for 50% of total",
                "sql": "SELECT drug, paid FROM dataset ORDER BY paid DESC",
                "chart_type": "bar",
                "priority": 1,
            }
        ],
    }
    (test_ds / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")

    main_module.DATASETS_DIR = ds_dir
    main_module.REFERENCES_DIR = ref_dir
    yield ds_dir
    main_module.DATASETS_DIR = original_ds
    main_module.REFERENCES_DIR = original_ref


@pytest.fixture()
def consent_client(consent_tmp):
    with TestClient(main_module.app) as c:
        yield c


# --- Test 14: insights with ai_consent=false ---
def test_insights_blocked_when_consent_false(consent_client, consent_tmp):
    # Set consent to false
    consent_client.post(
        "/api/datasets/consent_test/ai_consent",
        json={"ai_consent": False},
    )
    resp = consent_client.get("/api/ai/insights?dataset=consent_test")
    # If backend enforced consent, it would return empty insights or 403
    data = resp.json()
    assert resp.status_code != 200 or len(data.get("insights", [])) == 0


# --- Test 15: insights with ai_consent=true returns cached insights ---
def test_insights_returned_when_consent_true(consent_client, consent_tmp):
    consent_client.post(
        "/api/datasets/consent_test/ai_consent",
        json={"ai_consent": True},
    )
    resp = consent_client.get("/api/ai/insights?dataset=consent_test")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["insights"]) >= 1
    assert data["insights"][0]["headline"] == "Drug B dominates spending"


# --- Test 16: suggest_questions with ai_consent=false ---
def test_suggestions_blocked_when_consent_false(consent_client, consent_tmp):
    consent_client.post(
        "/api/datasets/consent_test/ai_consent",
        json={"ai_consent": False},
    )
    resp = consent_client.get("/api/ai/suggest_questions?dataset=consent_test")
    data = resp.json()
    assert resp.status_code != 200 or len(data.get("questions", [])) == 0


# --- Test 17: ai_consent field exposed in GET /api/datasets/{name}/meta ---
def test_ai_consent_exposed_in_meta(consent_client, consent_tmp):
    # Set consent to false and check meta
    consent_client.post(
        "/api/datasets/consent_test/ai_consent",
        json={"ai_consent": False},
    )
    resp = consent_client.get("/api/datasets/consent_test/meta")
    assert resp.status_code == 200
    assert resp.json()["ai_consent"] is False

    # Set consent to true and check meta
    consent_client.post(
        "/api/datasets/consent_test/ai_consent",
        json={"ai_consent": True},
    )
    resp = consent_client.get("/api/datasets/consent_test/meta")
    assert resp.status_code == 200
    assert resp.json()["ai_consent"] is True
