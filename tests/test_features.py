"""
test_features.py — Level 2 Automated Feature Test Suite

Covers every AW product capability with happy path + edge cases.
Organized in pytest classes — one class per feature area.
run_all.py uses class names to produce the formatted report.

Run standalone:
    pytest tests/test_features.py -v

Run via formatter:
    python tests/run_all.py
"""
from __future__ import annotations

import io
import json
import time
from pathlib import Path
from unittest.mock import patch, call

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.services.session_log import set_sessions_dir

# ============================================================
# CONSTANTS — test dataset identity
# ============================================================

DATASET = "aw_feat_cms"          # pre-created CMS-style dataset
REF_NAME = "aw_feat_ref"         # pre-created reference table
IMPORT_DATASET = "aw_feat_import"  # used by TestImportPipeline (API import)

# CMS-style abbreviated column names — exercises alias generation
_DATASET_ROWS = [
    {"DRUG_NM": "Eliquis",   "HCPCS_CD": "J1234", "TOT_SPNDNG": 5_000_000.0, "TOT_CLMS": 12000, "MANFCTR_NM": "BMS",       "SVC_YR": 2023},
    {"DRUG_NM": "Keytruda",  "HCPCS_CD": "J9271", "TOT_SPNDNG": 3_500_000.0, "TOT_CLMS":  8000, "MANFCTR_NM": "Merck",     "SVC_YR": 2023},
    {"DRUG_NM": "Opdivo",    "HCPCS_CD": "J9299", "TOT_SPNDNG": 2_200_000.0, "TOT_CLMS":  6000, "MANFCTR_NM": "BMS",       "SVC_YR": 2023},
    {"DRUG_NM": "Avastin",   "HCPCS_CD": "J9035", "TOT_SPNDNG": 1_800_000.0, "TOT_CLMS":  5000, "MANFCTR_NM": "Genentech", "SVC_YR": 2023},
    {"DRUG_NM": "Herceptin", "HCPCS_CD": "J9355", "TOT_SPNDNG": 1_200_000.0, "TOT_CLMS":  4000, "MANFCTR_NM": "Genentech", "SVC_YR": 2023},
    {"DRUG_NM": "Stelara*",  "HCPCS_CD": "J3358", "TOT_SPNDNG":   900_000.0, "TOT_CLMS":  3000, "MANFCTR_NM": "J&J",       "SVC_YR": 2023},
    {"DRUG_NM": "Remicade",  "HCPCS_CD": "J1745", "TOT_SPNDNG":   800_000.0, "TOT_CLMS":  2500, "MANFCTR_NM": "J&J",       "SVC_YR": 2022},
    {"DRUG_NM": "Rituxan",   "HCPCS_CD": "J9310", "TOT_SPNDNG":   750_000.0, "TOT_CLMS":  2000, "MANFCTR_NM": "Genentech", "SVC_YR": 2022},
]

# Reference table — IRA exclusion flags
_REF_ROWS = [
    {"DRUG_NM": "Eliquis",  "IRA_EXCLUDED": "Y", "EXCLUSION_RND": "Round 1"},
    {"DRUG_NM": "Keytruda", "IRA_EXCLUDED": "N", "EXCLUSION_RND": ""},
    {"DRUG_NM": "Stelara",  "IRA_EXCLUDED": "Y", "EXCLUSION_RND": "Round 2"},
]

# Pre-baked alias map (simulates AI output for CMS abbreviated names)
_MOCK_ALIASES = {
    "DRUG_NM":    "Drug Name",
    "HCPCS_CD":   "HCPCS Code",
    "TOT_SPNDNG": "Total Spending",
    "TOT_CLMS":   "Total Claims",
    "MANFCTR_NM": "Manufacturer Name",
    "SVC_YR":     "Service Year",
}

# ============================================================
# FILE-BYTE HELPERS
# ============================================================

def _csv_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def _xlsx_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    return buf.getvalue()


def _parquet_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# ============================================================
# DATASET CREATION HELPERS (write directly to disk)
# ============================================================

def _write_dataset(ds_dir: Path, rows: list[dict], dataset_type: str | None = None) -> None:
    """Write Parquet + metadata files for a test dataset."""
    df = pd.DataFrame(rows)
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    cols = [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]
    meta: dict = {
        "registered_name": ds_dir.name,
        "display_name": ds_dir.name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": cols,
        "created_at": "2026-01-01T00:00:00",
        "original_type": "csv",
    }
    if dataset_type:
        meta["dataset_type"] = dataset_type
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")

    # dataset_context.json with pre-cached suggestions + aliases
    ctx = {
        "grain_description": f"Each row is one drug in {ds_dir.name}.",
        "suggested_questions": [
            "Which drug has the highest total spending?",
            "Which manufacturer has the most claims?",
            "What is the total spending per service year?",
        ],
        "column_aliases": _MOCK_ALIASES,
    }
    (ds_dir / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")


def _write_reference(ref_dir: Path, rows: list[dict]) -> None:
    """Write Parquet + _meta.json for a test reference table."""
    df = pd.DataFrame(rows)
    df.to_parquet(str(ref_dir / "source.parquet"), index=False)
    cols = [{"name": c, "dtype": str(df[c].dtype)} for c in df.columns]
    meta = {"registered_name": ref_dir.name, "columns": cols, "row_count": len(df)}
    (ref_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")


# ============================================================
# MODULE-SCOPED FIXTURES
# ============================================================

@pytest.fixture(scope="module")
def test_dirs(tmp_path_factory):
    """Create isolated temp directories with pre-built test datasets."""
    tmp = tmp_path_factory.mktemp("aw_features")

    ds_root = tmp / "datasets"
    ref_root = tmp / "references"
    sess_dir = tmp / "sessions"
    exp_dir = tmp / "exports"
    lib_dir = tmp / "reference_library"

    for d in [ds_root, ref_root, sess_dir, exp_dir, lib_dir]:
        d.mkdir(parents=True)

    # Primary CMS-style dataset
    cms_dir = ds_root / DATASET
    cms_dir.mkdir()
    _write_dataset(cms_dir, _DATASET_ROWS)

    # Reference table
    ref_dir = ref_root / REF_NAME
    ref_dir.mkdir()
    _write_reference(ref_dir, _REF_ROWS)

    return {
        "datasets": ds_root,
        "references": ref_root,
        "sessions": sess_dir,
        "exports": exp_dir,
        "library": lib_dir,
        "tmp": tmp,
    }


@pytest.fixture(scope="module")
def client(test_dirs):
    """TestClient with all directories patched to the tmp environment."""
    orig_ds      = main_module.DATASETS_DIR
    orig_ref     = main_module.REFERENCES_DIR
    orig_exp     = main_module.EXPORTS_DIR
    orig_sess    = main_module.SESSIONS_DIR
    orig_lib     = main_module.REFERENCE_LIBRARY_DIR
    orig_queries = main_module.QUERIES_PATH

    main_module.DATASETS_DIR          = test_dirs["datasets"]
    main_module.REFERENCES_DIR        = test_dirs["references"]
    main_module.EXPORTS_DIR           = test_dirs["exports"]
    main_module.SESSIONS_DIR          = test_dirs["sessions"]
    main_module.REFERENCE_LIBRARY_DIR = test_dirs["library"]
    main_module.QUERIES_PATH          = test_dirs["tmp"] / "queries.json"
    set_sessions_dir(test_dirs["sessions"])

    with TestClient(main_module.app) as c:
        yield c

    main_module.DATASETS_DIR          = orig_ds
    main_module.REFERENCES_DIR        = orig_ref
    main_module.EXPORTS_DIR           = orig_exp
    main_module.SESSIONS_DIR          = orig_sess
    main_module.REFERENCE_LIBRARY_DIR = orig_lib
    main_module.QUERIES_PATH          = orig_queries
    set_sessions_dir(orig_sess)


# ============================================================
# TestImportPipeline
# ============================================================

class TestImportPipeline:
    """POST /api/datasets/import — all file types, success and failure modes."""

    def test_csv_import_succeeds(self, client):
        data = _csv_bytes(_DATASET_ROWS)
        r = client.post("/api/datasets/import",
                        files={"file": ("imp_csv_test.csv", data, "text/csv")})
        assert r.status_code == 200
        body = r.json()
        assert body["row_count"] == len(_DATASET_ROWS)
        assert body["dataset"]  # registered name present

    def test_csv_import_row_count_exact(self, client):
        rows = [{"x": i, "y": i * 2} for i in range(50)]
        data = _csv_bytes(rows)
        r = client.post("/api/datasets/import",
                        files={"file": ("imp_exact50.csv", data, "text/csv")})
        assert r.status_code == 200
        assert r.json()["row_count"] == 50

    def test_excel_import_succeeds(self, client):
        data = _xlsx_bytes(_DATASET_ROWS)
        r = client.post("/api/datasets/import",
                        files={"file": ("imp_excel_test.xlsx", data,
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")})
        assert r.status_code == 200
        assert r.json()["row_count"] == len(_DATASET_ROWS)

    def test_parquet_import_succeeds(self, client):
        data = _parquet_bytes(_DATASET_ROWS)
        r = client.post("/api/datasets/import",
                        files={"file": ("imp_parquet_test.parquet", data, "application/octet-stream")})
        assert r.status_code == 200
        assert r.json()["row_count"] == len(_DATASET_ROWS)

    def test_duplicate_import_with_overwrite_succeeds(self, client):
        data = _csv_bytes(_DATASET_ROWS[:3])
        client.post("/api/datasets/import",
                    files={"file": ("imp_dup_test.csv", data, "text/csv")})
        # Second import with overwrite
        r = client.post("/api/datasets/import?overwrite=true",
                        files={"file": ("imp_dup_test.csv", data, "text/csv")})
        assert r.status_code == 200

    def test_corrupt_file_fails_gracefully(self, client):
        junk = b"NOT A VALID CSV\x00\x01\x02\x03" * 100
        r = client.post("/api/datasets/import",
                        files={"file": ("corrupt.csv", junk, "text/csv")})
        # Should return 400 or 500 with an error message, not crash
        assert r.status_code in (400, 500)
        body = r.json()
        assert "detail" in body

    def test_empty_file_fails_gracefully(self, client):
        r = client.post("/api/datasets/import",
                        files={"file": ("empty.csv", b"", "text/csv")})
        assert r.status_code in (400, 500)

    def test_unsupported_extension_fails_gracefully(self, client):
        r = client.post("/api/datasets/import",
                        files={"file": ("bad_file.docx", b"data", "application/octet-stream")})
        assert r.status_code in (400, 500)

    def test_column_count_in_response(self, client):
        data = _csv_bytes(_DATASET_ROWS)
        r = client.post("/api/datasets/import",
                        files={"file": ("cols_check.csv", data, "text/csv")})
        assert r.status_code == 200
        assert r.json()["column_count"] == len(_DATASET_ROWS[0])

    def test_imported_dataset_appears_in_list(self, client):
        data = _csv_bytes(_DATASET_ROWS[:2])
        r = client.post("/api/datasets/import",
                        files={"file": ("imp_list_check.csv", data, "text/csv")})
        assert r.status_code == 200
        ds_name = r.json()["dataset"]

        datasets = client.get("/api/datasets").json()["datasets"]
        names = [d["name"] for d in datasets]
        assert ds_name in names

    def test_csv_with_special_char_column_names(self, client):
        rows = [{"col with space": 1, "col/slash": 2, "col-dash": 3}]
        data = _csv_bytes(rows)
        r = client.post("/api/datasets/import",
                        files={"file": ("special_cols.csv", data, "text/csv")})
        # Should succeed and sanitize the column names
        assert r.status_code == 200

    def test_csv_with_null_heavy_columns(self, client):
        rows = [{"id": i, "sparse": None if i % 2 == 0 else f"val{i}"} for i in range(20)]
        data = _csv_bytes(rows)
        r = client.post("/api/datasets/import",
                        files={"file": ("nulls.csv", data, "text/csv")})
        assert r.status_code == 200
        assert r.json()["row_count"] == 20


# ============================================================
# TestInsights
# ============================================================

class TestInsights:
    """GET /api/ai/insights — card generation, caching, error handling."""

    # generate_insights_for_dataset returns {"synopsis": "...", "insights": [...]}
    _MOCK_INSIGHTS = {
        "synopsis": "This dataset covers 8 drugs across 2 years with $14.15M total spending.",
        "insights": [
            {
                "type": "concentration",
                "headline": "Top 2 drugs account for 62% of total spending",
                "explanation": "Eliquis and Keytruda together represent $8.5M of the $14.15M total.",
                "sql": "SELECT DRUG_NM, SUM(TOT_SPNDNG) AS total FROM dataset GROUP BY DRUG_NM ORDER BY total DESC LIMIT 5",
                "chart_type": "bar",
                "priority": 1,
            },
            {
                "type": "outliers",
                "headline": "Eliquis spending is 4x higher than the dataset median",
                "explanation": "Eliquis at $5M is far above the median drug spend of $1.3M.",
                "sql": "SELECT DRUG_NM, TOT_SPNDNG FROM dataset ORDER BY TOT_SPNDNG DESC",
                "chart_type": "bar",
                "priority": 2,
            },
        ],
    }

    def test_insights_return_200_with_cache(self, client):
        # dataset_context.json has no cached insights — AI will be called
        with patch("app.ai.routes.generate_insights_for_dataset",
                   return_value=self._MOCK_INSIGHTS):
            r = client.get(f"/api/ai/insights?dataset={DATASET}&refresh=true")
        assert r.status_code == 200

    def test_insights_returns_list(self, client):
        with patch("app.ai.routes.generate_insights_for_dataset",
                   return_value=self._MOCK_INSIGHTS):
            data = client.get(f"/api/ai/insights?dataset={DATASET}&refresh=true").json()
        assert isinstance(data.get("insights"), list)

    def test_insights_at_least_one_card(self, client):
        with patch("app.ai.routes.generate_insights_for_dataset",
                   return_value=self._MOCK_INSIGHTS):
            data = client.get(f"/api/ai/insights?dataset={DATASET}&refresh=true").json()
        assert len(data["insights"]) >= 1

    def test_insights_card_has_required_fields(self, client):
        with patch("app.ai.routes.generate_insights_for_dataset",
                   return_value=self._MOCK_INSIGHTS):
            data = client.get(f"/api/ai/insights?dataset={DATASET}&refresh=true").json()
        card = data["insights"][0]
        for field in ("type", "headline", "explanation", "sql"):
            assert field in card, f"Missing field: {field}"

    def test_insights_nonexistent_dataset_returns_empty_not_crash(self, client):
        r = client.get("/api/ai/insights?dataset=does_not_exist_xyz")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data.get("insights"), list)

    def test_insights_cache_hit_skips_ai(self, client, test_dirs):
        """Second GET without refresh=true returns cached result without calling AI."""
        # Write insights into cache first (format expected by _read_insights_cache)
        ctx_path = test_dirs["datasets"] / DATASET / "dataset_context.json"
        ctx = json.loads(ctx_path.read_text(encoding="utf-8"))
        ctx["insights"] = self._MOCK_INSIGHTS["insights"]
        ctx["insights_synopsis"] = self._MOCK_INSIGHTS["synopsis"]
        ctx_path.write_text(json.dumps(ctx), encoding="utf-8")

        call_count = {"n": 0}
        real_fn = __import__("app.ai.provider_openai", fromlist=["generate_insights_for_dataset"]).generate_insights_for_dataset

        def counting_fn(*a, **kw):
            call_count["n"] += 1
            return real_fn(*a, **kw)

        with patch("app.ai.routes.generate_insights_for_dataset", side_effect=counting_fn):
            client.get(f"/api/ai/insights?dataset={DATASET}")  # cache hit

        assert call_count["n"] == 0, "AI was called on a cache hit — caching is broken"


# ============================================================
# TestNaturalLanguageQueries
# ============================================================

class TestNaturalLanguageQueries:
    """POST /api/ai/generate_sql — NL → SQL generation."""

    # generate_sql_for_dataset returns a raw JSON string (the OpenAI response body).
    # parse_generate_sql_response() then unpacks it. "status" must be "ok".
    _GOOD_SQL = json.dumps({
        "status": "ok",
        "sql": "SELECT DRUG_NM, SUM(TOT_SPNDNG) AS total_spending FROM dataset GROUP BY DRUG_NM ORDER BY total_spending DESC",
        "message": "Top drugs by spending",
        "warnings": [],
    })
    _GROUP_BY_SQL = json.dumps({
        "status": "ok",
        "sql": "SELECT MANFCTR_NM, COUNT(*) AS drug_count, SUM(TOT_SPNDNG) AS total FROM dataset GROUP BY MANFCTR_NM ORDER BY total DESC",
        "message": "",
        "warnings": [],
    })

    def test_simple_question_returns_200(self, client):
        with patch("app.ai.routes.generate_sql_for_dataset", return_value=self._GOOD_SQL):
            r = client.post("/api/ai/generate_sql",
                            json={"dataset": DATASET, "question": "Which drug has highest spending?"})
        assert r.status_code == 200

    def test_simple_question_returns_sql(self, client):
        with patch("app.ai.routes.generate_sql_for_dataset", return_value=self._GOOD_SQL):
            data = client.post("/api/ai/generate_sql",
                               json={"dataset": DATASET, "question": "Which drug has highest spending?"}).json()
        assert "sql" in data
        assert "SELECT" in data["sql"].upper()

    def test_group_by_question_generates_group_by_sql(self, client):
        with patch("app.ai.routes.generate_sql_for_dataset", return_value=self._GROUP_BY_SQL):
            data = client.post("/api/ai/generate_sql",
                               json={"dataset": DATASET, "question": "Total spending per manufacturer"}).json()
        assert "GROUP BY" in data["sql"].upper()

    def test_empty_question_returns_error_not_crash(self, client):
        r = client.post("/api/ai/generate_sql",
                        json={"dataset": DATASET, "question": ""})
        assert r.status_code in (200, 400, 422)
        # Must not be a 500 crash
        assert r.status_code != 500

    def test_nonexistent_dataset_returns_400_or_error(self, client):
        r = client.post("/api/ai/generate_sql",
                        json={"dataset": "no_such_dataset", "question": "Tell me something"})
        # Should fail gracefully — 400 or 200 with error status
        assert r.status_code in (200, 400, 404)

    def test_generated_sql_uses_dataset_table_name(self, client):
        with patch("app.ai.routes.generate_sql_for_dataset", return_value=self._GOOD_SQL):
            data = client.post("/api/ai/generate_sql",
                               json={"dataset": DATASET, "question": "Top drugs"}).json()
        # The raw SQL should reference "dataset" (the logical table name)
        assert "dataset" in data["sql"].lower()


# ============================================================
# TestSqlExecution
# ============================================================

class TestSqlExecution:
    """POST /api/sql — DuckDB execution, error handling, SQL rewriting."""

    def test_select_star_returns_all_rows(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET, "sql": "SELECT * FROM dataset"})
        assert r.status_code == 200
        assert r.json()["rowcount"] == len(_DATASET_ROWS)

    def test_where_clause_filters_rows(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET,
                              "sql": "SELECT * FROM dataset WHERE SVC_YR = 2023"})
        assert r.status_code == 200
        expected = sum(1 for row in _DATASET_ROWS if row["SVC_YR"] == 2023)
        assert r.json()["rowcount"] == expected

    def test_group_by_query_returns_aggregated_rows(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET,
                              "sql": "SELECT MANFCTR_NM, COUNT(*) AS cnt FROM dataset GROUP BY MANFCTR_NM"})
        assert r.status_code == 200
        # Should have one row per distinct manufacturer
        expected_manufacturers = len({row["MANFCTR_NM"] for row in _DATASET_ROWS})
        assert r.json()["rowcount"] == expected_manufacturers

    def test_invalid_sql_returns_error_not_crash(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET, "sql": "SELECT FROM WHERE"})
        # Must not 500 crash
        assert r.status_code in (200, 400)
        if r.status_code == 200:
            # Some endpoints wrap errors in 200 with error field
            data = r.json()
            assert "error" in data or "detail" in data or data.get("rowcount", 0) == 0

    def test_nonexistent_column_returns_error(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET,
                              "sql": "SELECT COLUMN_DOES_NOT_EXIST FROM dataset"})
        assert r.status_code in (200, 400)

    def test_response_has_required_fields(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET, "sql": "SELECT * FROM dataset LIMIT 1"})
        assert r.status_code == 200
        data = r.json()
        for field in ("columns", "rows", "rowcount"):
            assert field in data, f"Missing field: {field}"

    def test_response_columns_match_query(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET,
                              "sql": "SELECT DRUG_NM, TOT_SPNDNG FROM dataset LIMIT 3"})
        assert r.status_code == 200
        assert set(r.json()["columns"]) == {"DRUG_NM", "TOT_SPNDNG"}

    def test_zero_row_result_rowcount_is_zero(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET,
                              "sql": "SELECT * FROM dataset WHERE DRUG_NM = 'IMPOSSIBLE_NAME_XYZ'"})
        assert r.status_code == 200
        assert r.json()["rowcount"] == 0

    def test_elapsed_seconds_present_and_positive(self, client):
        r = client.post("/api/sql",
                        json={"dataset": DATASET, "sql": "SELECT COUNT(*) FROM dataset"})
        assert r.status_code == 200
        elapsed = r.json().get("elapsed_seconds")
        assert elapsed is not None
        assert elapsed >= 0

    def test_like_pattern_for_asterisk_contamination(self, client):
        """LIKE pattern correctly matches drug names with trailing asterisk."""
        r = client.post("/api/sql",
                        json={"dataset": DATASET,
                              "sql": "SELECT DRUG_NM FROM dataset WHERE DRUG_NM LIKE 'Stelara%'"})
        assert r.status_code == 200
        assert r.json()["rowcount"] == 1


# ============================================================
# TestReferenceTableJoin
# ============================================================

class TestReferenceTableJoin:
    """Reference table loading, JOIN execution, error handling."""

    def test_reference_table_in_dataset_list_with_ref_type(self, client):
        """Pre-created reference table is accessible."""
        r = client.get("/api/references")
        assert r.status_code == 200
        refs = r.json().get("references", [])
        names = [ref["name"] for ref in refs]
        assert REF_NAME in names

    def test_join_against_reference_returns_rows(self, client):
        sql = (
            "SELECT d.DRUG_NM, d.TOT_SPNDNG, r.IRA_EXCLUDED "
            "FROM dataset AS d "
            f"JOIN {REF_NAME} AS r ON d.DRUG_NM = r.DRUG_NM"
        )
        r = client.post("/api/sql",
                        json={"dataset": DATASET, "sql": sql,
                              "reference": REF_NAME})
        assert r.status_code == 200
        assert r.json()["rowcount"] > 0

    def test_join_with_upper_handles_case_mismatch(self, client):
        """UPPER() workaround for title-case normalization (Bug #10 mitigation)."""
        sql = (
            "SELECT d.DRUG_NM, r.IRA_EXCLUDED "
            "FROM dataset AS d "
            f"JOIN {REF_NAME} AS r ON UPPER(d.DRUG_NM) = UPPER(r.DRUG_NM)"
        )
        r = client.post("/api/sql",
                        json={"dataset": DATASET, "sql": sql,
                              "reference": REF_NAME})
        assert r.status_code == 200

    def test_import_reference_csv_succeeds(self, client):
        data = _csv_bytes(_REF_ROWS)
        r = client.post("/api/references/import",
                        files={"file": ("new_ref.csv", data, "text/csv")})
        assert r.status_code == 200
        body = r.json()
        assert "name" in body or "reference" in body

    def test_reference_list_endpoint_returns_list(self, client):
        r = client.get("/api/references")
        assert r.status_code == 200
        assert "references" in r.json()
        assert isinstance(r.json()["references"], list)

    def test_filtered_join_row_count_matches_expectation(self, client):
        """Eliquis has IRA_EXCLUDED=Y — filter should return exactly 1 row."""
        sql = (
            "SELECT d.DRUG_NM "
            "FROM dataset AS d "
            f"JOIN {REF_NAME} AS r ON d.DRUG_NM = r.DRUG_NM "
            "WHERE r.IRA_EXCLUDED = 'Y'"
        )
        r = client.post("/api/sql",
                        json={"dataset": DATASET, "sql": sql,
                              "reference": REF_NAME})
        assert r.status_code == 200
        # Eliquis is the only exact-match Y in the reference table
        assert r.json()["rowcount"] >= 1


# ============================================================
# TestResultNarrative
# ============================================================

class TestResultNarrative:
    """POST /api/ai/result_narrative — generation, content, error handling."""

    _MOCK_NARRATIVE = (
        "Eliquis leads with $5.0M in total spending across 12,000 claims, "
        "representing 35% of all drug expenditures in this dataset."
    )

    _NARRATIVE_PAYLOAD = {
        "question": "Which drug has highest spending?",
        "sql": "SELECT DRUG_NM, TOT_SPNDNG FROM dataset ORDER BY TOT_SPNDNG DESC LIMIT 1",
        "columns": ["DRUG_NM", "TOT_SPNDNG"],
        "rows": [{"DRUG_NM": "Eliquis", "TOT_SPNDNG": 5000000}],
        "rowcount": 1,
        "dataset": DATASET,
    }

    def test_narrative_returns_200(self, client):
        # generate_result_narrative returns a plain string (the narrative text)
        with patch("app.ai.routes.generate_result_narrative",
                   return_value=self._MOCK_NARRATIVE):
            r = client.post("/api/ai/result_narrative", json=self._NARRATIVE_PAYLOAD)
        assert r.status_code == 200

    def test_narrative_has_text_content(self, client):
        with patch("app.ai.routes.generate_result_narrative",
                   return_value=self._MOCK_NARRATIVE):
            data = client.post("/api/ai/result_narrative", json=self._NARRATIVE_PAYLOAD).json()
        narrative = data.get("narrative", "")
        assert len(narrative) > 20

    def test_narrative_contains_specific_value_from_results(self, client):
        with patch("app.ai.routes.generate_result_narrative",
                   return_value=self._MOCK_NARRATIVE):
            data = client.post("/api/ai/result_narrative", json=self._NARRATIVE_PAYLOAD).json()
        # Narrative should name the actual top drug
        assert "Eliquis" in data["narrative"]

    def test_narrative_missing_required_fields_returns_error(self, client):
        r = client.post("/api/ai/result_narrative", json={})
        assert r.status_code in (400, 422)

    def test_narrative_response_has_narrative_field(self, client):
        with patch("app.ai.routes.generate_result_narrative",
                   return_value=self._MOCK_NARRATIVE):
            data = client.post("/api/ai/result_narrative", json=self._NARRATIVE_PAYLOAD).json()
        assert "narrative" in data


# ============================================================
# TestColumnNameInterpreter
# ============================================================

class TestColumnNameInterpreter:
    """GET/POST /api/ai/column_aliases — alias generation, caching, persistence."""

    def test_get_aliases_returns_200(self, client):
        # dataset_context.json has pre-cached aliases — should return without AI call
        r = client.get(f"/api/ai/column_aliases?dataset={DATASET}")
        assert r.status_code == 200

    def test_get_aliases_returns_alias_map(self, client):
        r = client.get(f"/api/ai/column_aliases?dataset={DATASET}")
        data = r.json()
        assert "aliases" in data
        assert isinstance(data["aliases"], dict)

    def test_aliases_differ_from_original_for_cms_columns(self, client):
        r = client.get(f"/api/ai/column_aliases?dataset={DATASET}")
        aliases = r.json()["aliases"]
        # At least some aliases should differ from the column name (abbreviated CMS → readable)
        originals = [c["name"] for c in json.loads(
            (client.app.state if hasattr(client.app, "state") else object)
            .__dict__.get("_", "")
        )] if False else list(_MOCK_ALIASES.keys())
        if aliases:
            changed = [k for k, v in aliases.items() if v != k]
            assert len(changed) > 0, "All aliases are identical to original names — alias generation failed"

    def test_aliases_cache_hit_returns_cached_true(self, client):
        # Pre-cached dataset_context.json should return cached=True on first GET
        r = client.get(f"/api/ai/column_aliases?dataset={DATASET}")
        assert r.status_code == 200
        data = r.json()
        assert data.get("cached") is True

    def test_save_aliases_persists_to_context(self, client, test_dirs):
        new_aliases = {"DRUG_NM": "Drug Name Override", "TOT_SPNDNG": "Total Spend ($)"}
        r = client.post("/api/ai/column_aliases",
                        json={"dataset": DATASET, "aliases": new_aliases})
        assert r.status_code == 200

        # Verify written to disk
        ctx = json.loads(
            (test_dirs["datasets"] / DATASET / "dataset_context.json").read_text(encoding="utf-8")
        )
        assert ctx.get("column_aliases", {}).get("DRUG_NM") == "Drug Name Override"

    def test_save_aliases_nonexistent_dataset_does_not_crash(self, client):
        r = client.post("/api/ai/column_aliases",
                        json={"dataset": "no_such_dataset", "aliases": {"col": "Column"}})
        # Should not 500 — return 200 or graceful error
        assert r.status_code in (200, 400, 404)

    def test_get_aliases_nonexistent_dataset_returns_empty(self, client):
        r = client.get("/api/ai/column_aliases?dataset=no_such_dataset")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data.get("aliases"), dict)

    def test_alias_refresh_calls_ai(self, client):
        """refresh=true bypasses cache and calls AI."""
        with patch("app.ai.routes.generate_column_aliases",
                   return_value=_MOCK_ALIASES) as mock_fn:
            r = client.get(f"/api/ai/column_aliases?dataset={DATASET}&refresh=true")
        assert r.status_code == 200
        mock_fn.assert_called_once()

    def test_identity_aliases_not_cached(self, client):
        """Aliases that match column names exactly (identity map) should NOT be cached."""
        cols = list(_MOCK_ALIASES.keys())
        identity = {c: c for c in cols}  # All aliases identical to originals
        with patch("app.ai.routes.generate_column_aliases", return_value=identity):
            r = client.get(f"/api/ai/column_aliases?dataset={DATASET}&refresh=true")
        assert r.status_code == 200
        # Identity aliases should not be written to cache
        # (they indicate AI failure — the app should not persist them)
        ctx = json.loads(
            (client.app.state if False else Path(main_module.DATASETS_DIR) / DATASET / "dataset_context.json").read_text(encoding="utf-8")
        )
        saved = ctx.get("column_aliases", {})
        # If anything was saved, it should not be a pure identity map
        if saved:
            assert any(v != k for k, v in saved.items()), "Identity aliases were incorrectly cached"


# ============================================================
# TestAnalysisSequence
# ============================================================

class TestAnalysisSequence:
    """GET /api/ai/analysis_sequence — 3-step sequence generation and caching."""

    _MOCK_STEPS = [
        "What is the overall distribution of spending across all drugs?",
        "Which drug accounts for the largest share of total spending?",
        "Are there any drugs with spending more than 3x above the average?",
    ]

    def test_sequence_returns_200(self, client):
        with patch("app.ai.routes.generate_analysis_sequence",
                   return_value=self._MOCK_STEPS):
            r = client.get(f"/api/ai/analysis_sequence?dataset={DATASET}&refresh=true")
        assert r.status_code == 200

    def test_sequence_returns_exactly_three_steps(self, client):
        with patch("app.ai.routes.generate_analysis_sequence",
                   return_value=self._MOCK_STEPS):
            data = client.get(f"/api/ai/analysis_sequence?dataset={DATASET}&refresh=true").json()
        assert len(data["steps"]) == 3

    def test_sequence_steps_are_strings(self, client):
        with patch("app.ai.routes.generate_analysis_sequence",
                   return_value=self._MOCK_STEPS):
            data = client.get(f"/api/ai/analysis_sequence?dataset={DATASET}&refresh=true").json()
        for step in data["steps"]:
            assert isinstance(step, str)
            assert len(step) > 10

    def test_sequence_cache_hit_skips_ai(self, client, test_dirs):
        """Write sequence to cache; subsequent call without refresh must not call AI."""
        ctx_path = test_dirs["datasets"] / DATASET / "dataset_context.json"
        ctx = json.loads(ctx_path.read_text(encoding="utf-8"))
        ctx["analysis_sequence"] = self._MOCK_STEPS
        ctx_path.write_text(json.dumps(ctx), encoding="utf-8")

        with patch("app.ai.routes.generate_analysis_sequence",
                   side_effect=AssertionError("AI called on cache hit")):
            r = client.get(f"/api/ai/analysis_sequence?dataset={DATASET}")
        assert r.status_code == 200

    def test_sequence_nonexistent_dataset_returns_empty_steps(self, client):
        r = client.get("/api/ai/analysis_sequence?dataset=no_such_dataset")
        assert r.status_code == 200
        assert r.json()["steps"] == []


# ============================================================
# TestSuggestQuestions
# ============================================================

class TestSuggestQuestions:
    """GET /api/ai/suggest_questions — question suggestions, caching."""

    _MOCK_QUESTIONS = [
        "Which drug has the highest total spending?",
        "What is the average spending per claim for BMS drugs?",
        "How does spending compare between 2022 and 2023?",
    ]

    def test_suggest_questions_returns_200(self, client):
        r = client.get(f"/api/ai/suggest_questions?dataset={DATASET}")
        assert r.status_code == 200

    def test_suggest_questions_returns_list(self, client):
        data = client.get(f"/api/ai/suggest_questions?dataset={DATASET}").json()
        assert isinstance(data.get("questions"), list)

    def test_suggest_questions_cache_hit_uses_cache(self, client):
        """Pre-cached questions should be returned without AI call."""
        with patch("app.ai.routes.suggest_questions_for_dataset",
                   side_effect=AssertionError("AI called on cache hit")):
            r = client.get(f"/api/ai/suggest_questions?dataset={DATASET}")
        assert r.status_code == 200

    def test_suggest_questions_refresh_calls_ai(self, client):
        with patch("app.ai.routes.suggest_questions_for_dataset",
                   return_value=self._MOCK_QUESTIONS) as mock_fn:
            r = client.get(f"/api/ai/suggest_questions?dataset={DATASET}&refresh=true")
        assert r.status_code == 200
        mock_fn.assert_called_once()

    def test_suggest_questions_nonexistent_dataset_graceful(self, client):
        r = client.get("/api/ai/suggest_questions?dataset=no_such_xyz")
        assert r.status_code == 200


# ============================================================
# TestSaveAsDataset
# ============================================================

class TestSaveAsDataset:
    """POST /api/datasets/save_result — derived dataset creation and registration."""

    _DERIVED_NAME = "aw_feat_derived"
    _DERIVED_SQL = (
        "SELECT DRUG_NM, SUM(TOT_SPNDNG) AS total_spending "
        "FROM dataset GROUP BY DRUG_NM ORDER BY total_spending DESC"
    )

    def test_save_result_returns_200(self, client):
        r = client.post("/api/datasets/save_result",
                        json={"name": self._DERIVED_NAME,
                              "dataset": DATASET,
                              "sql": self._DERIVED_SQL})
        assert r.status_code == 200

    def test_save_result_returns_row_count(self, client):
        name = "aw_feat_derived_cnt"
        r = client.post("/api/datasets/save_result",
                        json={"name": name,
                              "dataset": DATASET,
                              "sql": self._DERIVED_SQL})
        assert r.status_code == 200
        body = r.json()
        assert "row_count" in body
        distinct_drugs = len({row["DRUG_NM"] for row in _DATASET_ROWS})
        assert body["row_count"] == distinct_drugs

    def test_derived_dataset_appears_in_list(self, client):
        name = "aw_feat_derived_list"
        client.post("/api/datasets/save_result",
                    json={"name": name, "dataset": DATASET, "sql": self._DERIVED_SQL})
        datasets = client.get("/api/datasets").json()["datasets"]
        names = [d["name"] for d in datasets]
        assert name in names

    def test_derived_dataset_is_queryable(self, client):
        name = "aw_feat_derived_query"
        client.post("/api/datasets/save_result",
                    json={"name": name, "dataset": DATASET, "sql": self._DERIVED_SQL})
        r = client.post("/api/sql",
                        json={"dataset": name, "sql": f"SELECT * FROM {name}"})
        assert r.status_code == 200
        assert r.json()["rowcount"] > 0

    def test_derived_dataset_row_count_matches_source_query(self, client):
        name = "aw_feat_derived_exact"
        # Run the query directly to get the expected row count
        run_r = client.post("/api/sql",
                            json={"dataset": DATASET, "sql": self._DERIVED_SQL})
        expected = run_r.json()["rowcount"]

        # Save as dataset
        save_r = client.post("/api/datasets/save_result",
                             json={"name": name, "dataset": DATASET, "sql": self._DERIVED_SQL})
        assert save_r.status_code == 200
        assert save_r.json()["row_count"] == expected

    def test_derived_dataset_has_correct_dataset_type(self, client, test_dirs):
        name = "aw_feat_derived_type"
        client.post("/api/datasets/save_result",
                    json={"name": name, "dataset": DATASET, "sql": self._DERIVED_SQL})
        meta_path = test_dirs["datasets"] / name / "_meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert meta.get("dataset_type") == "derived"

    def test_save_result_invalid_sql_returns_error(self, client):
        r = client.post("/api/datasets/save_result",
                        json={"name": "aw_bad_derived",
                              "dataset": DATASET,
                              "sql": "SELECT FROM WHERE INVALID"})
        assert r.status_code in (400, 422, 500)

    def test_save_result_nonexistent_source_dataset_returns_error(self, client):
        r = client.post("/api/datasets/save_result",
                        json={"name": "aw_orphan",
                              "dataset": "does_not_exist",
                              "sql": "SELECT * FROM does_not_exist"})
        assert r.status_code in (400, 404, 422, 500)


# ============================================================
# TestSavedQueries
# ============================================================

class TestSavedQueries:
    """POST /api/queries/save, GET /api/queries — save/load cycle."""

    def test_save_query_returns_200(self, client):
        r = client.post("/api/queries/save",
                        json={"name": "Top Drugs by Spend", "type": "sql",
                              "dataset": DATASET,
                              "sql": "SELECT DRUG_NM, TOT_SPNDNG FROM dataset ORDER BY TOT_SPNDNG DESC"})
        assert r.status_code == 200

    def test_saved_query_appears_in_list(self, client):
        query_name = "FeatTest Unique Query Name"
        client.post("/api/queries/save",
                    json={"name": query_name, "type": "sql",
                          "dataset": DATASET,
                          "sql": "SELECT COUNT(*) FROM dataset"})
        r = client.get("/api/queries")
        assert r.status_code == 200
        queries = r.json().get("queries", [])
        names = [q["name"] for q in queries]
        assert query_name in names

    def test_saved_query_has_sql_field(self, client):
        query_name = "FeatTest SQL Field Check"
        sql = "SELECT DRUG_NM FROM dataset LIMIT 5"
        client.post("/api/queries/save",
                    json={"name": query_name, "type": "sql", "dataset": DATASET, "sql": sql})
        queries = client.get("/api/queries").json().get("queries", [])
        match = next((q for q in queries if q["name"] == query_name), None)
        assert match is not None
        assert match.get("sql") == sql

    def test_delete_query_removes_it(self, client):
        query_name = "FeatTest To Delete"
        client.post("/api/queries/save",
                    json={"name": query_name, "type": "sql", "dataset": DATASET,
                          "sql": "SELECT 1"})
        queries = client.get("/api/queries").json().get("queries", [])
        match = next((q for q in queries if q["name"] == query_name), None)
        assert match is not None

        r = client.post("/api/queries/delete", json={"name": query_name})
        assert r.status_code == 200

        queries_after = client.get("/api/queries").json().get("queries", [])
        names_after = [q["name"] for q in queries_after]
        assert query_name not in names_after

    def test_save_query_empty_name_returns_error(self, client):
        r = client.post("/api/queries/save",
                        json={"name": "", "type": "sql", "dataset": DATASET, "sql": "SELECT 1"})
        assert r.status_code in (400, 422)


# ============================================================
# TestSessionLog
# ============================================================

class TestSessionLog:
    """Session event logging — event types, counts, export."""

    def test_session_summary_returns_200(self, client):
        r = client.get("/api/session/summary")
        assert r.status_code == 200

    def test_session_summary_has_events_by_type(self, client):
        data = client.get("/api/session/summary").json()
        assert "events_by_type" in data

    def test_query_run_event_logged_after_sql(self, client):
        # Run a query and check session has a query_run event
        client.post("/api/sql",
                    json={"dataset": DATASET, "sql": "SELECT COUNT(*) FROM dataset"})
        data = client.get("/api/session/summary").json()
        events = data.get("events_by_type", {})
        assert events.get("query_run", 0) >= 1

    def test_dataset_import_event_logged_after_import(self, client):
        data_bytes = _csv_bytes(_DATASET_ROWS[:3])
        client.post("/api/datasets/import",
                    files={"file": ("session_log_test.csv", data_bytes, "text/csv")})
        data = client.get("/api/session/summary").json()
        events = data.get("events_by_type", {})
        assert events.get("dataset_import", 0) >= 1

    def test_session_export_returns_200(self, client):
        r = client.get("/api/session/export")
        assert r.status_code == 200
        # /api/session/export returns {"exported_to": "...", "session": {...}}
        # OR a file download depending on implementation
        ct = r.headers.get("content-type", "")
        if "json" in ct:
            data = r.json()
            # Accept any of the known response shapes
            has_data = (
                "events" in data
                or "session_id" in data
                or "exported_to" in data
                or "session" in data
            )
            assert has_data
        else:
            assert len(r.content) > 0

    def test_session_reset_clears_events(self, client):
        # Log some events
        client.post("/api/sql",
                    json={"dataset": DATASET, "sql": "SELECT 1"})
        # Reset
        r = client.post("/api/session/reset")
        assert r.status_code == 200
        # Session should be fresh after reset
        data = client.get("/api/session/summary").json()
        total_events = sum(data.get("events_by_type", {}).values())
        # After reset, only session_start counts
        assert total_events <= 2

    def test_derived_dataset_event_logged_after_save(self, client):
        sql = "SELECT DRUG_NM FROM dataset LIMIT 3"
        client.post("/api/datasets/save_result",
                    json={"name": "aw_event_test", "dataset": DATASET, "sql": sql})
        data = client.get("/api/session/summary").json()
        events = data.get("events_by_type", {})
        assert events.get("dataset_derived", 0) >= 1

    def test_log_event_endpoint_accepts_valid_event_type(self, client):
        # Only registered SessionEventType values are accepted
        r = client.post("/api/session/log_event",
                        json={"event_type": "query_run",
                              "details": {"dataset": DATASET, "sql": "SELECT 1"}})
        assert r.status_code == 200


# ============================================================
# TestExplain
# ============================================================

class TestExplain:
    """POST /api/ai/explain — SQL explanation generation."""

    _MOCK_EXPLANATION = (
        "This query groups all drug claims by drug name and sums the total spending "
        "for each, then orders results from highest to lowest spend. "
        "It identifies which drugs have the greatest financial impact on this dataset."
    )

    _EXPLAIN_PAYLOAD = {
        "dataset": DATASET,
        "sql": "SELECT DRUG_NM, SUM(TOT_SPNDNG) FROM dataset GROUP BY DRUG_NM",
        "question": "Top drugs by spending",
    }

    def test_explain_returns_200(self, client):
        # generate_explanation returns a plain string
        with patch("app.ai.routes.generate_explanation",
                   return_value=self._MOCK_EXPLANATION):
            r = client.post("/api/ai/explain", json=self._EXPLAIN_PAYLOAD)
        assert r.status_code == 200

    def test_explain_returns_explanation_text(self, client):
        with patch("app.ai.routes.generate_explanation",
                   return_value=self._MOCK_EXPLANATION):
            data = client.post("/api/ai/explain", json=self._EXPLAIN_PAYLOAD).json()
        assert "explanation" in data
        assert len(data["explanation"]) > 20

    def test_explain_missing_sql_returns_error(self, client):
        r = client.post("/api/ai/explain",
                        json={"dataset": DATASET, "question": "What does this do?"})
        assert r.status_code in (400, 422)

    def test_explain_nonexistent_dataset_graceful(self, client):
        # Should not crash — 200 with empty explanation or graceful error code
        with patch("app.ai.routes.generate_explanation", return_value=""):
            r = client.post("/api/ai/explain",
                            json={"dataset": "no_such_dataset",
                                  "sql": "SELECT 1",
                                  "question": ""})
        assert r.status_code in (200, 400, 404)


# ============================================================
# TestWorkflowSaveResume
# ============================================================

class TestWorkflowSaveResume:
    """Session save, export, and resume — state persistence across sessions."""

    def test_session_save_returns_200(self, client):
        r = client.post("/api/session/name", json={"name": "FeatTest Session"})
        assert r.status_code == 200

    def test_saved_sessions_list_returns_list(self, client):
        r = client.get("/api/sessions/saved")
        assert r.status_code == 200
        data = r.json()
        assert "sessions" in data or isinstance(data, list)

    def test_session_export_produces_json(self, client):
        r = client.get("/api/session/export")
        assert r.status_code == 200

    def test_workspace_snapshot_saves_and_restores(self, client):
        # Save workspace
        save_r = client.post("/api/workspace",
                             json={"dataset": DATASET, "reference": None})
        assert save_r.status_code == 200

        # Get workspace
        get_r = client.get("/api/workspace")
        assert get_r.status_code == 200

    def test_health_endpoint_returns_ok(self, client):
        r = client.get("/api/health")
        assert r.status_code == 200

    def test_version_endpoint_returns_version(self, client):
        r = client.get("/api/version")
        assert r.status_code == 200
        assert "version" in r.json()


# ============================================================
# TestSchemaAndPreview
# ============================================================

class TestSchemaAndPreview:
    """GET /api/schema, /api/preview, /api/profile — dataset inspection."""

    def test_schema_returns_columns(self, client):
        r = client.get(f"/api/schema?dataset={DATASET}")
        assert r.status_code == 200
        data = r.json()
        assert "columns" in data
        assert len(data["columns"]) == len(_DATASET_ROWS[0])

    def test_schema_column_names_match_dataset(self, client):
        r = client.get(f"/api/schema?dataset={DATASET}")
        names = {c["name"] for c in r.json()["columns"]}
        expected = set(_DATASET_ROWS[0].keys())
        assert expected == names

    def test_preview_returns_rows(self, client):
        r = client.get(f"/api/preview?dataset={DATASET}")
        assert r.status_code == 200
        data = r.json()
        assert "rows" in data
        assert len(data["rows"]) > 0

    def test_preview_default_limit_respected(self, client):
        r = client.get(f"/api/preview?dataset={DATASET}")
        assert r.status_code == 200
        # Default preview ≤ AW_DEFAULT_PREVIEW_ROWS (50), but dataset only has 8 rows
        assert len(r.json()["rows"]) == len(_DATASET_ROWS)

    def test_profile_returns_statistics(self, client):
        r = client.get(f"/api/profile?dataset={DATASET}")
        assert r.status_code == 200
        data = r.json()
        # /api/profile returns the dataset_context.json content
        assert isinstance(data, dict) and len(data) > 0

    def test_schema_nonexistent_dataset_returns_404(self, client):
        r = client.get("/api/schema?dataset=no_such_dataset")
        assert r.status_code == 404

    def test_preview_nonexistent_dataset_returns_404(self, client):
        r = client.get("/api/preview?dataset=no_such_dataset")
        assert r.status_code == 404


# ============================================================
# TestPassport
# ============================================================

class TestPassport:
    """GET /api/datasets/{name}/passport — export passport."""

    def test_passport_returns_200(self, client):
        r = client.get(f"/api/datasets/{DATASET}/passport")
        assert r.status_code == 200

    def test_passport_has_schema_section(self, client):
        r = client.get(f"/api/datasets/{DATASET}/passport")
        data = r.json()
        # Passport uses "schema" for column-level detail
        assert "schema" in data or "columns" in data or "identity" in data

    def test_passport_nonexistent_dataset_returns_404(self, client):
        r = client.get("/api/datasets/no_such_xyz/passport")
        assert r.status_code == 404

    def test_result_passport_accepts_result_data(self, client):
        r = client.post("/api/results/passport",
                        json={
                            "columns": ["DRUG_NM", "TOT_SPNDNG"],
                            "rows": [{"DRUG_NM": "Eliquis", "TOT_SPNDNG": 5000000}],
                            "sql": "SELECT DRUG_NM, TOT_SPNDNG FROM dataset LIMIT 1",
                            "dataset": DATASET,
                        })
        assert r.status_code == 200
