"""
test_passport.py — pytest test suite for GET /api/datasets/{name}/passport

Run from project root:
    pytest tests/test_passport.py -v

Three fixture datasets cover different risk surfaces:

  FIXTURE_NAME        — 1 000 rows, mixed types, known outliers / nulls / asterisks
  CMS_FIXTURE_NAME    — 3 000 rows, CMS-style column names and medical data patterns
  SINGLE_ROW_NAME     — 1 row, exercises edge-case quantile / sample-value handling

All three live in the same temp directory so DATASETS_DIR is patched once.
"""
from __future__ import annotations

import json
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module

# ---------------------------------------------------------------------------
# DATASET NAMES
# ---------------------------------------------------------------------------

FIXTURE_NAME      = "aw_test_passport"
CMS_FIXTURE_NAME  = "aw_test_passport_cms"
SINGLE_ROW_NAME   = "aw_test_passport_single_row"

# Pre-built grain descriptions — written into dataset_context.json so the AI
# call is bypassed entirely; the caching layer returns them on every request.
FAKE_GRAIN = (
    "Each row represents a single billing transaction for a specific HCPCS code, "
    "provider, and service month. "
    "The dataset covers reimbursement activity from 2022 through 2024 "
    "across multiple service categories and payment codes."
)
CMS_GRAIN = (
    "Each row represents one provider's annual Medicare Part B spending for a "
    "single HCPCS code. "
    "The dataset covers drug reimbursement activity from 2019 through 2021 "
    "across branded and generic drugs billed to Medicare."
)
SINGLE_ROW_GRAIN = (
    "Each row represents a single drug reimbursement record for one provider "
    "and one HCPCS code in a given service year. "
    "This dataset contains one record used for unit testing edge cases."
)


# ===========================================================================
# FIXTURE 1 — MAIN (1 000 rows, mixed types, known data-quality triggers)
# ===========================================================================

def _build_fixture_rows() -> list[dict]:
    """
    category       VARCHAR  — 20% null  → high_null_rate flag
                             ~14% of non-null values end in "*"  → trailing_special_chars
    item_code      VARCHAR  — J-style codes, no measure keyword
    payment_code   VARCHAR  — contains "payment" BUT ends in "_code"  → excluded from measures
    total_paid     DOUBLE   — mostly 50–200, row 0 = 50 000  → extreme_outlier flag
    total_claims   BIGINT   — 1–20, always positive  → has_negatives = False
    balance        DOUBLE   — −500 to +1 000  → has_negatives = True
    event_ts       TIMESTAMP
    tot_spndng_2022/2023/2024  DOUBLE  → time-series family
    """
    rng = random.Random(42)
    categories = ["DrugA", "DrugB", "DrugC", "DrugD", "DrugE"]
    rows = []

    for i in range(1000):
        if i % 5 == 0:
            cat = None
        else:
            cat = categories[i % len(categories)]
            if i % 7 == 0:            # ~114/800 ≈ 14 % → above 5 % threshold
                cat = cat + "*"

        rows.append({
            "category":        cat,
            "item_code":       f"J{1000 + (i % 200):04d}",
            "payment_code":    f"PAY{i % 50:03d}",
            "total_paid":      50_000.0 if i == 0 else round(50.0 + rng.random() * 150.0, 2),
            "total_claims":    rng.randint(1, 20),
            "balance":         round(rng.uniform(-500.0, 1000.0), 2),
            "event_ts":        datetime(2022 + (i % 3), 1 + (i % 12), 1 + (i % 28)),
            "tot_spndng_2022": round(rng.random() * 10_000, 2),
            "tot_spndng_2023": round(rng.random() * 10_000, 2),
            "tot_spndng_2024": round(rng.random() * 10_000, 2),
        })

    return rows


def _create_fixture_dataset(ds_dir: Path) -> None:
    rows = _build_fixture_rows()
    df = pd.DataFrame(rows)
    df["total_paid"]      = df["total_paid"].astype("float64")
    df["total_claims"]    = df["total_claims"].astype("int64")
    df["balance"]         = df["balance"].astype("float64")
    df["tot_spndng_2022"] = df["tot_spndng_2022"].astype("float64")
    df["tot_spndng_2023"] = df["tot_spndng_2023"].astype("float64")
    df["tot_spndng_2024"] = df["tot_spndng_2024"].astype("float64")
    df["event_ts"]        = pd.to_datetime(df["event_ts"])
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    meta = {
        "row_count": len(rows), "column_count": len(df.columns),
        "columns": list(df.columns), "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    ctx = {"grain_description": FAKE_GRAIN,
           "grain_description_generated_at": datetime.now().isoformat()}
    (ds_dir / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")


# ===========================================================================
# FIXTURE 2 — CMS (3 000 rows, realistic Medicare/Medicaid column patterns)
# ===========================================================================

def _build_cms_rows() -> list[dict]:
    """
    Brnd_Name      VARCHAR  — drug names, ~17 % with trailing "*"  → trailing_special_chars
    HCPCS_Code     VARCHAR  — J-codes; matches "hcpcs_code" group priority
    NPI            VARCHAR  — mix of standard 10-digit NPIs and A-prefix Medicaid IDs
    Srvc_Yr        BIGINT   — 2019–2024  → is_year_column = True; NOT a measure
    TOTAL_PAID     DOUBLE   — spending amounts  → primary measure ("paid")
    TOTAL_CLAIMS   BIGINT   — claim counts  → secondary measure ("claims")
    TOTAL_UNIQUE_BENEFICIARIES  BIGINT  → secondary measure ("beneficiaries")
    drug_type      VARCHAR  — "branded"/"generic"; ends in "_type"  → excluded from measures
    provider_id    VARCHAR  — "PROV0001"; ends in "_id"  → excluded from measures
    ndc_flag       VARCHAR  — "Y"/"N"; ends in "_flag"; 2 distinct / 3 000 rows
                             → low_distinct_count flag
    numeric_as_text VARCHAR — 95 % of values are numeric strings  → looks_numeric flag
    all_null_col   VARCHAR  — 1 non-null value, 2 999 nulls  → high_null_rate; graceful handling
    Provider Name  VARCHAR  — space in column name  → tests SQL quoting robustness
    Tot_Spndng_2019/2020/2021  DOUBLE  → time-series family
    """
    rng = random.Random(99)
    drugs    = ["Keytruda", "Opdivo", "Humira", "Stelara", "Entyvio", "Prolia", "Xgeva"]
    hcpcs    = [f"J{129 + i:04d}" for i in range(20)]

    rows = []
    for i in range(3000):
        drug = drugs[i % len(drugs)]
        if i % 6 == 0:           # ~500 / 3 000 ≈ 16.7 %  → above 5 % threshold
            drug = drug + "*"

        # Standard 10-digit NPI vs A-prefix Medicaid provider ID.
        # Use a small repeating pool (10 standard + 5 A-prefix) so A-prefix
        # values appear with measurable frequency in the top_values distribution.
        _NPI_POOL = [
            "1000000001", "1000000002", "1000000003", "1000000004", "1000000005",
            "1000000006", "1000000007", "1000000008", "1000000009", "1000000010",
            "A500000001", "A500000002", "A500000003", "A500000004", "A500000005",
        ]
        npi = _NPI_POOL[i % len(_NPI_POOL)]

        rows.append({
            "Brnd_Name":                   drug,
            "HCPCS_Code":                  hcpcs[i % len(hcpcs)],
            "NPI":                         npi,
            "Srvc_Yr":                     2019 + (i % 6),
            "TOTAL_PAID":                  round(100.0 + rng.random() * 50_000.0, 2),
            "TOTAL_CLAIMS":                rng.randint(1, 500),
            "TOTAL_UNIQUE_BENEFICIARIES":  rng.randint(1, 200),
            "drug_type":                   "branded" if i % 3 != 0 else "generic",
            "provider_id":                 f"PROV{i % 100:04d}",
            "ndc_flag":                    "Y" if i % 2 == 0 else "N",
            # 95 % numeric strings, 5 % "N/A"  → looks_numeric_but_stored_as_text
            "numeric_as_text":             "N/A" if i % 20 == 0 else str(round(rng.random() * 1000, 2)),
            # 1 non-null / 2 999 null  → tests graceful handling of near-all-null column
            "all_null_col":                "present" if i == 0 else None,
            "Provider Name":               f"Provider {i % 50:02d}",
            "Tot_Spndng_2019":             round(rng.random() * 100_000, 2),
            "Tot_Spndng_2020":             round(rng.random() * 100_000, 2),
            "Tot_Spndng_2021":             round(rng.random() * 100_000, 2),
        })

    return rows


def _create_cms_dataset(ds_dir: Path) -> None:
    rows = _build_cms_rows()
    df = pd.DataFrame(rows)
    df["Srvc_Yr"]                    = df["Srvc_Yr"].astype("int64")
    df["TOTAL_PAID"]                 = df["TOTAL_PAID"].astype("float64")
    df["TOTAL_CLAIMS"]               = df["TOTAL_CLAIMS"].astype("int64")
    df["TOTAL_UNIQUE_BENEFICIARIES"] = df["TOTAL_UNIQUE_BENEFICIARIES"].astype("int64")
    df["Tot_Spndng_2019"]            = df["Tot_Spndng_2019"].astype("float64")
    df["Tot_Spndng_2020"]            = df["Tot_Spndng_2020"].astype("float64")
    df["Tot_Spndng_2021"]            = df["Tot_Spndng_2021"].astype("float64")
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    meta = {
        "row_count": len(rows), "column_count": len(df.columns),
        "columns": list(df.columns), "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    ctx = {"grain_description": CMS_GRAIN,
           "grain_description_generated_at": datetime.now().isoformat()}
    (ds_dir / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")


# ===========================================================================
# FIXTURE 3 — SINGLE ROW (edge-case: min == max, < 5 sample values)
# ===========================================================================

def _create_single_row_dataset(ds_dir: Path) -> None:
    df = pd.DataFrame([{
        "drug_name":    "Keytruda",
        "hcpcs_code":   "J0129",
        "total_paid":   12_345.67,
        "service_year": 2023,
    }])
    df["total_paid"]   = df["total_paid"].astype("float64")
    df["service_year"] = df["service_year"].astype("int64")
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)

    meta = {
        "row_count": 1, "column_count": len(df.columns),
        "columns": list(df.columns), "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    ctx = {"grain_description": SINGLE_ROW_GRAIN,
           "grain_description_generated_at": datetime.now().isoformat()}
    (ds_dir / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")


# ===========================================================================
# MODULE-SCOPED FIXTURES
# All three datasets live in one temp dir → DATASETS_DIR patched once.
# ===========================================================================

@pytest.fixture(scope="module")
def datasets_tmp(tmp_path_factory):
    """Create all three test datasets in a single temp directory."""
    tmp = tmp_path_factory.mktemp("aw_datasets")

    for name, builder in [
        (FIXTURE_NAME,     _create_fixture_dataset),
        (CMS_FIXTURE_NAME, _create_cms_dataset),
        (SINGLE_ROW_NAME,  _create_single_row_dataset),
    ]:
        d = tmp / name
        d.mkdir()
        builder(d)

    return tmp


@pytest.fixture(scope="module")
def client(datasets_tmp):
    """FastAPI TestClient with DATASETS_DIR patched to the temp directory."""
    original = main_module.DATASETS_DIR
    main_module.DATASETS_DIR = datasets_tmp
    with TestClient(main_module.app) as c:
        yield c
    main_module.DATASETS_DIR = original


@pytest.fixture(scope="module")
def passport(client):
    """Main fixture passport — fetched once for the module."""
    resp = client.get(f"/api/datasets/{FIXTURE_NAME}/passport")
    assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
    return resp.json()


@pytest.fixture(scope="module")
def cms_passport(client):
    """CMS fixture passport — fetched once for the module."""
    resp = client.get(f"/api/datasets/{CMS_FIXTURE_NAME}/passport")
    assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
    return resp.json()


@pytest.fixture(scope="module")
def single_row_passport(client):
    """Single-row fixture passport — fetched once for the module."""
    resp = client.get(f"/api/datasets/{SINGLE_ROW_NAME}/passport")
    assert resp.status_code == 200, f"{resp.status_code}: {resp.text}"
    return resp.json()


# ===========================================================================
# HELPERS
# ===========================================================================

def _schema_col(passport: dict, name: str) -> dict:
    for col in passport["schema"]:
        if col["column_name"] == name:
            return col
    raise KeyError(f"Column '{name}' not found in schema")


def _is_numeric_type(data_type: str) -> bool:
    t = data_type.upper()
    return any(x in t for x in ["INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL"])


def _is_varchar_type(data_type: str) -> bool:
    return "VARCHAR" in data_type.upper() or "TEXT" in data_type.upper()


def _flag_columns(passport: dict, flag: str) -> list[str]:
    return [f["column"] for f in passport["data_quality_flags"] if f["flag"] == flag]


def _flag_details(passport: dict, flag: str) -> list[dict]:
    return [f for f in passport["data_quality_flags"] if f["flag"] == flag]


# ===========================================================================
# ── SECTION A: STRUCTURAL TESTS (main fixture) ──────────────────────────────
# ===========================================================================

# Prevents regressions where a top-level key is accidentally removed or renamed
def test_top_level_keys_present(passport):
    expected = {
        "identity", "schema", "grain_description",
        "data_quality_flags", "time_series_column_families", "sql_quickstart",
    }
    assert expected == set(passport.keys())


# Prevents the identity block from omitting required fields
def test_identity_required_fields(passport):
    for field in ("dataset_name", "row_count", "column_count",
                  "source_file_type", "import_date", "file_size_bytes"):
        assert field in passport["identity"], f"Missing identity field: {field}"


# Prevents silent empty-dataset responses
def test_identity_row_count_positive(passport):
    assert passport["identity"]["row_count"] > 0


# Prevents column_count from drifting out of sync with the schema list
def test_identity_column_count_matches_schema(passport):
    assert passport["identity"]["column_count"] == len(passport["schema"])


# Prevents file_size_bytes from being zero or None when a Parquet file exists
def test_identity_file_size_bytes_positive(passport):
    assert passport["identity"]["file_size_bytes"] > 0


# ===========================================================================
# ── SECTION B: GRAIN DESCRIPTION ────────────────────────────────────────────
# ===========================================================================

# Prevents grain_description from being absent or empty
def test_grain_description_non_empty(passport):
    assert isinstance(passport["grain_description"], str)
    assert passport["grain_description"].strip() != ""


# Prevents one-sentence truncations or placeholder strings from passing
def test_grain_description_at_least_20_words(passport):
    words = passport["grain_description"].split()
    assert len(words) >= 20, (
        f"grain_description only {len(words)} words: {passport['grain_description']}"
    )


# ===========================================================================
# ── SECTION C: NUMERIC COLUMN ACCURACY ──────────────────────────────────────
# ===========================================================================

# Prevents numeric_range from silently returning {} for BIGINT or DOUBLE columns
def test_every_numeric_column_has_non_empty_numeric_range(passport):
    for col in passport["schema"]:
        if _is_numeric_type(col["data_type"]):
            nr = col.get("numeric_range")
            assert nr, (
                f"'{col['column_name']}' ({col['data_type']}) has empty numeric_range"
            )


# Prevents numeric_range from omitting any required sub-field
def test_numeric_range_has_all_required_fields(passport):
    required = {"min", "max", "mean", "has_negatives", "is_year_column"}
    for col in passport["schema"]:
        if _is_numeric_type(col["data_type"]) and col.get("numeric_range"):
            missing = required - set(col["numeric_range"].keys())
            assert not missing, (
                f"'{col['column_name']}' numeric_range missing fields: {missing}"
            )


# Prevents has_negatives from being wrong for a column with known negative values
def test_balance_column_has_negatives_true(passport):
    assert _schema_col(passport, "balance")["numeric_range"]["has_negatives"] is True


# Prevents has_negatives from firing on a column that is always positive
def test_total_claims_has_negatives_false(passport):
    assert _schema_col(passport, "total_claims")["numeric_range"]["has_negatives"] is False


# Prevents is_year_column from firing on obviously non-year numeric columns
def test_non_year_columns_not_flagged_as_year(passport):
    for name in ("total_paid", "total_claims", "balance"):
        nr = _schema_col(passport, name)["numeric_range"]
        assert nr["is_year_column"] is False, f"'{name}' incorrectly flagged as year column"


# Prevents numeric min from being larger than max (query result ordering bug)
def test_numeric_range_min_lte_max(passport):
    for col in passport["schema"]:
        nr = col.get("numeric_range")
        if nr and nr.get("min") is not None and nr.get("max") is not None:
            try:
                assert float(nr["min"]) <= float(nr["max"]), (
                    f"'{col['column_name']}' min ({nr['min']}) > max ({nr['max']})"
                )
            except (TypeError, ValueError):
                pass


# ===========================================================================
# ── SECTION D: VARCHAR / DISTRIBUTION ACCURACY ──────────────────────────────
# ===========================================================================

# Prevents distribution block from being absent on any VARCHAR column
def test_every_varchar_column_has_distribution(passport):
    for col in passport["schema"]:
        if _is_varchar_type(col["data_type"]):
            dist = col.get("distribution")
            assert dist, f"'{col['column_name']}' (VARCHAR) has no distribution"
            assert "top_values" in dist
            assert "distinct_count" in dist


# Prevents top_values from being over-populated (limit should be 15)
def test_distribution_top_values_at_most_15(passport):
    for col in passport["schema"]:
        dist = col.get("distribution")
        if dist:
            assert len(dist["top_values"]) <= 15, (
                f"'{col['column_name']}' has {len(dist['top_values'])} top_values, max is 15"
            )


# Prevents distinct_count from being zero for a column that has non-null data
def test_distinct_count_positive_for_non_null_columns(passport):
    for col in passport["schema"]:
        dist = col.get("distribution")
        if dist and col["null_pct"] < 99.0:
            assert dist["distinct_count"] > 0, (
                f"'{col['column_name']}' has distinct_count=0 but null_pct={col['null_pct']}"
            )


# ===========================================================================
# ── SECTION E: SAMPLE VALUES ─────────────────────────────────────────────────
# ===========================================================================

# Prevents sample_values from being short or empty for well-populated columns
def test_all_columns_have_five_sample_values(passport):
    for col in passport["schema"]:
        sv = col.get("sample_values", [])
        assert len(sv) == 5, (
            f"'{col['column_name']}' has {len(sv)} sample values, expected 5"
        )


# Prevents null values from appearing in sample_values list
def test_sample_values_are_all_non_null(passport):
    for col in passport["schema"]:
        for v in col.get("sample_values", []):
            assert v is not None, f"'{col['column_name']}' has null in sample_values"


# Prevents the sampler from reading only from the top of the file — for a column
# with 1 000 distinct float values in range 50–200, all 5 samples being identical
# would only happen if sampling is broken (top-of-file bias)
def test_sample_values_for_total_paid_not_all_identical(passport):
    sv = _schema_col(passport, "total_paid")["sample_values"]
    assert len(set(sv)) > 1, f"total_paid sample_values all identical: {sv}"


# Prevents null_pct from being wildly wrong for a column with a known 20 % null rate
def test_null_pct_approximately_correct_for_category(passport):
    col = _schema_col(passport, "category")
    assert 15.0 <= col["null_pct"] <= 25.0, (
        f"category null_pct={col['null_pct']} expected ~20 %"
    )


# ===========================================================================
# ── SECTION F: SQL QUICKSTART ────────────────────────────────────────────────
# ===========================================================================

# Prevents select_all from being missing or malformed
def test_sql_quickstart_has_select_all(passport):
    qs = passport["sql_quickstart"]
    assert "select_all" in qs
    assert "FROM dataset" in qs["select_all"]
    assert "LIMIT" in qs["select_all"]


# Prevents select_all from silently dropping columns
def test_select_all_references_all_columns(passport):
    qs = passport["sql_quickstart"]["select_all"]
    for col in passport["schema"]:
        assert col["column_name"] in qs, (
            f"select_all does not reference column '{col['column_name']}'"
        )


# Prevents aggregate_by_top_category from disappearing when measure columns exist
def test_sql_quickstart_has_aggregate_query(passport):
    qs = passport["sql_quickstart"]
    assert "aggregate_by_top_category" in qs, (
        f"aggregate_by_top_category missing; keys: {list(qs.keys())}"
    )


# Prevents GROUP BY and ORDER BY from being silently dropped from the aggregate
def test_aggregate_query_contains_group_by_and_order_by(passport):
    agg = passport["sql_quickstart"]["aggregate_by_top_category"]
    assert "GROUP BY" in agg
    assert "ORDER BY" in agg


# Prevents identifier/code columns from leaking into measure_columns via keyword match
def test_measure_columns_exclude_identifier_suffix_columns(passport):
    for col in passport["sql_quickstart"].get("measure_columns", []):
        for sfx in ("_type", "_id", "_code", "_flag"):
            assert not col.lower().endswith(sfx), (
                f"Measure column '{col}' has identifier suffix '{sfx}'"
            )


# Prevents payment_code specifically from being misclassified as a financial measure
def test_payment_code_not_in_measure_columns(passport):
    assert "payment_code" not in passport["sql_quickstart"].get("measure_columns", [])


# Prevents a DOUBLE or FLOAT column from being chosen as the GROUP BY dimension
def test_group_column_is_not_float_or_double(passport):
    group = passport["sql_quickstart"].get("group_column")
    if group is None:
        pytest.skip("No group_column present")
    col = _schema_col(passport, group)
    assert "DOUBLE" not in col["data_type"].upper() and "FLOAT" not in col["data_type"].upper(), (
        f"group_column '{group}' is floating-point type '{col['data_type']}'"
    )


# Prevents lower-priority measures (claims/count) from displacing the primary
# paid/spend/cost/amount column in the ORDER BY clause
def test_aggregate_orders_by_paid_before_claims(passport):
    agg = passport["sql_quickstart"]["aggregate_by_top_category"]
    assert "total_paid" in agg.lower(), (
        f"Expected ORDER BY to reference total_paid:\n{agg}"
    )


# ===========================================================================
# ── SECTION G: DATA QUALITY FLAGS (main fixture) ────────────────────────────
# ===========================================================================

# Prevents high_null_rate from failing to fire on a column with 20 % nulls
def test_high_null_rate_flag_on_category(passport):
    flagged = _flag_columns(passport, "high_null_rate")
    assert "category" in flagged, (
        f"Expected high_null_rate on 'category' (20 % nulls); flagged: {flagged}"
    )


# Prevents extreme_outlier from failing to fire when max is ~400× the median
def test_extreme_outlier_flag_on_total_paid(passport):
    flagged = _flag_columns(passport, "extreme_outlier")
    assert "total_paid" in flagged, (
        f"Expected extreme_outlier on 'total_paid' (50 000 vs ~125 median); flagged: {flagged}"
    )


# Prevents trailing_special_chars from missing asterisk-contaminated values
def test_trailing_special_chars_flag_on_category(passport):
    flagged = _flag_columns(passport, "trailing_special_chars")
    assert "category" in flagged, (
        f"Expected trailing_special_chars on 'category' (~14 % asterisk); flagged: {flagged}"
    )


# Prevents extreme_outlier from firing on a column with no outliers (false positive)
def test_extreme_outlier_does_not_fire_on_total_claims(passport):
    flagged = _flag_columns(passport, "extreme_outlier")
    assert "total_claims" not in flagged, (
        "'total_claims' (range 1–20, no outliers) should not trigger extreme_outlier"
    )


# ===========================================================================
# ── SECTION H: TIME-SERIES FAMILIES (main fixture) ──────────────────────────
# ===========================================================================

# Prevents year-suffix family detection from missing the tot_spndng group
def test_time_series_family_detected(passport):
    bases = [f["base_pattern"] for f in passport["time_series_column_families"]]
    assert "tot_spndng" in bases, f"Expected 'tot_spndng' family; found: {bases}"


# Prevents a family from being reported with fewer than 2 years
def test_time_series_family_has_multiple_years(passport):
    for fam in passport["time_series_column_families"]:
        assert fam["column_count"] >= 2, (
            f"Family '{fam['base_pattern']}' has < 2 years: {fam['years']}"
        )


# Prevents years from being returned in arbitrary order
def test_time_series_years_in_ascending_order(passport):
    for fam in passport["time_series_column_families"]:
        assert fam["years"] == sorted(fam["years"]), (
            f"Family '{fam['base_pattern']}' years not sorted: {fam['years']}"
        )


# ===========================================================================
# ── SECTION I: ERROR HANDLING ────────────────────────────────────────────────
# ===========================================================================

# Prevents a nonexistent dataset from returning 200 instead of 404
def test_nonexistent_dataset_returns_404(client):
    resp = client.get("/api/datasets/does_not_exist_xyz/passport")
    assert resp.status_code == 404


# ===========================================================================
# ── SECTION J: CMS / MEDICAL DATA ACCURACY ──────────────────────────────────
# ===========================================================================

# Prevents asterisk contamination (CMS drug name trailing "*") from going undetected
def test_cms_brnd_name_asterisk_triggers_trailing_special_chars(cms_passport):
    flagged = _flag_columns(cms_passport, "trailing_special_chars")
    assert "Brnd_Name" in flagged, (
        f"Expected trailing_special_chars on 'Brnd_Name' (~17 % asterisk); flagged: {flagged}"
    )


# Prevents asterisk-contaminated names from being hidden — they must appear in top_values
# so analysts see the data quality problem when reviewing the passport
def test_cms_asterisk_values_visible_in_distribution(cms_passport):
    dist = _schema_col(cms_passport, "Brnd_Name")["distribution"]
    top_vals = [entry["value"] for entry in dist["top_values"]]
    asterisk_vals = [v for v in top_vals if v and v.endswith("*")]
    assert len(asterisk_vals) > 0, (
        f"No asterisk-contaminated values visible in Brnd_Name top_values: {top_vals}"
    )


# Prevents non-standard NPI values (A-prefix Medicaid IDs) from being hidden —
# they must appear in the distribution so analysts know they exist
def test_cms_nonstandard_npi_visible_in_distribution(cms_passport):
    dist = _schema_col(cms_passport, "NPI")["distribution"]
    top_vals = [entry["value"] for entry in dist["top_values"]]
    nonstandard = [v for v in top_vals if v and v.startswith("A")]
    assert len(nonstandard) > 0, (
        f"No A-prefix Medicaid NPIs visible in NPI top_values: {top_vals}"
    )


# Prevents a year column (Srvc_Yr: 2019–2024) from being misidentified as a measure
def test_cms_year_column_not_in_measure_columns(cms_passport):
    measure_cols = cms_passport["sql_quickstart"].get("measure_columns", [])
    assert "Srvc_Yr" not in measure_cols, (
        "Srvc_Yr contains year values (2019–2024) and must not be treated as a financial measure"
    )


# Prevents is_year_column from being False for a BIGINT column whose values are
# entirely within 1900–2100 (the service year detection heuristic)
def test_cms_srvc_yr_identified_as_year_column(cms_passport):
    nr = _schema_col(cms_passport, "Srvc_Yr")["numeric_range"]
    assert nr["is_year_column"] is True, (
        f"Srvc_Yr (values 2019–2024) should have is_year_column=True; got: {nr}"
    )


# Prevents drug_type (_type suffix) from appearing in measure_columns
def test_cms_drug_type_excluded_from_measures(cms_passport):
    assert "drug_type" not in cms_passport["sql_quickstart"].get("measure_columns", [])


# Prevents provider_id (_id suffix) from appearing in measure_columns
def test_cms_provider_id_excluded_from_measures(cms_passport):
    assert "provider_id" not in cms_passport["sql_quickstart"].get("measure_columns", [])


# Prevents ndc_flag (_flag suffix) from appearing in measure_columns
def test_cms_ndc_flag_excluded_from_measures(cms_passport):
    assert "ndc_flag" not in cms_passport["sql_quickstart"].get("measure_columns", [])


# Prevents HCPCS_Code from being excluded as group_col just because it ends in "_code"
# — it should still be selected because it matches the "hcpcs_code" priority keyword
def test_cms_hcpcs_code_is_group_column(cms_passport):
    group = cms_passport["sql_quickstart"].get("group_column")
    assert group == "HCPCS_Code", (
        f"Expected HCPCS_Code as group_column (highest priority match); got: {group!r}"
    )


# Prevents TOTAL_PAID (primary: contains "paid") from being ordered after
# TOTAL_CLAIMS or TOTAL_UNIQUE_BENEFICIARIES (secondary measures) in the aggregate
def test_cms_aggregate_orders_by_total_paid_not_claims(cms_passport):
    agg = cms_passport["sql_quickstart"]["aggregate_by_top_category"]
    assert "TOTAL_PAID" in agg, (
        f"Expected ORDER BY to reference TOTAL_PAID (primary measure):\n{agg}"
    )


# Prevents a numeric column that stores TEXT values from going undetected —
# analysts need to know the column type is wrong so they don't do arithmetic on it
def test_cms_numeric_as_text_triggers_looks_numeric_flag(cms_passport):
    flagged = _flag_columns(cms_passport, "looks_numeric_but_stored_as_text")
    assert "numeric_as_text" in flagged, (
        f"Expected looks_numeric_but_stored_as_text on 'numeric_as_text'; flagged: {flagged}"
    )


# Prevents low_distinct_count from failing to fire on a near-binary column
# (ndc_flag has only 2 values across 3 000 rows = 0.067 % < 0.1 % threshold)
def test_cms_low_distinct_count_flag_fires_on_binary_column(cms_passport):
    flagged = _flag_columns(cms_passport, "low_distinct_count")
    assert "ndc_flag" in flagged, (
        f"Expected low_distinct_count on 'ndc_flag' (2 distinct / 3 000 rows); flagged: {flagged}"
    )


# Prevents a near-all-null column (99.97 % null) from crashing the endpoint
def test_cms_near_all_null_column_handled_gracefully(cms_passport):
    col = _schema_col(cms_passport, "all_null_col")
    assert col["null_pct"] > 90.0, (
        f"all_null_col should have null_pct > 90; got {col['null_pct']}"
    )
    assert isinstance(col["sample_values"], list)  # must not crash, list may be short


# Prevents column names containing a space from crashing the SQL quoting logic
def test_cms_column_with_space_in_name_present_in_schema(cms_passport):
    col_names = [c["column_name"] for c in cms_passport["schema"]]
    assert "Provider Name" in col_names, (
        f"'Provider Name' (space in column name) missing from schema: {col_names}"
    )


# Prevents "Provider Name" from silently losing its distribution due to quoting failure
def test_cms_column_with_space_has_distribution(cms_passport):
    col = _schema_col(cms_passport, "Provider Name")
    assert col.get("distribution"), (
        "'Provider Name' (space in column name) has no distribution — possible SQL quoting failure"
    )


# Prevents the CMS time-series family (Tot_Spndng_2019/2020/2021) from going undetected
def test_cms_time_series_family_detected(cms_passport):
    bases = [f["base_pattern"] for f in cms_passport["time_series_column_families"]]
    assert "Tot_Spndng" in bases, (
        f"Expected 'Tot_Spndng' time-series family; found: {bases}"
    )


# Prevents Srvc_Yr from being included in the time-series family detection
# (it doesn't have a base_year_year suffix pattern — it IS the year column)
def test_cms_srvc_yr_not_a_time_series_family(cms_passport):
    for fam in cms_passport["time_series_column_families"]:
        assert fam["base_pattern"] != "Srvc", (
            "Srvc_Yr should not create a spurious 'Srvc' time-series family"
        )


# ===========================================================================
# ── SECTION K: ROBUSTNESS EDGE CASES (single-row dataset) ───────────────────
# ===========================================================================

# Prevents a 1-row dataset from crashing the endpoint (quantile / stats edge case)
def test_single_row_dataset_returns_200(single_row_passport):
    # fixture setup asserts 200; reaching this line means it passed
    assert "schema" in single_row_passport


# Prevents min/max from diverging when there is exactly one data point
def test_single_row_numeric_min_equals_max(single_row_passport):
    for col in single_row_passport["schema"]:
        nr = col.get("numeric_range")
        if nr and nr.get("min") is not None and nr.get("max") is not None:
            assert nr["min"] == nr["max"], (
                f"Single-row dataset: '{col['column_name']}' min ({nr['min']}) "
                f"!= max ({nr['max']})"
            )


# Prevents the sampler from crashing or returning zero values on a 1-row dataset
def test_single_row_each_column_has_at_least_one_sample_value(single_row_passport):
    for col in single_row_passport["schema"]:
        sv = col.get("sample_values", [])
        assert len(sv) >= 1, (
            f"Single-row dataset: '{col['column_name']}' has no sample values"
        )


# Prevents service_year (2023) from being misidentified as a financial measure
def test_single_row_service_year_not_a_measure(single_row_passport):
    measures = single_row_passport["sql_quickstart"].get("measure_columns", [])
    assert "service_year" not in measures, (
        "service_year (a year value) should not be treated as a financial measure"
    )


# Prevents service_year from having is_year_column=False when its only value is 2023
def test_single_row_service_year_is_year_column(single_row_passport):
    nr = _schema_col(single_row_passport, "service_year")["numeric_range"]
    assert nr["is_year_column"] is True, (
        f"service_year=2023 should have is_year_column=True; got: {nr}"
    )


# ===========================================================================
# FIXTURE 4 — ROLLUP ROWS (1000 rows, has 'Overall' rollup values)
# ===========================================================================

ROLLUP_FIXTURE_NAME = "aw_test_passport_rollup"
ROLLUP_GRAIN = (
    "Each row represents one manufacturer's annual drug spending. "
    "Contains rollup rows where Mftr_Name = 'Overall'."
)


def _build_rollup_rows() -> list[dict]:
    """
    Mftr_Name  VARCHAR — 'Overall' appears in ~50% of rows (rollup/subtotal)
                         Other manufacturers appear ~3% each
    Drug       VARCHAR — drug names, no rollup
    Spending   DOUBLE  — spending amounts
    """
    rng = random.Random(77)
    manufacturers = ["Pfizer", "Roche", "Novartis", "AbbVie", "Merck",
                     "Lilly", "AstraZeneca", "BMS", "Amgen", "Sanofi"]
    drugs = ["DrugA", "DrugB", "DrugC", "DrugD", "DrugE"]
    rows = []
    for i in range(1000):
        # 500 rows = 'Overall' (rollup), 500 rows = real manufacturers
        if i < 500:
            mftr = "Overall"
        else:
            mftr = manufacturers[i % len(manufacturers)]
        rows.append({
            "Mftr_Name": mftr,
            "Drug": drugs[i % len(drugs)],
            "Spending": round(1000 + rng.random() * 50000, 2),
        })
    return rows


def _create_rollup_dataset(ds_dir: Path) -> None:
    rows = _build_rollup_rows()
    df = pd.DataFrame(rows)
    df["Spending"] = df["Spending"].astype("float64")
    df.to_parquet(str(ds_dir / "source.parquet"), index=False)
    meta = {
        "row_count": len(rows), "column_count": len(df.columns),
        "columns": list(df.columns), "original_type": "csv",
        "created_at": datetime.now().isoformat(),
    }
    (ds_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (ds_dir / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    ctx = {"grain_description": ROLLUP_GRAIN,
           "grain_description_generated_at": datetime.now().isoformat()}
    (ds_dir / "dataset_context.json").write_text(json.dumps(ctx), encoding="utf-8")


@pytest.fixture(scope="module")
def rollup_passport(datasets_tmp):
    d = datasets_tmp / ROLLUP_FIXTURE_NAME
    d.mkdir(exist_ok=True)
    _create_rollup_dataset(d)
    with TestClient(main_module.app) as c:
        resp = c.get(f"/api/datasets/{ROLLUP_FIXTURE_NAME}/passport")
        assert resp.status_code == 200
        return resp.json()


# ===========================================================================
# ROLLUP DETECTION TESTS
# ===========================================================================


def test_rollup_flag_detected(rollup_passport):
    """Mftr_Name = 'Overall' in 50% of rows should trigger possible_rollup_rows."""
    flags = rollup_passport.get("data_quality_flags", [])
    rollup_flags = [f for f in flags if f["flag"] == "possible_rollup_rows"]
    assert len(rollup_flags) >= 1, (
        f"Expected possible_rollup_rows flag for Mftr_Name='Overall'; "
        f"got flags: {[f['flag'] for f in flags]}"
    )


def test_rollup_flag_on_correct_column(rollup_passport):
    flags = rollup_passport.get("data_quality_flags", [])
    rollup_flags = [f for f in flags if f["flag"] == "possible_rollup_rows"]
    columns = [f["column"] for f in rollup_flags]
    assert "Mftr_Name" in columns


def test_rollup_flag_detail_mentions_value(rollup_passport):
    flags = rollup_passport.get("data_quality_flags", [])
    rollup_flags = [f for f in flags if f["flag"] == "possible_rollup_rows"]
    assert any("Overall" in f["detail"] for f in rollup_flags)


def test_no_rollup_flag_on_normal_column(rollup_passport):
    """Drug column has no rollup terms — should NOT be flagged."""
    flags = rollup_passport.get("data_quality_flags", [])
    rollup_flags = [f for f in flags
                    if f["flag"] == "possible_rollup_rows" and f["column"] == "Drug"]
    assert len(rollup_flags) == 0


def test_no_rollup_on_main_fixture(passport):
    """Main fixture has no rollup rows — should not have the flag."""
    flags = passport.get("data_quality_flags", [])
    rollup_flags = [f for f in flags if f["flag"] == "possible_rollup_rows"]
    assert len(rollup_flags) == 0
