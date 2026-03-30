"""
test_accuracy.py -- AW Query Accuracy Test Suite

Verifies that analytical SQL queries against the real synthetic CSVs
return exact or near-exact values matching pre-computed golden values.

Run as pytest:
    pytest tests/test_accuracy.py -q --tb=short

Run standalone (formatted output):
    python tests/test_accuracy.py

Set AW_RUN_AI_TESTS=1 to include live OpenAI SQL generation tests.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Path setup so tests can import app.main directly
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT / "backend"))

import app.main as main_module
from app.services.session_log import set_sessions_dir

# ---------------------------------------------------------------------------
# Golden values
# ---------------------------------------------------------------------------

_GOLDEN_PATH = Path(__file__).parent / "golden_queries.json"
GOLDEN: dict = json.loads(_GOLDEN_PATH.read_text(encoding="utf-8"))

# ---------------------------------------------------------------------------
# Example case paths
# ---------------------------------------------------------------------------

_CASES = ROOT / "data" / "example_cases"


# ===========================================================================
# Module-scoped fixture
# ===========================================================================

@pytest.fixture(scope="module")
def acc_client(tmp_path_factory):
    """
    TestClient with isolated tmp dirs, all real example-case CSVs imported.
    Module-scoped so the expensive import step runs once per test session.
    """
    tmp = tmp_path_factory.mktemp("aw_accuracy")

    ds_root  = tmp / "datasets"
    ref_root = tmp / "references"
    sess_dir = tmp / "sessions"
    exp_dir  = tmp / "exports"
    lib_dir  = tmp / "reference_library"

    for d in [ds_root, ref_root, sess_dir, exp_dir, lib_dir]:
        d.mkdir(parents=True)

    # Save originals
    orig_ds      = main_module.DATASETS_DIR
    orig_ref     = main_module.REFERENCES_DIR
    orig_exp     = main_module.EXPORTS_DIR
    orig_sess    = main_module.SESSIONS_DIR
    orig_lib     = main_module.REFERENCE_LIBRARY_DIR
    orig_queries = main_module.QUERIES_PATH

    # Patch
    main_module.DATASETS_DIR          = ds_root
    main_module.REFERENCES_DIR        = ref_root
    main_module.EXPORTS_DIR           = exp_dir
    main_module.SESSIONS_DIR          = sess_dir
    main_module.REFERENCE_LIBRARY_DIR = lib_dir
    main_module.QUERIES_PATH          = tmp / "queries.json"
    set_sessions_dir(sess_dir)

    with TestClient(main_module.app) as client:

        # ---- Dataset imports ------------------------------------------------
        _datasets_to_import = [
            (_CASES / "medicaid_pe_diligence" / "data" / "tx_medicaid_claims.csv",  "tx_medicaid_claims.csv"),
            (_CASES / "medicaid_pe_diligence" / "data" / "fl_medicaid_claims.csv",  "fl_medicaid_claims.csv"),
            (_CASES / "medicaid_pe_diligence" / "data" / "oh_medicaid_claims.csv",  "oh_medicaid_claims.csv"),
            (_CASES / "part_d_ira_exclusion"  / "data" / "part_d_spending_sample.csv", "part_d_spending_sample.csv"),
            (_CASES / "real_estate_market_analysis" / "data" / "austin_listings.csv",   "austin_listings.csv"),
            (_CASES / "real_estate_market_analysis" / "data" / "denver_listings.csv",   "denver_listings.csv"),
            (_CASES / "parameterized_workflow_retail" / "data" / "electronics_sales.csv",   "electronics_sales.csv"),
            (_CASES / "parameterized_workflow_retail" / "data" / "sporting_goods_sales.csv", "sporting_goods_sales.csv"),
        ]

        for csv_path, filename in _datasets_to_import:
            assert csv_path.exists(), f"Missing dataset CSV: {csv_path}"
            with open(csv_path, "rb") as f:
                r = client.post(
                    "/api/datasets/import",
                    files={"file": (filename, f, "text/csv")},
                )
            assert r.status_code == 200, (
                f"Dataset import failed for {filename}: {r.status_code} {r.text}"
            )

        # ---- Reference table imports ----------------------------------------
        _refs_to_import = [
            (_CASES / "part_d_ira_exclusion"  / "reference" / "ira_negotiated_drugs.csv",  "ira_negotiated_drugs.csv"),
            (_CASES / "medicaid_pe_diligence"  / "reference" / "medicaid_schema_map.csv",   "medicaid_schema_map.csv"),
            (_CASES / "medicaid_pe_diligence"  / "reference" / "audit_risk_flags.csv",      "audit_risk_flags.csv"),
        ]

        for csv_path, filename in _refs_to_import:
            assert csv_path.exists(), f"Missing reference CSV: {csv_path}"
            with open(csv_path, "rb") as f:
                r = client.post(
                    "/api/references/import",
                    files={"file": (filename, f, "text/csv")},
                )
            assert r.status_code == 200, (
                f"Reference import failed for {filename}: {r.status_code} {r.text}"
            )

        yield client

    # Restore originals
    main_module.DATASETS_DIR          = orig_ds
    main_module.REFERENCES_DIR        = orig_ref
    main_module.EXPORTS_DIR           = orig_exp
    main_module.SESSIONS_DIR          = orig_sess
    main_module.REFERENCE_LIBRARY_DIR = orig_lib
    main_module.QUERIES_PATH          = orig_queries
    set_sessions_dir(orig_sess)


# ===========================================================================
# Helper
# ===========================================================================

def run_sql(client, sql: str, dataset: str, reference: str = None) -> list[dict]:
    """Run SQL via /api/sql and return rows list."""
    body = {"sql": sql, "dataset": dataset}
    if reference:
        body["reference"] = reference
    r = client.post("/api/sql", json=body)
    assert r.status_code == 200, f"SQL failed: {r.text}\nSQL: {sql}"
    return r.json()["rows"]


# ===========================================================================
# TestAggregationAccuracy
# ===========================================================================

class TestAggregationAccuracy:

    def test_tx_row_count_exact(self, acc_client):
        rows = run_sql(acc_client, "SELECT COUNT(*) as cnt FROM dataset", "tx_medicaid_claims")
        assert rows[0]["cnt"] == GOLDEN["tx_row_count"]

    def test_fl_row_count_exact(self, acc_client):
        rows = run_sql(acc_client, "SELECT COUNT(*) as cnt FROM dataset", "fl_medicaid_claims")
        assert rows[0]["cnt"] == GOLDEN["fl_row_count"]

    def test_oh_row_count_exact(self, acc_client):
        rows = run_sql(acc_client, "SELECT COUNT(*) as cnt FROM dataset", "oh_medicaid_claims")
        assert rows[0]["cnt"] == GOLDEN["oh_row_count"]

    def test_tx_sum_paid_amt(self, acc_client):
        rows = run_sql(acc_client, "SELECT SUM(PAID_AMT) as total FROM dataset", "tx_medicaid_claims")
        actual = rows[0]["total"]
        expected = GOLDEN["tx_total_paid"]
        assert abs(actual - expected) / expected < 0.01, f"SUM off: {actual} vs {expected}"

    def test_tx_avg_reimbursement_rate(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT AVG(PAID_AMT / BILL_AMT) as rate FROM dataset WHERE BILL_AMT > 0",
            "tx_medicaid_claims")
        actual = rows[0]["rate"]
        expected = GOLDEN["tx_avg_reimbursement_rate"]
        assert abs(actual - expected) < 0.02, f"AVG rate off: {actual} vs {expected}"

    def test_top_tx_mco_by_claims(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT MCO_NAME, COUNT(*) as claims FROM dataset GROUP BY MCO_NAME ORDER BY claims DESC LIMIT 1",
            "tx_medicaid_claims")
        assert rows[0]["MCO_NAME"] == GOLDEN["top_tx_mco"]

    def test_part_d_row_count(self, acc_client):
        rows = run_sql(acc_client, "SELECT COUNT(*) as cnt FROM dataset", "part_d_spending_sample")
        assert rows[0]["cnt"] == GOLDEN["part_d_row_count"]

    def test_top_drug_by_spend_2023(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT Brnd_Name FROM dataset ORDER BY Tot_Spndng_2023 DESC LIMIT 1",
            "part_d_spending_sample")
        assert rows[0]["Brnd_Name"] == GOLDEN["top_drug_by_spend_2023"]


# ===========================================================================
# TestFilterAccuracy
# ===========================================================================

class TestFilterAccuracy:

    def test_tx_null_diag_cd_exact(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT COUNT(*) as cnt FROM dataset WHERE DIAG_CD IS NULL",
            "tx_medicaid_claims")
        assert rows[0]["cnt"] == GOLDEN["tx_null_diag_count"]

    def test_tx_anomalous_providers_exact(self, acc_client):
        rows = run_sql(acc_client, """
            SELECT PRVDR_NPI, AVG(BILL_AMT / PAID_AMT) as ratio
            FROM dataset
            WHERE PAID_AMT > 0
            GROUP BY PRVDR_NPI
            HAVING AVG(BILL_AMT / PAID_AMT) > 3.0
        """, "tx_medicaid_claims")
        assert len(rows) == GOLDEN["tx_anomalous_provider_count"]

    def test_tx_lone_star_concentration(self, acc_client):
        rows = run_sql(acc_client, """
            SELECT MCO_NAME,
                   COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as pct
            FROM dataset
            GROUP BY MCO_NAME
            ORDER BY pct DESC
            LIMIT 1
        """, "tx_medicaid_claims")
        actual_pct = rows[0]["pct"]
        expected_pct = GOLDEN["tx_lone_star_concentration_pct"]
        assert abs(actual_pct - expected_pct) < 1.0, f"Concentration off: {actual_pct} vs {expected_pct}"

    def test_fl_service_type_count(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT COUNT(DISTINCT SERVICE_TYPE) as cnt FROM dataset",
            "fl_medicaid_claims")
        assert rows[0]["cnt"] == GOLDEN["fl_service_type_count"]

    def test_fl_health_plan_count(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT COUNT(DISTINCT HEALTH_PLAN) as cnt FROM dataset",
            "fl_medicaid_claims")
        assert rows[0]["cnt"] == GOLDEN["fl_health_plan_count"]


# ===========================================================================
# TestJoinAccuracy
# ===========================================================================

class TestJoinAccuracy:

    def test_ira_exclusion_reduces_count(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT COUNT(*) as cnt FROM dataset WHERE LOWER(Brnd_Name) NOT IN (SELECT LOWER(drug_name) FROM reference)",
            "part_d_spending_sample",
            reference="ira_negotiated_drugs")
        actual = rows[0]["cnt"]
        assert actual == GOLDEN["part_d_after_ira_exclusion"], (
            f"IRA exclusion count wrong: {actual} vs {GOLDEN['part_d_after_ira_exclusion']}"
        )
        assert actual < GOLDEN["part_d_row_count"]
        assert actual > 0

    def test_schema_map_bene_id_canonical(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT m.canonical_column FROM dataset d JOIN reference m ON LOWER(m.source_column) = LOWER('BENE_ID') WHERE m.state = 'TX' LIMIT 1",
            "tx_medicaid_claims",
            reference="medicaid_schema_map")
        assert rows[0]["canonical_column"] == GOLDEN["schema_map_bene_id_canonical"]

    def test_fl_audit_risk_join_returns_matches(self, acc_client):
        rows = run_sql(acc_client,
            "SELECT COUNT(DISTINCT d.PROCEDURE_CODE) as flagged FROM dataset d INNER JOIN reference r ON LOWER(d.PROCEDURE_CODE) = LOWER(r.procedure_code)",
            "fl_medicaid_claims",
            reference="audit_risk_flags")
        assert rows[0]["flagged"] == GOLDEN["fl_audit_risk_match_count"]

    def test_join_without_manual_case_handling(self, acc_client):
        # Bug #10 fix: JOIN must work with LOWER() -- no raw equality needed
        rows = run_sql(acc_client,
            "SELECT COUNT(*) as cnt FROM dataset d JOIN reference r ON LOWER(d.Brnd_Name) = LOWER(r.drug_name)",
            "part_d_spending_sample",
            reference="ira_negotiated_drugs")
        matched = rows[0]["cnt"]
        assert matched > 0, "IRA JOIN returned zero rows -- Bug #10 may have regressed"


# ===========================================================================
# TestCrossStateAccuracy
# ===========================================================================

class TestCrossStateAccuracy:

    def test_oh_tx_reimbursement_delta(self, acc_client):
        oh_rate = GOLDEN["oh_avg_reimbursement_rate"]
        tx_rate = GOLDEN["tx_avg_reimbursement_rate"]
        delta = tx_rate - oh_rate
        assert 0.12 < delta < 0.22, f"OH/TX delta {delta:.3f} outside expected 12-22% range"

    def test_cross_state_union_row_count(self, acc_client):
        # Uses registered dataset names directly (Bug #11 fix: OH ZIP_CODE is now VARCHAR)
        rows = run_sql(acc_client, """
            SELECT 'TX' as state, COUNT(*) as cnt FROM tx_medicaid_claims
            UNION ALL
            SELECT 'FL', COUNT(*) FROM fl_medicaid_claims
            UNION ALL
            SELECT 'OH', COUNT(*) FROM oh_medicaid_claims
        """, "tx_medicaid_claims")
        assert len(rows) == 3
        total = sum(r["cnt"] for r in rows)
        expected = GOLDEN["tx_row_count"] + GOLDEN["fl_row_count"] + GOLDEN["oh_row_count"]
        assert total == expected

    def test_oh_zip_code_is_varchar(self, acc_client):
        # Bug #11 fix: ZIP_CODE must be VARCHAR (not INTEGER) after import
        rows = run_sql(acc_client,
            "SELECT typeof(ZIP_CODE) as ztype FROM dataset LIMIT 1",
            "oh_medicaid_claims")
        ztype = rows[0]["ztype"].upper()
        assert "INT" not in ztype and "BIGINT" not in ztype, f"ZIP_CODE still numeric: {ztype}"

    def test_cross_state_union_no_cast_needed(self, acc_client):
        # Must work WITHOUT ::VARCHAR cast on OH ZIP_CODE -- Bug #11 confirmed fixed
        # DuckDB requires parentheses around UNION legs that contain LIMIT
        rows = run_sql(acc_client, """
            SELECT state, zip FROM (
                (SELECT 'TX' as state, COUNTY_CD as zip FROM tx_medicaid_claims LIMIT 1)
                UNION ALL
                (SELECT 'OH' as state, ZIP_CODE as zip FROM oh_medicaid_claims LIMIT 1)
            ) sub
        """, "tx_medicaid_claims")
        assert len(rows) == 2


# ===========================================================================
# TestDerivedDatasetAccuracy
# ===========================================================================

class TestDerivedDatasetAccuracy:

    def test_save_as_dataset_preserves_values(self, acc_client):
        # Run a query, save result, re-query derived dataset, compare values
        source_sql = "SELECT Brnd_Name, Tot_Spndng_2023 FROM dataset ORDER BY Tot_Spndng_2023 DESC LIMIT 10"
        original = run_sql(acc_client, source_sql, "part_d_spending_sample")
        assert len(original) == 10

        # Save as derived dataset via the actual endpoint
        r = acc_client.post("/api/datasets/save_result", json={
            "sql": source_sql,
            "dataset": "part_d_spending_sample",
            "name": "top_10_drugs_acc_test"
        })
        assert r.status_code == 200, f"Save failed: {r.text}"

        # Query derived dataset
        derived = run_sql(acc_client, "SELECT * FROM dataset ORDER BY Tot_Spndng_2023 DESC", "top_10_drugs_acc_test")
        assert len(derived) == len(original), f"Row count mismatch: {len(derived)} vs {len(original)}"
        for i, (orig, deriv) in enumerate(zip(original, derived)):
            assert orig["Brnd_Name"] == deriv["Brnd_Name"], f"Row {i} brand mismatch"
            assert abs(float(orig["Tot_Spndng_2023"]) - float(deriv["Tot_Spndng_2023"])) < 1.0, (
                f"Row {i} spend mismatch"
            )

    def test_derived_dataset_row_count_matches_query(self, acc_client):
        source_sql = "SELECT * FROM dataset WHERE PAID_AMT > 500"
        source_rows = run_sql(acc_client, source_sql, "tx_medicaid_claims")
        source_count = len(source_rows)

        r = acc_client.post("/api/datasets/save_result", json={
            "sql": source_sql,
            "dataset": "tx_medicaid_claims",
            "name": "tx_high_paid_acc_test"
        })
        assert r.status_code == 200, f"Save failed: {r.text}"

        derived = run_sql(acc_client, "SELECT COUNT(*) as cnt FROM dataset", "tx_high_paid_acc_test")
        assert derived[0]["cnt"] == source_count


# ===========================================================================
# TestAiSqlAccuracy
# ===========================================================================

AI_TESTS = [
    {"dataset": "tx_medicaid_claims",    "question": "Which MCO has the highest number of claims?",           "expected_top_value": "Lone Star Health", "expected_column": "MCO_NAME"},
    {"dataset": "tx_medicaid_claims",    "question": "How many claims have a missing diagnosis code?",         "expected_row_count": 1, "expected_value_approx": 19,     "tolerance": 0},
    {"dataset": "tx_medicaid_claims",    "question": "Which providers have a billed to paid ratio above 3?",   "expected_row_count": 2, "tolerance": 0},
    {"dataset": "oh_medicaid_claims",    "question": "What is the average reimbursement rate?",                "expected_row_count": 1, "expected_value_approx": 0.6682, "tolerance": 0.03},
    {"dataset": "fl_medicaid_claims",    "question": "What is the total paid amount by health plan?",          "expected_row_count": 3, "tolerance": 0},
    {"dataset": "part_d_spending_sample","question": "What are the top 5 drugs by total spending in 2023?",   "expected_top_value": "Eliquis", "expected_column": "Brnd_Name"},
    {"dataset": "part_d_spending_sample","question": "How many unique manufacturers are there?",              "expected_row_count": 1},
    {"dataset": "austin_listings",       "question": "What is the average list price by neighborhood?",       "expected_row_count": 10, "tolerance": 0},
    {"dataset": "austin_listings",       "question": "Which neighborhood has the highest average days on market?", "expected_top_value": "Southeast", "expected_column": "NEIGHBORHOOD"},
    {"dataset": "electronics_sales",     "question": "What is the total revenue by region?",                  "expected_row_count": 4, "tolerance": 0},
    {"dataset": "electronics_sales",     "question": "Which region has the lowest total revenue?",            "expected_top_value": "West", "expected_column": "REGION"},
    {"dataset": "tx_medicaid_claims",    "question": "What percentage of claims are from Lone Star Health?",  "expected_row_count": 1},
    {"dataset": "tx_medicaid_claims",    "question": "What is the monthly trend in claims?",                  "expected_row_count": 12, "tolerance": 0},
    {"dataset": "part_d_spending_sample","question": "How many drugs have only one manufacturer (Tot_Mftr = 1)?", "expected_row_count": 1},
    {"dataset": "denver_listings",       "question": "What is the average price per square foot by property type?", "expected_row_count": 4, "tolerance": 0},
    {"dataset": "sporting_goods_sales",  "question": "What is the total revenue by product category?",        "expected_row_count": 5, "tolerance": 0},
    {"dataset": "tx_medicaid_claims",    "question": "Show claims where billed amount is more than 3 times the paid amount", "expected_row_count_min": 1},
    {"dataset": "fl_medicaid_claims",    "question": "What is the count of claims by service type?",          "expected_row_count": 5, "tolerance": 0},
    {"dataset": "oh_medicaid_claims",    "question": "Which managed care organization has the most claims?",  "expected_row_count": 1},
    {"dataset": "electronics_sales",     "question": "What are the top 3 products by total revenue?",         "expected_row_count": 3, "tolerance": 0},
]


class TestAiSqlAccuracy:
    # AI accuracy tests -- skip unless AW_RUN_AI_TESTS=1 env var is set.
    # These make real OpenAI API calls, so they are not run in the default pre-push hook.

    @pytest.fixture(autouse=True)
    def check_ai_enabled(self):
        if not os.getenv("AW_RUN_AI_TESTS"):
            pytest.skip("Set AW_RUN_AI_TESTS=1 to run AI accuracy tests")

    @pytest.mark.parametrize("spec", AI_TESTS, ids=[s["question"][:50] for s in AI_TESTS])
    def test_ai_query(self, acc_client, spec):
        # Call AI generate SQL endpoint (real API call -- no mock)
        r = acc_client.post("/api/ai/generate_sql", json={
            "question": spec["question"],
            "dataset": spec["dataset"]
        })
        assert r.status_code == 200
        data = r.json()
        sql = data.get("sql", "")
        assert sql, "AI returned no SQL"

        # Execute the generated SQL
        rows = run_sql(acc_client, sql, spec["dataset"])

        # Check row count if specified
        if "expected_row_count" in spec:
            assert len(rows) == spec["expected_row_count"], (
                f"Row count {len(rows)} != expected {spec['expected_row_count']} for: {spec['question']}"
            )
        if "expected_row_count_min" in spec:
            assert len(rows) >= spec["expected_row_count_min"]

        # Check top value if specified
        if "expected_top_value" in spec and "expected_column" in spec:
            col = spec["expected_column"]
            top_row = rows[0] if rows else {}
            assert top_row.get(col) == spec["expected_top_value"], (
                f"Top value {top_row.get(col)} != expected {spec['expected_top_value']} for: {spec['question']}"
            )

        # Check approximate numeric value if specified
        if "expected_value_approx" in spec and rows:
            first_numeric = next((v for v in rows[0].values() if isinstance(v, (int, float))), None)
            if first_numeric is not None:
                tol = spec.get("tolerance", 0.02)
                if tol == 0:
                    assert first_numeric == spec["expected_value_approx"]
                else:
                    assert abs(first_numeric - spec["expected_value_approx"]) <= tol, (
                        f"Value {first_numeric} vs expected {spec['expected_value_approx']} (tol {tol})"
                    )


# ===========================================================================
# Standalone runner
# ===========================================================================

_SECTION_MAP = {
    "TestAggregationAccuracy":    "AGGREGATION ACCURACY",
    "TestFilterAccuracy":         "FILTER ACCURACY",
    "TestJoinAccuracy":           "JOIN ACCURACY",
    "TestCrossStateAccuracy":     "CROSS-STATE ACCURACY",
    "TestDerivedDatasetAccuracy": "DERIVED DATASET ACCURACY",
    "TestAiSqlAccuracy":          "AI SQL ACCURACY",
}

TESTS_DIR  = Path(__file__).parent
REPORT_DIR = TESTS_DIR.parent / "reports"
REPORT_XML = REPORT_DIR / "accuracy_report.xml"

WIDTH = 63
_SEP  = "=" * WIDTH


def _ansi(code: str, text: str) -> str:
    if sys.stdout.isatty():
        return f"\033[{code}m{text}\033[0m"
    return text


GREEN  = lambda t: _ansi("32", t)
RED    = lambda t: _ansi("31", t)
YELLOW = lambda t: _ansi("33", t)
BOLD   = lambda t: _ansi("1",  t)
DIM    = lambda t: _ansi("2",  t)
CYAN   = lambda t: _ansi("36", t)


def _get_version() -> str:
    try:
        ctx = ROOT / "CONTEXT.md"
        for line in ctx.read_text(encoding="utf-8").splitlines():
            if "**Current version:**" in line:
                parts = line.split("**Current version:**")
                if len(parts) > 1:
                    return parts[1].strip().split()[0]
    except Exception:
        pass
    return "unknown"


def _readable_test_name(raw_name: str) -> str:
    name = raw_name.removeprefix("test_")
    return name.replace("_", " ")


def _get_section(classname: str) -> str:
    short = classname.rsplit(".", 1)[-1] if "." in classname else classname
    return _SECTION_MAP.get(short, short.replace("Test", "").upper().strip())


def _parse_junit_xml(xml_path: Path) -> list[dict]:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    if root.tag == "testsuites":
        suites = list(root)
    elif root.tag == "testsuite":
        suites = [root]
    else:
        suites = list(root.iter("testsuite"))

    results = []
    for suite in suites:
        for tc in suite.iter("testcase"):
            classname = tc.attrib.get("classname", "")
            name      = tc.attrib.get("name", "")
            elapsed   = float(tc.attrib.get("time", "0") or "0")

            failure = tc.find("failure")
            error   = tc.find("error")
            skipped = tc.find("skipped")

            if failure is not None:
                status  = "fail"
                message = (failure.attrib.get("message") or failure.text or "")[:200]
            elif error is not None:
                status  = "error"
                message = (error.attrib.get("message") or error.text or "")[:200]
            elif skipped is not None:
                status  = "skip"
                message = skipped.attrib.get("message", "")
            else:
                status  = "pass"
                message = ""

            results.append({
                "section":   _get_section(classname),
                "classname": classname,
                "name":      name,
                "label":     _readable_test_name(name),
                "status":    status,
                "time":      elapsed,
                "message":   message,
            })

    return results


def _status_icon(status: str) -> str:
    return {"pass": GREEN("PASS"), "fail": RED("FAIL"),
            "error": RED("ERR "), "skip": YELLOW("SKIP")}.get(status, "?   ")


def _print_result(r: dict) -> None:
    icon   = _status_icon(r["status"])
    label  = r["label"][:55]
    timing = DIM(f"{r['time']:.2f}s")

    if r["status"] in ("fail", "error") and r["message"]:
        msg = r["message"].strip().replace("\n", " ")[:80]
        print(f"  {icon} {label} {timing}")
        print(f"      {RED(msg)}")
    elif r["status"] == "skip":
        print(f"  {icon} {DIM(label)} {timing}")
    else:
        print(f"  {icon} {label} {timing}")


def _print_golden_values() -> None:
    g = GOLDEN
    tx_rate  = g["tx_avg_reimbursement_rate"]
    oh_rate  = g["oh_avg_reimbursement_rate"]
    delta    = tx_rate - oh_rate
    delta_ok = 0.12 < delta < 0.22
    null_pct = g["tx_null_diag_count"] / g["tx_row_count"] * 100

    print(BOLD("GOLDEN VALUES"))
    print(f"  TX row count:              {g['tx_row_count']}")
    print(f"  TX Lone Star Health:       {g['tx_lone_star_concentration_pct']}%")
    print(f"  TX null DIAG_CD:           {g['tx_null_diag_count']} ({null_pct:.1f}%)")
    print(f"  TX anomalous providers:    {g['tx_anomalous_provider_count']}")
    print(f"  OH avg reimbursement rate: {oh_rate * 100:.1f}%")
    print(f"  TX avg reimbursement rate: {tx_rate * 100:.1f}%")
    delta_label = f"{delta * 100:+.1f} ppts  <- {'PASS' if delta_ok else 'FAIL'} (expect 12-22%)"
    print(f"  OH vs TX delta:            {RED(delta_label) if not delta_ok else GREEN(delta_label)}")
    print()


def _print_summary(results: list[dict], total_time: float) -> None:
    passed  = sum(1 for r in results if r["status"] == "pass")
    failed  = sum(1 for r in results if r["status"] in ("fail", "error"))
    skipped = sum(1 for r in results if r["status"] == "skip")
    total   = len(results)

    print()
    print(BOLD(_SEP))
    print(BOLD("SUMMARY"))
    print("  " + "-" * (WIDTH - 2))

    # Category breakdown
    categories = list(_SECTION_MAP.values())
    for cat in categories:
        cat_results = [r for r in results if r["section"] == cat]
        if not cat_results:
            continue
        c_pass = sum(1 for r in cat_results if r["status"] == "pass")
        c_fail = sum(1 for r in cat_results if r["status"] in ("fail", "error"))
        c_skip = sum(1 for r in cat_results if r["status"] == "skip")
        line = f"  {cat:<28}  {GREEN(str(c_pass))} passed"
        if c_fail:
            line += f", {RED(str(c_fail))} failed"
        if c_skip:
            line += f", {YELLOW(str(c_skip))} skipped"
        print(line)

    print("  " + "-" * (WIDTH - 2))

    # AI accuracy note
    ai_results = [r for r in results if r["section"] == "AI SQL ACCURACY"]
    if ai_results and any(r["status"] != "skip" for r in ai_results):
        ai_pass = sum(1 for r in ai_results if r["status"] == "pass")
        ai_total = sum(1 for r in ai_results if r["status"] != "skip")
        pct = ai_pass / ai_total * 100 if ai_total else 0
        print(f"  AI accuracy rate:          {GREEN(f'{pct:.0f}%')} ({ai_pass}/{ai_total})")
    else:
        print(f"  AI tests: {YELLOW('SKIPPED')} (set AW_RUN_AI_TESTS=1)")

    print(f"  Total:                     {passed}/{total} passed  ({skipped} skipped)")
    print(f"  Duration:                  {total_time:.1f}s")
    print("  " + "-" * (WIDTH - 2))

    if failed == 0:
        print(f"  {GREEN('ALL TESTS PASSED')} [OK]")
    else:
        print(f"  {RED(f'{failed} FAILURE(S) - fix before pushing')}")

    print(BOLD(_SEP))

    failures = [r for r in results if r["status"] in ("fail", "error")]
    if failures:
        print()
        print(BOLD(RED("FAILURES (fix before pushing):")))
        for i, r in enumerate(failures, 1):
            label = r["label"]
            msg   = r["message"].strip().replace("\n", " ")[:100] if r["message"] else ""
            print(f"  {RED(str(i) + '.')} {r['section']} - {label}")
            if msg:
                print(f"     {DIM(msg)}")
        print()


def main() -> int:
    version = _get_version()
    today   = date.today().isoformat()
    title   = f"AW QUERY ACCURACY TEST SUITE  {version}  {today}"

    print(BOLD(_SEP))
    print(BOLD(title))
    print(BOLD(_SEP))
    print()

    _print_golden_values()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "pytest",
        str(Path(__file__)),
        f"--junit-xml={REPORT_XML}",
        "-q",
        "--tb=short",
        "--no-header",
    ]

    t0 = time.perf_counter()
    result = subprocess.run(cmd, cwd=ROOT)
    elapsed = time.perf_counter() - t0

    if not REPORT_XML.exists():
        print(RED("ERROR: pytest did not produce JUnit XML report."))
        print(DIM("  Check that pytest is installed: pip install pytest"))
        return 1

    results = _parse_junit_xml(REPORT_XML)

    if not results:
        print(YELLOW("No test results found in report."))
        return result.returncode

    # Group by section and print
    seen_sections: set[str] = set()
    current_section = ""

    for r in results:
        if r["section"] != current_section:
            if current_section:
                print()
            current_section = r["section"]
            if current_section not in seen_sections:
                print(BOLD(CYAN(current_section)))
                seen_sections.add(current_section)
        _print_result(r)

    _print_summary(results, elapsed)

    return 1 if any(r["status"] in ("fail", "error") for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
