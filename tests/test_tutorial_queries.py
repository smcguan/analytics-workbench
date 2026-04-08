"""
test_tutorial_queries.py — integration test for all tutorial SQL queries

Validates that every query_run event in every example case session.json
actually executes against the case's sample data and returns the expected
row count from the baseline.

This catches:
  - SQL syntax errors in hand-authored session.json files
  - Column name mismatches between SQL and sample CSVs
  - Baseline row counts that don't match reality
  - Broken JOINs against reference tables or additional datasets

Does NOT cover ai_ask events (SQL generated at runtime by AI, not stored).

Run from project root:
    pytest tests/test_tutorial_queries.py -v
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXAMPLE_CASES_DIR = PROJECT_ROOT / "data" / "example_cases"


# ---------------------------------------------------------------------------
# SQL rewrite — replicate what main.py does at runtime
# ---------------------------------------------------------------------------

_SQL_KEYWORDS = {
    "WHERE", "GROUP", "ORDER", "LIMIT", "UNION", "ALL", "ON", "AND", "OR",
    "HAVING", "SET", "INTO", "VALUES", "SELECT", "FROM", "JOIN", "LEFT",
    "RIGHT", "INNER", "OUTER", "CROSS", "FULL", "NATURAL", "USING", "AS",
    "NOT", "IN", "IS", "NULL", "BETWEEN", "LIKE", "EXISTS", "CASE", "WHEN",
    "THEN", "ELSE", "END", "WITH", "OVER", "PARTITION", "BY", "ASC", "DESC",
    "DISTINCT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
    "OFFSET", "FETCH", "EXCEPT", "INTERSECT",
}


def _rewrite_sql(
    sql: str,
    primary_csv: Path,
    primary_name: str,
    reference_csvs: dict[str, Path],
    additional_csvs: dict[str, Path],
) -> str:
    """Rewrite table names in SQL to point at CSV file paths.

    Mirrors the rewrite logic in main.py:
    - "dataset" or the primary dataset name → primary CSV path
    - "reference" → first loaded reference CSV (for single-reference cases)
    - Named reference tables → their CSV paths
    - Named additional datasets → their CSV paths
    """
    # Build replacement map: table_name → csv_path
    table_map: dict[str, str] = {}

    # Primary dataset: match both "dataset" and the actual dataset name
    table_map["dataset"] = str(primary_csv)
    table_map[primary_name] = str(primary_csv)

    # Additional datasets
    for name, path in additional_csvs.items():
        table_map[name] = str(path)

    # Reference tables — also register "reference" as an alias for single-ref cases
    for name, path in reference_csvs.items():
        table_map[name] = str(path)
    if len(reference_csvs) == 1:
        table_map["reference"] = str(list(reference_csvs.values())[0])

    # Sort by length descending so longer names match first
    # (e.g. "fl_medicaid_claims" before "dataset")
    sorted_names = sorted(table_map.keys(), key=len, reverse=True)

    for table_name in sorted_names:
        csv_path = table_map[table_name].replace("\\", "/")

        # Pattern: FROM/JOIN + table_name + optional alias (not a SQL keyword)
        # The alias group uses a lookahead to avoid capturing SQL keywords.
        pattern = (
            rf"(FROM|JOIN)"           # group 1: keyword
            rf"\s+"
            rf"{re.escape(table_name)}"
            rf"(?:"                    # optional alias group
            rf"\s+(AS\s+)?(\w+)"       # group 2: AS prefix, group 3: alias word
            rf")?"
        )

        def _replacer(m: re.Match, p: str = csv_path) -> str:
            keyword = m.group(1)
            alias = m.group(3) or ""
            # If the "alias" is actually a SQL keyword, don't consume it
            if alias.upper() in _SQL_KEYWORDS:
                return f"{keyword} '{p}'\n{alias}"
            if alias:
                return f"{keyword} '{p}' AS {alias}"
            return f"{keyword} '{p}'"

        sql = re.sub(pattern, _replacer, sql, flags=re.IGNORECASE)

    return sql


# ---------------------------------------------------------------------------
# Discover all example cases with query_run events
# ---------------------------------------------------------------------------

def _discover_test_cases() -> list[tuple[str, int, dict, dict]]:
    """Return a list of (case_id, event_index, event, metadata) for each query_run."""
    if not EXAMPLE_CASES_DIR.exists():
        return []

    test_cases = []
    for case_dir in sorted(EXAMPLE_CASES_DIR.iterdir()):
        if not case_dir.is_dir():
            continue

        meta_path = case_dir / "metadata.json"
        session_path = case_dir / "session.json"
        if not meta_path.exists() or not session_path.exists():
            continue

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        session = json.loads(session_path.read_text(encoding="utf-8"))

        for i, event in enumerate(session.get("events", [])):
            if event["event_type"] != "query_run":
                continue
            if "sql" not in event.get("details", {}):
                continue
            test_cases.append((case_dir.name, i, event, meta))

    return test_cases


_TEST_CASES = _discover_test_cases()


def _test_id(val):
    """Generate readable pytest IDs."""
    if isinstance(val, tuple) and len(val) == 4:
        case_id, idx, event, _ = val
        return f"{case_id}[{idx}]"
    return str(val)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def duckdb_conn():
    """Shared DuckDB connection for all tests in this module."""
    conn = duckdb.connect()
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Parameterized test — one test per query_run event
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "case_id, event_index, event, metadata",
    _TEST_CASES,
    ids=[f"{c[0]}[{c[1]}]" for c in _TEST_CASES],
)
def test_tutorial_query(duckdb_conn, case_id, event_index, event, metadata):
    """Execute a tutorial query_run against sample data and verify row count."""
    case_dir = EXAMPLE_CASES_DIR / case_id
    details = event["details"]
    sql = details["sql"]
    baseline = event.get("baseline", {})
    expected_row_count = baseline.get("expected_row_count")

    # Resolve primary dataset CSV
    primary_file = metadata.get("dataset_file", "")
    primary_name = Path(primary_file).stem if primary_file else "dataset"
    primary_csv = case_dir / "data" / primary_file

    # The query might target a different dataset (e.g. sporting_goods after edit panel)
    query_dataset = details.get("dataset", primary_name)

    # Check if query_dataset is an additional dataset — use it as primary
    additional_datasets = metadata.get("additional_datasets", [])
    additional_csvs: dict[str, Path] = {}
    for ad in additional_datasets:
        ad_name = Path(ad["file"]).stem
        ad_path = case_dir / "data" / ad["file"]
        additional_csvs[ad_name] = ad_path
        if ad_name == query_dataset:
            primary_csv = ad_path
            primary_name = ad_name

    # Resolve reference table CSVs
    reference_names = metadata.get("reference_tables", [])
    reference_csvs: dict[str, Path] = {}
    for ref_name in reference_names:
        # Reference CSVs are named {ref_name}.csv in the reference/ directory
        ref_csv = case_dir / "reference" / f"{ref_name}.csv"
        if ref_csv.exists():
            reference_csvs[ref_name] = ref_csv

    # Rewrite SQL
    rewritten = _rewrite_sql(
        sql,
        primary_csv=primary_csv,
        primary_name=primary_name,
        reference_csvs=reference_csvs,
        additional_csvs=additional_csvs,
    )

    # Execute
    try:
        result = duckdb_conn.execute(rewritten).fetchall()
    except Exception as exc:
        pytest.fail(
            f"{case_id} event[{event_index}]: SQL execution failed\n"
            f"  Error: {exc}\n"
            f"  Rewritten SQL:\n{rewritten}"
        )

    actual_row_count = len(result)

    # Verify row count against baseline
    if expected_row_count is not None:
        # Scalar COUNT convention: if the query returns exactly 1 row with 1
        # numeric column, the baseline may encode the scalar value rather than
        # the row count (e.g. COUNT(DISTINCT ...) = 89 → baseline 89).
        if (
            actual_row_count == 1
            and len(result[0]) == 1
            and isinstance(result[0][0], (int, float))
            and expected_row_count != 1
        ):
            scalar_value = int(result[0][0])
            assert scalar_value == expected_row_count, (
                f"{case_id} event[{event_index}]: "
                f"scalar COUNT expected {expected_row_count}, got {scalar_value}\n"
                f"  SQL: {sql[:200]}..."
            )
        else:
            assert actual_row_count == expected_row_count, (
                f"{case_id} event[{event_index}]: "
                f"expected {expected_row_count} rows, got {actual_row_count}\n"
                f"  SQL: {sql[:200]}..."
            )


# ---------------------------------------------------------------------------
# Summary test — verify every case has at least one testable query
# ---------------------------------------------------------------------------

def test_all_cases_discovered():
    """Ensure we discovered test cases from the example_cases directory."""
    assert len(_TEST_CASES) > 0, "No query_run events found in any example case"

    # Check that cases with known query_run events are present
    case_ids = {c[0] for c in _TEST_CASES}
    expected_cases = {
        "parameterized_workflow_retail",
        "medicaid_pe_diligence",
        "part_b_globe_candidates",
        "real_estate_market_analysis",
        "usp_category_classification",
        "taxi_trip_analysis",
        "retail_order_analysis",
        "cash_pay_medspa",
        "idre_readmissions",
    }
    missing = expected_cases - case_ids
    assert not missing, f"Expected cases missing from test discovery: {missing}"


def test_all_cases_have_baselines():
    """Every query_run event should have a baseline for regression testing."""
    missing_baselines = []
    for case_id, idx, event, _ in _TEST_CASES:
        baseline = event.get("baseline", {})
        if "expected_row_count" not in baseline:
            missing_baselines.append(f"{case_id}[{idx}]")

    assert not missing_baselines, (
        f"query_run events without baselines: {missing_baselines}"
    )
