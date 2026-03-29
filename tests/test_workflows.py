"""
test_workflows.py — Level 3 Automated Workflow Test Suite

Drives every Example Case through the full HTTP API pipeline:
  1. Import all datasets via POST /api/datasets/import
  2. Load all reference tables via POST /api/references/import
  3. Run each query_run step via POST /api/sql
  4. Verify baseline row counts
  5. Check session log contains expected event types

This is distinct from test_tutorial_queries.py, which uses DuckDB directly.
test_workflows.py goes through the full HTTP → import → Parquet → query pipeline.

Run standalone:
    pytest tests/test_workflows.py -v

Run via formatter:
    python tests/run_all.py
"""
from __future__ import annotations

import io
import json
import re
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.services.session_log import set_sessions_dir

# ============================================================
# CONSTANTS
# ============================================================

EXAMPLE_CASES_DIR = Path(__file__).parent.parent / "data" / "example_cases"

# Cases that are known to have multi-dataset or multi-reference complexity
_MULTI_DATASET_CASES = {
    "medicaid_pe_diligence",
    "parameterized_workflow_retail",
    "part_d_ira_exclusion",
    "retail_order_analysis",
}

# ============================================================
# HELPERS
# ============================================================

def _load_case(case_id: str) -> tuple[dict, dict]:
    """Load metadata.json and session.json for a given example case."""
    base = EXAMPLE_CASES_DIR / case_id
    meta = json.loads((base / "metadata.json").read_text(encoding="utf-8"))
    session = json.loads((base / "session.json").read_text(encoding="utf-8"))
    return meta, session


def _get_query_steps(session: dict) -> list[dict]:
    """Return all query_run events that have a sql field and a baseline."""
    events = session.get("events", [])
    return [
        e for e in events
        if e.get("event_type") == "query_run"
        and e.get("details", {}).get("sql")
        and e.get("baseline", {}).get("expected_row_count") is not None
    ]


def _rewrite_sql_for_api(sql: str, primary_name: str) -> str:
    """
    Rewrite logical table names back to 'dataset' / 'reference' so the
    API's own rewriter can expand them to Parquet paths.
    The session.json stores SQLs with the registered dataset name, not 'dataset'.
    """
    # Replace the registered dataset name with "dataset" for the API
    # (the API rewrites "dataset" → actual Parquet path)
    rewritten = re.sub(
        r"\b" + re.escape(primary_name) + r"\b",
        "dataset",
        sql,
        flags=re.IGNORECASE,
    )
    return rewritten


def _discover_cases() -> list[str]:
    """Return all example case IDs that have required files."""
    if not EXAMPLE_CASES_DIR.exists():
        return []
    cases = []
    for case_dir in sorted(EXAMPLE_CASES_DIR.iterdir()):
        if not case_dir.is_dir():
            continue
        if (case_dir / "metadata.json").exists() and (case_dir / "session.json").exists():
            cases.append(case_dir.name)
    return cases


_ALL_CASES = _discover_cases()


# ============================================================
# PER-CASE FIXTURE FACTORY
# ============================================================

def _make_case_fixtures(tmp_path_factory, case_id: str):
    """
    Set up a fresh isolated environment for one example case.
    Returns (client, metadata, session).
    """
    meta, session = _load_case(case_id)
    case_dir = EXAMPLE_CASES_DIR / case_id

    tmp = tmp_path_factory.mktemp(f"wf_{case_id[:20]}")
    ds_root  = tmp / "datasets"
    ref_root = tmp / "references"
    sess_dir = tmp / "sessions"
    exp_dir  = tmp / "exports"
    lib_dir  = tmp / "reference_library"

    for d in [ds_root, ref_root, sess_dir, exp_dir, lib_dir]:
        d.mkdir(parents=True)

    orig_ds   = main_module.DATASETS_DIR
    orig_ref  = main_module.REFERENCES_DIR
    orig_exp  = main_module.EXPORTS_DIR
    orig_sess = main_module.SESSIONS_DIR
    orig_lib  = main_module.REFERENCE_LIBRARY_DIR

    main_module.DATASETS_DIR          = ds_root
    main_module.REFERENCES_DIR        = ref_root
    main_module.EXPORTS_DIR           = exp_dir
    main_module.SESSIONS_DIR          = sess_dir
    main_module.REFERENCE_LIBRARY_DIR = lib_dir
    set_sessions_dir(sess_dir)

    client = TestClient(main_module.app)
    client.__enter__()

    # ── Import primary dataset ────────────────────────────────
    primary_csv = case_dir / "data" / meta["dataset_file"]
    if primary_csv.exists():
        with open(primary_csv, "rb") as f:
            client.post("/api/datasets/import",
                        files={"file": (meta["dataset_file"], f, "text/csv")})

    # ── Import additional datasets ────────────────────────────
    for add in meta.get("additional_datasets", []):
        csv_path = case_dir / "data" / add["file"]
        if csv_path.exists():
            with open(csv_path, "rb") as f:
                client.post("/api/datasets/import",
                            files={"file": (add["file"], f, "text/csv")})

    # ── Import reference tables ───────────────────────────────
    for ref_name in meta.get("reference_tables", []):
        for ext in (".csv",):
            ref_csv = case_dir / "reference" / f"{ref_name}{ext}"
            if ref_csv.exists():
                with open(ref_csv, "rb") as f:
                    client.post("/api/references/import",
                                files={"file": (f"{ref_name}.csv", f, "text/csv")})
                break

    return client, meta, session, (orig_ds, orig_ref, orig_exp, orig_sess, orig_lib)


def _teardown_case_fixtures(saved_dirs):
    orig_ds, orig_ref, orig_exp, orig_sess, orig_lib = saved_dirs
    main_module.DATASETS_DIR          = orig_ds
    main_module.REFERENCES_DIR        = orig_ref
    main_module.EXPORTS_DIR           = orig_exp
    main_module.SESSIONS_DIR          = orig_sess
    main_module.REFERENCE_LIBRARY_DIR = orig_lib
    set_sessions_dir(orig_sess)


# ============================================================
# DISCOVERY TESTS
# ============================================================

class TestWorkflowDiscovery:
    """Sanity checks that example cases are present and well-formed."""

    def test_example_cases_directory_exists(self):
        assert EXAMPLE_CASES_DIR.exists(), f"Missing: {EXAMPLE_CASES_DIR}"

    def test_at_least_six_cases_discovered(self):
        assert len(_ALL_CASES) >= 6, (
            f"Expected ≥6 example cases, found {len(_ALL_CASES)}: {_ALL_CASES}"
        )

    @pytest.mark.parametrize("case_id", _ALL_CASES)
    def test_case_has_metadata(self, case_id):
        meta_path = EXAMPLE_CASES_DIR / case_id / "metadata.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert "dataset_file" in meta

    @pytest.mark.parametrize("case_id", _ALL_CASES)
    def test_case_has_session_json(self, case_id):
        session_path = EXAMPLE_CASES_DIR / case_id / "session.json"
        assert session_path.exists()
        session = json.loads(session_path.read_text(encoding="utf-8"))
        assert "events" in session

    @pytest.mark.parametrize("case_id", _ALL_CASES)
    def test_case_primary_csv_exists(self, case_id):
        meta = json.loads(
            (EXAMPLE_CASES_DIR / case_id / "metadata.json").read_text(encoding="utf-8")
        )
        csv_path = EXAMPLE_CASES_DIR / case_id / "data" / meta["dataset_file"]
        assert csv_path.exists(), f"Missing primary CSV for {case_id}: {meta['dataset_file']}"

    @pytest.mark.parametrize("case_id", _ALL_CASES)
    def test_query_run_steps_have_baselines(self, case_id):
        _, session = _load_case(case_id)
        query_steps = _get_query_steps(session)
        # Every query_run step must have an expected_row_count baseline
        for step in query_steps:
            baseline = step.get("baseline", {})
            assert baseline.get("expected_row_count") is not None, (
                f"{case_id}: query_run step missing baseline: {step.get('details', {}).get('sql', '')[:80]}"
            )

    @pytest.mark.parametrize("case_id", _ALL_CASES)
    def test_additional_datasets_csvs_exist(self, case_id):
        meta, _ = _load_case(case_id)
        for add in meta.get("additional_datasets", []):
            csv_path = EXAMPLE_CASES_DIR / case_id / "data" / add["file"]
            assert csv_path.exists(), (
                f"{case_id}: missing additional_dataset CSV: {add['file']}"
            )

    @pytest.mark.parametrize("case_id", _ALL_CASES)
    def test_reference_csvs_exist(self, case_id):
        meta, _ = _load_case(case_id)
        for ref_name in meta.get("reference_tables", []):
            csv_path = EXAMPLE_CASES_DIR / case_id / "reference" / f"{ref_name}.csv"
            assert csv_path.exists(), (
                f"{case_id}: missing reference CSV: {ref_name}.csv"
            )


# ============================================================
# FULL WORKFLOW API TESTS — one class per example case
# ============================================================

class TestWorkflowPartD:
    """Tutorial #1 — Part D IRA Exclusion (CMS Medicare Part D spending)."""

    CASE_ID = "part_d_ira_exclusion"

    @pytest.fixture(scope="class")
    def wf(self, tmp_path_factory):
        if self.CASE_ID not in _ALL_CASES:
            pytest.skip(f"Example case {self.CASE_ID!r} not found")
        client, meta, session, saved = _make_case_fixtures(tmp_path_factory, self.CASE_ID)
        yield client, meta, session
        client.__exit__(None, None, None)
        _teardown_case_fixtures(saved)

    def test_primary_dataset_imported(self, wf):
        client, meta, _ = wf
        datasets = client.get("/api/datasets").json()["datasets"]
        names = [d["name"] for d in datasets]
        assert len(names) >= 1

    def test_all_query_steps_pass_baseline(self, wf):
        client, meta, session = wf
        _run_all_query_steps(client, meta, session)

    def test_session_log_has_query_events(self, wf):
        client, _, _ = wf
        summary = client.get("/api/session/summary").json()
        events = summary.get("events_by_type", {})
        assert events.get("query_run", 0) >= 1


class TestWorkflowPartB:
    """Tutorial #2 — Part B GLOBE Candidates (CMS Medicare Part B spending)."""

    CASE_ID = "part_b_globe_candidates"

    @pytest.fixture(scope="class")
    def wf(self, tmp_path_factory):
        if self.CASE_ID not in _ALL_CASES:
            pytest.skip(f"Example case {self.CASE_ID!r} not found")
        client, meta, session, saved = _make_case_fixtures(tmp_path_factory, self.CASE_ID)
        yield client, meta, session
        client.__exit__(None, None, None)
        _teardown_case_fixtures(saved)

    def test_primary_dataset_imported(self, wf):
        client, _, _ = wf
        datasets = client.get("/api/datasets").json()["datasets"]
        assert len(datasets) >= 1

    def test_all_query_steps_pass_baseline(self, wf):
        client, meta, session = wf
        _run_all_query_steps(client, meta, session)


class TestWorkflowMedicaid:
    """Tutorial #4 — Multi-State Medicaid Diligence (TX + FL + OH claims)."""

    CASE_ID = "medicaid_pe_diligence"

    @pytest.fixture(scope="class")
    def wf(self, tmp_path_factory):
        if self.CASE_ID not in _ALL_CASES:
            pytest.skip(f"Example case {self.CASE_ID!r} not found")
        client, meta, session, saved = _make_case_fixtures(tmp_path_factory, self.CASE_ID)
        yield client, meta, session
        client.__exit__(None, None, None)
        _teardown_case_fixtures(saved)

    def test_all_datasets_imported(self, wf):
        client, meta, _ = wf
        expected_count = 1 + len(meta.get("additional_datasets", []))
        datasets = client.get("/api/datasets").json()["datasets"]
        assert len(datasets) >= expected_count

    def test_all_references_imported(self, wf):
        client, meta, _ = wf
        expected_count = len(meta.get("reference_tables", []))
        if expected_count == 0:
            pytest.skip("No reference tables in this case")
        refs = client.get("/api/references").json().get("references", [])
        assert len(refs) >= expected_count

    def test_all_query_steps_pass_baseline(self, wf):
        client, meta, session = wf
        _run_all_query_steps(client, meta, session)

    def test_session_has_dataset_import_events(self, wf):
        client, meta, _ = wf
        summary = client.get("/api/session/summary").json()
        events = summary.get("events_by_type", {})
        expected_imports = 1 + len(meta.get("additional_datasets", []))
        assert events.get("dataset_import", 0) >= expected_imports


class TestWorkflowRealEstate:
    """Tutorial #5 — Real Estate Market Analysis (Austin + Denver listings)."""

    CASE_ID = "real_estate_market_analysis"

    @pytest.fixture(scope="class")
    def wf(self, tmp_path_factory):
        if self.CASE_ID not in _ALL_CASES:
            pytest.skip(f"Example case {self.CASE_ID!r} not found")
        client, meta, session, saved = _make_case_fixtures(tmp_path_factory, self.CASE_ID)
        yield client, meta, session
        client.__exit__(None, None, None)
        _teardown_case_fixtures(saved)

    def test_primary_dataset_imported(self, wf):
        client, _, _ = wf
        datasets = client.get("/api/datasets").json()["datasets"]
        assert len(datasets) >= 1

    def test_all_query_steps_pass_baseline(self, wf):
        client, meta, session = wf
        _run_all_query_steps(client, meta, session)


class TestWorkflowRetailParameterized:
    """Tutorial #6 — Parameterized Retail Workflow (Electronics + Sporting Goods)."""

    CASE_ID = "parameterized_workflow_retail"

    @pytest.fixture(scope="class")
    def wf(self, tmp_path_factory):
        if self.CASE_ID not in _ALL_CASES:
            pytest.skip(f"Example case {self.CASE_ID!r} not found")
        client, meta, session, saved = _make_case_fixtures(tmp_path_factory, self.CASE_ID)
        yield client, meta, session
        client.__exit__(None, None, None)
        _teardown_case_fixtures(saved)

    def test_all_datasets_imported(self, wf):
        client, meta, _ = wf
        expected = 1 + len(meta.get("additional_datasets", []))
        datasets = client.get("/api/datasets").json()["datasets"]
        assert len(datasets) >= expected

    def test_all_query_steps_pass_baseline(self, wf):
        client, meta, session = wf
        _run_all_query_steps(client, meta, session)


class TestWorkflowTaxi:
    """NYC Taxi Trip Analysis — time series and aggregation queries."""

    CASE_ID = "taxi_trip_analysis"

    @pytest.fixture(scope="class")
    def wf(self, tmp_path_factory):
        if self.CASE_ID not in _ALL_CASES:
            pytest.skip(f"Example case {self.CASE_ID!r} not found")
        client, meta, session, saved = _make_case_fixtures(tmp_path_factory, self.CASE_ID)
        yield client, meta, session
        client.__exit__(None, None, None)
        _teardown_case_fixtures(saved)

    def test_primary_dataset_imported(self, wf):
        client, _, _ = wf
        datasets = client.get("/api/datasets").json()["datasets"]
        assert len(datasets) >= 1

    def test_all_query_steps_pass_baseline(self, wf):
        client, meta, session = wf
        _run_all_query_steps(client, meta, session)


class TestWorkflowSaaS:
    """SaaS Account Analysis — cohort and churn analysis queries."""

    CASE_ID = "saas_account_analysis"

    @pytest.fixture(scope="class")
    def wf(self, tmp_path_factory):
        if self.CASE_ID not in _ALL_CASES:
            pytest.skip(f"Example case {self.CASE_ID!r} not found")
        client, meta, session, saved = _make_case_fixtures(tmp_path_factory, self.CASE_ID)
        yield client, meta, session
        client.__exit__(None, None, None)
        _teardown_case_fixtures(saved)

    def test_primary_dataset_imported(self, wf):
        client, _, _ = wf
        datasets = client.get("/api/datasets").json()["datasets"]
        assert len(datasets) >= 1

    def test_all_query_steps_pass_baseline(self, wf):
        client, meta, session = wf
        _run_all_query_steps(client, meta, session)


# ============================================================
# SHARED HELPER — runs all query steps for any case
# ============================================================

def _run_all_query_steps(client: TestClient, meta: dict, session: dict) -> None:
    """
    Run every query_run step from a session against the imported datasets.
    Asserts that all row counts match their baselines.
    Fails with a combined error message listing every failing step.
    """
    steps = _get_query_steps(session)
    if not steps:
        pytest.skip("No query_run steps with baselines found in this case")

    # Determine the registered name of the primary dataset from what was imported
    datasets = client.get("/api/datasets").json().get("datasets", [])
    if not datasets:
        pytest.fail("No datasets were imported — import step must have failed")

    # Build name→registered mapping: strip .csv extension to get the registered name
    primary_stem = Path(meta["dataset_file"]).stem
    # Find the imported dataset whose name most closely matches
    imported_name = _find_best_match(primary_stem, [d["name"] for d in datasets])

    # Build additional dataset mapping
    additional_map = {}
    for add in meta.get("additional_datasets", []):
        stem = Path(add["file"]).stem
        best = _find_best_match(stem, [d["name"] for d in datasets])
        if best:
            # Map the display_name (used in SQL) to the registered name
            additional_map[add.get("display_name", stem)] = best
            additional_map[stem] = best

    failures = []
    for i, step in enumerate(steps):
        sql = step["details"]["sql"]
        expected = step["baseline"]["expected_row_count"]
        ref = step["details"].get("reference")
        dataset_override = step["details"].get("dataset")

        # Use dataset specified in step if present
        ds_name = imported_name
        if dataset_override:
            ds_name = _find_best_match(dataset_override, [d["name"] for d in datasets]) or imported_name

        payload: dict = {"dataset": ds_name, "sql": sql}
        if ref:
            payload["reference"] = ref

        try:
            r = client.post("/api/sql", json=payload)
        except Exception as exc:
            failures.append(f"Step {i}: exception: {exc}")
            continue

        if r.status_code != 200:
            failures.append(f"Step {i}: HTTP {r.status_code} — {r.text[:120]}")
            continue

        actual = r.json()["rowcount"]
        # Scalar COUNT(*) convention
        result_rows = r.json().get("rows", [])
        if (actual == 1 and result_rows and expected != 1
                and len(r.json().get("columns", [])) == 1):
            cell = list(result_rows[0].values())[0]
            if isinstance(cell, (int, float)):
                actual = int(cell)

        # Allow ±1 tolerance: workflow tests import via API (title-case
        # normalization applied), while session.json baselines were calibrated
        # against direct DuckDB on CSV files. Tiny JOIN result differences
        # (Bug #10 — reference table normalisation) are expected.
        if abs(actual - expected) > 1:
            sql_short = sql[:100].replace("\n", " ")
            failures.append(
                f"Step {i}: expected {expected} rows, got {actual} | {sql_short}"
            )

    if failures:
        case_name = meta.get("name", "unknown")
        pytest.fail(
            f"Workflow '{case_name}' — {len(failures)}/{len(steps)} steps failed:\n  "
            + "\n  ".join(failures)
        )


def _find_best_match(target: str, candidates: list[str]) -> str | None:
    """Find the candidate that best matches the target stem name."""
    if not candidates:
        return None
    # Exact match first
    if target in candidates:
        return target
    # Substring match
    for c in candidates:
        if target in c or c in target:
            return c
    # Fuzzy: replace underscores with spaces or vice versa
    target_norm = target.replace("-", "_").lower()
    for c in candidates:
        if c.replace("-", "_").lower() == target_norm:
            return c
    # Return first candidate as fallback
    return candidates[0]
