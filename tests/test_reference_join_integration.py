"""
test_reference_join_integration.py — end-to-end reference table JOIN execution
and reference library edge cases

Covers:
  INNER JOIN / LEFT JOIN with actual SQL execution against DuckDB
  Case-insensitive matching (title-case normalization on import)
  No-match and partial-match scenarios
  Error paths: missing reference, deleted reference
  Reference overwrite: import A then B, SQL uses B
  Library edge cases: spaces in filenames, header-only CSVs, corrupt manifest

Run from project root:
    pytest tests/test_reference_join_integration.py -v
"""
from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.services.dataset_import import import_reference_table


# ===========================================================================
# HELPERS
# ===========================================================================

def _make_csv_bytes(rows: list[dict]) -> bytes:
    df = pd.DataFrame(rows)
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def _create_test_dataset(ds_dir: Path, name: str = "test_drugs") -> Path:
    """Create a small test dataset with drug_name, category, and amount columns."""
    d = ds_dir / name
    d.mkdir(exist_ok=True)
    rows = [
        {"drug_name": "Keytruda", "category": "Oncology", "amount": 8000},
        {"drug_name": "Opdivo", "category": "Oncology", "amount": 6000},
        {"drug_name": "Eliquis", "category": "Cardiovascular", "amount": 5000},
        {"drug_name": "Humira", "category": "Immunology", "amount": 7000},
        {"drug_name": "Stelara", "category": "Immunology", "amount": 4000},
        {"drug_name": "Imbruvica", "category": "Oncology", "amount": 3500},
        {"drug_name": "Enbrel", "category": "Immunology", "amount": 3000},
        {"drug_name": "Revlimid", "category": "Oncology", "amount": 9000},
        {"drug_name": "Xarelto", "category": "Cardiovascular", "amount": 4500},
        {"drug_name": "Januvia", "category": "Diabetes", "amount": 2000},
        {"drug_name": "Jardiance", "category": "Diabetes", "amount": 2500},
        {"drug_name": "Ozempic", "category": "Diabetes", "amount": 6500},
        {"drug_name": "Entresto", "category": "Cardiovascular", "amount": 3800},
        {"drug_name": "Dupixent", "category": "Immunology", "amount": 5500},
        {"drug_name": "Tagrisso", "category": "Oncology", "amount": 4200},
        {"drug_name": "Ibrance", "category": "Oncology", "amount": 3200},
        {"drug_name": "Trulicity", "category": "Diabetes", "amount": 2800},
        {"drug_name": "Cosentyx", "category": "Immunology", "amount": 2600},
        {"drug_name": "Tecfidera", "category": "Neurology", "amount": 3100},
        {"drug_name": "Ocrevus", "category": "Neurology", "amount": 4800},
    ]
    df = pd.DataFrame(rows)
    df.to_parquet(str(d / "source.parquet"), index=False)
    meta = {
        "row_count": len(rows),
        "column_count": 3,
        "columns": ["drug_name", "category", "amount"],
    }
    (d / "_meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (d / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    return d


# ===========================================================================
# FIXTURES
# ===========================================================================

@pytest.fixture()
def env_dirs(tmp_path):
    """Set up temporary DATASETS_DIR, REFERENCES_DIR, and REFERENCE_LIBRARY_DIR."""
    orig_ds = main_module.DATASETS_DIR
    orig_ref = main_module.REFERENCES_DIR
    orig_lib = main_module.REFERENCE_LIBRARY_DIR

    ds_dir = tmp_path / "datasets"
    ds_dir.mkdir()
    ref_dir = tmp_path / "references"
    ref_dir.mkdir()
    lib_dir = tmp_path / "library"
    lib_dir.mkdir()

    main_module.DATASETS_DIR = ds_dir
    main_module.REFERENCES_DIR = ref_dir
    main_module.REFERENCE_LIBRARY_DIR = lib_dir

    _create_test_dataset(ds_dir)

    yield {"ds_dir": ds_dir, "ref_dir": ref_dir, "lib_dir": lib_dir}

    main_module.DATASETS_DIR = orig_ds
    main_module.REFERENCES_DIR = orig_ref
    main_module.REFERENCE_LIBRARY_DIR = orig_lib


@pytest.fixture()
def client(env_dirs):
    with TestClient(main_module.app) as c:
        yield c


def _import_ref(client, rows: list[dict], filename: str = "ref.csv") -> dict:
    """Import a reference table via the API and return the JSON response."""
    csv_data = _make_csv_bytes(rows)
    resp = client.post(
        "/api/references/import",
        files={"file": (filename, io.BytesIO(csv_data), "text/csv")},
    )
    assert resp.status_code == 200, f"Reference import failed: {resp.text}"
    return resp.json()


def _run_sql(client, sql: str, dataset: str = "test_drugs", reference: str | None = None) -> dict:
    """POST /api/sql and return the response object."""
    body = {"dataset": dataset, "sql": sql}
    if reference:
        body["reference"] = reference
    return client.post("/api/sql", json=body)


# ===========================================================================
# 1. INNER JOIN — correct rows returned
# ===========================================================================

def test_inner_join_returns_matching_rows(client):
    """Import dataset + reference, INNER JOIN returns only matched rows."""
    ref_data = _import_ref(client, [
        {"drug_name": "Keytruda", "ira_round": 1},
        {"drug_name": "Eliquis", "ira_round": 1},
        {"drug_name": "Imbruvica", "ira_round": 2},
    ])
    ref_name = ref_data["reference"]

    resp = _run_sql(
        client,
        "SELECT d.drug_name, d.amount, r.ira_round "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name "
        "ORDER BY d.drug_name",
        reference=ref_name,
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 3
    matched_names = sorted(r["drug_name"] for r in rows)
    assert matched_names == ["Eliquis", "Imbruvica", "Keytruda"]


# ===========================================================================
# 2. LEFT JOIN — all dataset rows present, reference columns populated where matched
# ===========================================================================

def test_left_join_preserves_all_dataset_rows(client):
    """LEFT JOIN returns all 20 dataset rows, with reference columns NULL for non-matches."""
    ref_data = _import_ref(client, [
        {"drug_name": "Keytruda", "excluded": 1},
        {"drug_name": "Opdivo", "excluded": 1},
    ])
    ref_name = ref_data["reference"]

    resp = _run_sql(
        client,
        "SELECT d.drug_name, d.amount, r.excluded "
        "FROM dataset d LEFT JOIN reference r ON d.drug_name = r.drug_name "
        "ORDER BY d.drug_name",
        reference=ref_name,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["rowcount"] == 20, "LEFT JOIN must return all dataset rows"

    # Verify matched rows have excluded=1
    rows_by_name = {r["drug_name"]: r for r in data["rows"]}
    assert rows_by_name["Keytruda"]["excluded"] == 1
    assert rows_by_name["Opdivo"]["excluded"] == 1
    # Non-matched rows have NULL
    assert rows_by_name["Eliquis"]["excluded"] is None


# ===========================================================================
# 3. Case-insensitive matching via title-case normalization on import
# ===========================================================================

def test_join_case_insensitive_via_title_case_import(client):
    """
    Dataset has title-cased 'Keytruda'. Reference CSV has 'KEYTRUDA'.
    After import (which title-cases strings), JOIN should match without LOWER().
    """
    ref_data = _import_ref(client, [
        {"drug_name": "KEYTRUDA", "flag": "yes"},
        {"drug_name": "opdivo", "flag": "yes"},
        {"drug_name": "Eliquis", "flag": "yes"},
    ])
    ref_name = ref_data["reference"]

    resp = _run_sql(
        client,
        "SELECT d.drug_name, r.flag "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name",
        reference=ref_name,
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    matched_names = sorted(r["drug_name"] for r in rows)
    assert "Keytruda" in matched_names, "KEYTRUDA should title-case to Keytruda and match"
    assert "Opdivo" in matched_names, "opdivo should title-case to Opdivo and match"
    assert "Eliquis" in matched_names, "Eliquis should match directly"
    assert len(rows) == 3


# ===========================================================================
# 4. JOIN with no matches — INNER JOIN returns 0 rows
# ===========================================================================

def test_inner_join_no_matches_returns_zero_rows(client):
    """Reference has values not present in dataset — INNER JOIN returns 0 rows."""
    ref_data = _import_ref(client, [
        {"drug_name": "Nonexistium", "flag": 1},
        {"drug_name": "Fakemab", "flag": 1},
        {"drug_name": "Imaginavir", "flag": 1},
    ])
    ref_name = ref_data["reference"]

    resp = _run_sql(
        client,
        "SELECT d.drug_name, r.flag "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name",
        reference=ref_name,
    )
    assert resp.status_code == 200
    assert resp.json()["rowcount"] == 0


# ===========================================================================
# 5. Partial matches — correct row count
# ===========================================================================

def test_inner_join_partial_matches(client):
    """Some reference values match, some do not — verify exact match count."""
    ref_data = _import_ref(client, [
        {"drug_name": "Keytruda", "category": "IRA"},
        {"drug_name": "Fakemab", "category": "IRA"},
        {"drug_name": "Humira", "category": "IRA"},
        {"drug_name": "Nonexistium", "category": "Other"},
        {"drug_name": "Ozempic", "category": "IRA"},
    ])
    ref_name = ref_data["reference"]

    resp = _run_sql(
        client,
        "SELECT d.drug_name, r.category AS ref_cat "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name "
        "ORDER BY d.drug_name",
        reference=ref_name,
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    # Keytruda, Humira, Ozempic match; Fakemab, Nonexistium do not
    assert len(rows) == 3
    matched = sorted(r["drug_name"] for r in rows)
    assert matched == ["Humira", "Keytruda", "Ozempic"]


# ===========================================================================
# 6. SQL referencing 'reference' without a loaded reference table -> HTTP 400
# ===========================================================================

def test_sql_reference_without_loaded_table_returns_400(client):
    """Using 'reference' in SQL without importing a reference table gives 400."""
    resp = _run_sql(
        client,
        "SELECT d.*, r.flag FROM dataset d JOIN reference r ON d.drug_name = r.drug_name",
        reference=None,
    )
    assert resp.status_code == 400
    assert "reference" in resp.json()["detail"].lower()


# ===========================================================================
# 7. Import reference, delete it, then try to JOIN -> HTTP 400/404
# ===========================================================================

def test_deleted_reference_returns_error(client):
    """After deleting a reference table, SQL using it should fail."""
    ref_data = _import_ref(client, [
        {"drug_name": "Keytruda", "flag": 1},
    ])
    ref_name = ref_data["reference"]

    # Delete the reference
    del_resp = client.post(f"/api/references/{ref_name}/delete")
    assert del_resp.status_code == 200

    # Attempt to use deleted reference
    resp = _run_sql(
        client,
        "SELECT d.drug_name FROM dataset d JOIN reference r ON d.drug_name = r.drug_name",
        reference=ref_name,
    )
    assert resp.status_code in (400, 404), f"Expected 400 or 404, got {resp.status_code}"


# ===========================================================================
# 8. Import reference A, then import reference B (overwrite) -> SQL uses B
# ===========================================================================

def test_overwrite_reference_uses_new_data(client):
    """Importing a second reference with the same filename overwrites the first."""
    # Import reference A
    _import_ref(client, [
        {"drug_name": "Keytruda", "source": "A"},
    ], filename="lookup.csv")

    # Import reference B (same filename -> same registered name -> overwrite)
    ref_b = _import_ref(client, [
        {"drug_name": "Keytruda", "source": "B"},
        {"drug_name": "Opdivo", "source": "B"},
    ], filename="lookup.csv")
    ref_name = ref_b["reference"]

    resp = _run_sql(
        client,
        "SELECT d.drug_name, r.source "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name "
        "ORDER BY d.drug_name",
        reference=ref_name,
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    # Should match B's data: 2 rows, source = "B" (title-cased, still "B")
    assert len(rows) == 2
    sources = {r["source"] for r in rows}
    assert sources == {"B"}


# ===========================================================================
# 9. Library CSV with spaces in filename: auto-discovers, can be loaded
# ===========================================================================

def test_library_csv_with_spaces_in_filename(client, env_dirs):
    """A library CSV with spaces in its filename should auto-discover and load."""
    lib_dir = env_dirs["lib_dir"]
    csv_path = lib_dir / "orphan drug status.csv"
    df = pd.DataFrame([
        {"drug_name": "Keytruda", "orphan": "Yes"},
        {"drug_name": "Opdivo", "orphan": "No"},
    ])
    df.to_csv(str(csv_path), index=False)

    # Auto-discover
    resp = client.get("/api/reference_library")
    assert resp.status_code == 200
    items = resp.json()["library"]
    filenames = [i["filename"] for i in items]
    assert "orphan drug status.csv" in filenames

    # Load it
    load_resp = client.post("/api/reference_library/orphan drug status.csv/load")
    assert load_resp.status_code == 200
    assert load_resp.json()["row_count"] == 2


# ===========================================================================
# 10. Library CSV with only headers (no data rows): auto-discovers, shows 0 rows
# ===========================================================================

def test_library_csv_headers_only(client, env_dirs):
    """A CSV with headers but no data rows should auto-discover with 0 rows."""
    lib_dir = env_dirs["lib_dir"]
    (lib_dir / "empty_table.csv").write_text("col_a,col_b,col_c\n", encoding="utf-8")

    resp = client.get("/api/reference_library")
    items = resp.json()["library"]
    empty_item = next((i for i in items if i["filename"] == "empty_table.csv"), None)
    assert empty_item is not None, "Header-only CSV should be auto-discovered"
    assert empty_item["row_count"] == 0
    assert empty_item["columns"] == ["col_a", "col_b", "col_c"]


# ===========================================================================
# 11. Load library CSV as active reference, then query it via SQL
# ===========================================================================

def test_load_library_csv_and_query_via_sql(client, env_dirs):
    """Load a library CSV as reference and execute a JOIN query."""
    lib_dir = env_dirs["lib_dir"]
    df = pd.DataFrame([
        {"drug_name": "Keytruda", "ira_round": 1},
        {"drug_name": "Eliquis", "ira_round": 1},
        {"drug_name": "Januvia", "ira_round": 2},
    ])
    df.to_csv(str(lib_dir / "ira_list.csv"), index=False)

    load_resp = client.post("/api/reference_library/ira_list.csv/load")
    assert load_resp.status_code == 200
    ref_name = load_resp.json()["reference"]

    resp = _run_sql(
        client,
        "SELECT d.drug_name, d.amount, r.ira_round "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name "
        "ORDER BY d.amount DESC",
        reference=ref_name,
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 3
    matched_names = sorted(r["drug_name"] for r in rows)
    assert matched_names == ["Eliquis", "Januvia", "Keytruda"]


# ===========================================================================
# 12. Library manifest missing entirely: auto-discover still works
# ===========================================================================

def test_library_no_manifest_auto_discover_works(client, env_dirs):
    """With no _library.json at all, CSVs in the directory are still discovered."""
    lib_dir = env_dirs["lib_dir"]
    # Ensure no manifest exists
    manifest_path = lib_dir / "_library.json"
    if manifest_path.exists():
        manifest_path.unlink()

    # Add some CSVs
    pd.DataFrame([{"x": 1}]).to_csv(str(lib_dir / "alpha.csv"), index=False)
    pd.DataFrame([{"y": 2}, {"y": 3}]).to_csv(str(lib_dir / "beta.csv"), index=False)

    resp = client.get("/api/reference_library")
    assert resp.status_code == 200
    items = resp.json()["library"]
    filenames = sorted(i["filename"] for i in items)
    assert "alpha.csv" in filenames
    assert "beta.csv" in filenames


# ===========================================================================
# 13. Library manifest with corrupt JSON: auto-discover still works
# ===========================================================================

def test_library_corrupt_manifest_auto_discover_works(client, env_dirs):
    """If _library.json is corrupt, auto-discover should still find CSVs."""
    lib_dir = env_dirs["lib_dir"]
    (lib_dir / "_library.json").write_text("{{{CORRUPT JSON!!!}", encoding="utf-8")
    pd.DataFrame([{"col": "val"}]).to_csv(str(lib_dir / "good_file.csv"), index=False)

    resp = client.get("/api/reference_library")
    assert resp.status_code == 200
    items = resp.json()["library"]
    filenames = [i["filename"] for i in items]
    assert "good_file.csv" in filenames


# ===========================================================================
# 14. Load library reference when another reference is already active
# ===========================================================================

def test_load_library_overwrites_existing_reference(client, env_dirs):
    """Loading a library reference when another is active should overwrite cleanly."""
    lib_dir = env_dirs["lib_dir"]

    # Create two library CSVs
    pd.DataFrame([{"drug_name": "Alpha", "group": "A"}]).to_csv(
        str(lib_dir / "ref_a.csv"), index=False
    )
    pd.DataFrame([
        {"drug_name": "Keytruda", "group": "B"},
        {"drug_name": "Opdivo", "group": "B"},
    ]).to_csv(str(lib_dir / "ref_b.csv"), index=False)

    # Load reference A
    resp_a = client.post("/api/reference_library/ref_a.csv/load")
    assert resp_a.status_code == 200

    # Load reference B (should overwrite A)
    resp_b = client.post("/api/reference_library/ref_b.csv/load")
    assert resp_b.status_code == 200
    ref_name = resp_b.json()["reference"]

    # Query should use B's data
    resp = _run_sql(
        client,
        "SELECT d.drug_name, r.group "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name "
        "ORDER BY d.drug_name",
        reference=ref_name,
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert len(rows) == 2
    matched_names = sorted(r["drug_name"] for r in rows)
    assert matched_names == ["Keytruda", "Opdivo"]
    # Verify it is B's data
    groups = {r["group"] for r in rows}
    assert groups == {"B"}


# ===========================================================================
# Bug #10 regression — reference auto-detected when not passed in request
# ===========================================================================

def test_reference_auto_detected_when_not_in_request(client, env_dirs):
    """
    Bug #10 regression: after app restart, frontend may not send 'reference'
    in the SQL request body, but the reference table is still on disk.
    The backend should auto-detect it.
    """
    ref_dir = env_dirs["ref_dir"]

    # Import a reference table
    ref_csv = _make_csv_bytes([
        {"drug_name": "Keytruda", "category": "Oncology"},
        {"drug_name": "Opdivo", "category": "Oncology"},
    ])
    import_reference_table(
        uploaded_file=io.BytesIO(ref_csv),
        original_filename="categories.csv",
        registered_root=ref_dir,
        overwrite=True,
    )

    # Run SQL with reference=None — simulates frontend after restart
    resp = _run_sql(
        client,
        "SELECT d.drug_name, r.category "
        "FROM dataset d INNER JOIN reference r ON d.drug_name = r.drug_name",
        reference=None,  # NOT passed — auto-detect should kick in
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    rows = resp.json()["rows"]
    assert len(rows) == 2


def test_reference_auto_detected_by_table_name(client, env_dirs):
    """
    Bug #10 regression: SQL uses the actual reference table name
    (e.g. JOIN usp_guard_categories) without frontend passing reference.
    The backend should still resolve it.
    """
    ref_dir = env_dirs["ref_dir"]

    ref_csv = _make_csv_bytes([
        {"drug_name": "Keytruda", "category": "Oncology"},
    ])
    import_reference_table(
        uploaded_file=io.BytesIO(ref_csv),
        original_filename="my_categories.csv",
        registered_root=ref_dir,
        overwrite=True,
    )

    # Use the actual registered name in SQL, not "reference"
    resp = _run_sql(
        client,
        "SELECT d.drug_name, r.category "
        "FROM dataset d INNER JOIN my_categories r ON d.drug_name = r.drug_name",
        reference=None,  # NOT passed
    )
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    rows = resp.json()["rows"]
    assert len(rows) == 1
    assert rows[0]["drug_name"] == "Keytruda"
