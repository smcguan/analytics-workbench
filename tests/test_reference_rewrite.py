"""
test_reference_rewrite.py — unit tests for reference table SQL rewriting

Tests that _rewrite_sql_dataset_reference correctly handles the reference_parquet_sql
parameter for FROM reference / JOIN reference rewriting.

Covers:
  JOIN reference rewriting alongside FROM dataset
  FROM reference as primary table (rare but valid)
  LEFT JOIN / INNER JOIN / RIGHT JOIN reference
  Error when SQL uses 'reference' but no reference table loaded
  Reference in subqueries and CTEs
  Mixed dataset + reference in complex queries

Run from project root:
    pytest tests/test_reference_rewrite.py -v
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.main import _rewrite_sql_dataset_reference


PARQUET = "read_parquet('/data/test/source.parquet')"
REF_PARQUET = "read_parquet('/data/references/ira_exclusions/source.parquet')"


# ===========================================================================
# BASIC REFERENCE JOIN REWRITING
# ===========================================================================

def test_join_reference_rewritten():
    sql = "SELECT d.*, r.category FROM dataset d JOIN reference r ON d.drug = r.drug"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert PARQUET in result
    assert REF_PARQUET in result
    assert "JOIN reference" not in result
    assert "FROM dataset" not in result


def test_left_join_reference_rewritten():
    sql = "SELECT * FROM dataset LEFT JOIN reference ON dataset.code = reference.code"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert PARQUET in result
    assert REF_PARQUET in result


def test_inner_join_reference_rewritten():
    sql = "SELECT * FROM dataset INNER JOIN reference ON dataset.id = reference.id"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert REF_PARQUET in result


def test_reference_case_insensitive():
    sql = "SELECT * FROM dataset JOIN REFERENCE ON 1=1"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert REF_PARQUET in result


# ===========================================================================
# ERROR — SQL uses 'reference' but no reference table loaded
# ===========================================================================

def test_join_reference_without_loaded_table_raises_400():
    sql = "SELECT * FROM dataset JOIN reference ON dataset.drug = reference.drug"
    with pytest.raises(HTTPException) as exc_info:
        _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, reference_parquet_sql=None)
    assert exc_info.value.status_code == 400
    assert "no reference table is loaded" in exc_info.value.detail.lower()


def test_from_reference_without_loaded_table_raises_400():
    sql = "SELECT * FROM reference LIMIT 10"
    # First need FROM dataset to pass the primary check
    sql = "SELECT r.* FROM dataset d JOIN reference r ON d.id = r.id"
    with pytest.raises(HTTPException) as exc_info:
        _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, reference_parquet_sql=None)
    assert exc_info.value.status_code == 400


# ===========================================================================
# NO REFERENCE — SQL without 'reference' should work fine
# ===========================================================================

def test_no_reference_in_sql_works_without_reference_loaded():
    """SQL that doesn't use reference table should work even without one loaded."""
    sql = "SELECT * FROM dataset LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, reference_parquet_sql=None)
    assert PARQUET in result


def test_no_reference_in_sql_works_with_reference_loaded():
    """Reference table loaded but SQL doesn't use it — should be fine."""
    sql = "SELECT * FROM dataset LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert PARQUET in result
    assert REF_PARQUET not in result


# ===========================================================================
# COMPLEX QUERIES WITH BOTH TABLES
# ===========================================================================

def test_cte_with_reference_join():
    sql = (
        "WITH filtered AS ("
        "  SELECT d.drug, d.spending FROM dataset d "
        "  JOIN reference r ON d.drug LIKE r.drug_pattern "
        "  WHERE r.excluded = 1"
        ") SELECT * FROM filtered ORDER BY spending DESC"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert PARQUET in result
    assert REF_PARQUET in result
    assert "FROM filtered" in result  # CTE reference preserved


def test_subquery_with_reference():
    sql = (
        "SELECT * FROM dataset "
        "WHERE drug NOT IN (SELECT drug FROM reference WHERE excluded = 1)"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert PARQUET in result
    assert REF_PARQUET in result


def test_multiple_references_in_query():
    """Two JOINs on reference — both should be rewritten."""
    sql = (
        "SELECT d.drug, r1.category "
        "FROM dataset d "
        "JOIN reference r1 ON d.drug = r1.drug "
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert PARQUET in result
    assert REF_PARQUET in result


# ===========================================================================
# COLUMN ALIASES AND STRING VALUES — must NOT rewrite 'reference'
# ===========================================================================

def test_reference_in_string_value_not_rewritten():
    sql = "SELECT * FROM dataset WHERE source = 'reference'"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert "'reference'" in result
    assert PARQUET in result


def test_reference_in_column_alias_not_rewritten():
    sql = "SELECT drug AS reference_drug FROM dataset"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert "reference_drug" in result


# ===========================================================================
# WHERE CLAUSE PRESERVATION WITH REFERENCE
# ===========================================================================

def test_where_clause_preserved_with_reference_join():
    sql = (
        "SELECT d.drug, d.spending "
        "FROM dataset d "
        "JOIN reference r ON d.drug LIKE r.drug_name "
        "WHERE d.spending > 1000000 "
        "ORDER BY d.spending DESC"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET, REF_PARQUET)
    assert "WHERE d.spending > 1000000" in result
    assert "ORDER BY d.spending DESC" in result
