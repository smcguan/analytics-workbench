"""
test_sql_rewrite.py — unit tests for _rewrite_sql_dataset_reference

This function is in the critical execution path of /api/sql. If it rewrites
SQL incorrectly, the user gets wrong results silently — the worst failure mode.

Covers:
  FROM dataset / JOIN dataset rewriting
  FROM <dataset_name> backward compatibility
  Quoted identifiers: FROM "dataset"
  Column aliases and string values that contain 'dataset' — must NOT be rewritten
  No FROM clause — must raise 400
  CTE + subquery combinations
  Multiple FROM/JOIN in a single query

Run from project root:
    pytest tests/test_sql_rewrite.py -v
"""
from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.main import _rewrite_sql_dataset_reference


PARQUET = "read_parquet('/data/test/source.parquet')"


# ===========================================================================
# BASIC REWRITING — FROM dataset
# ===========================================================================

def test_from_dataset_rewritten():
    sql = "SELECT * FROM dataset LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert f"FROM {PARQUET}" in result
    assert "FROM dataset" not in result


def test_from_dataset_case_insensitive():
    sql = "SELECT * FROM DATASET LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result


def test_from_dataset_with_extra_whitespace():
    sql = "SELECT *\n  FROM   dataset\n  LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result


# ===========================================================================
# BACKWARD COMPATIBILITY — FROM <dataset_name>
# ===========================================================================

def test_from_dataset_name_rewritten():
    sql = "SELECT * FROM mydata LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result
    assert "FROM mydata" not in result


def test_from_dataset_name_case_insensitive():
    sql = "SELECT * FROM MYDATA LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result


# ===========================================================================
# QUOTED IDENTIFIERS
# ===========================================================================

def test_from_quoted_dataset():
    sql = 'SELECT * FROM "dataset" LIMIT 10'
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result


def test_from_quoted_dataset_name():
    sql = 'SELECT * FROM "mydata" LIMIT 10'
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result


# ===========================================================================
# JOIN REWRITING
# ===========================================================================

def test_join_dataset_rewritten():
    sql = "SELECT a.* FROM dataset a JOIN dataset b ON a.id = b.id"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert result.count(PARQUET) == 2
    assert "FROM dataset" not in result
    assert "JOIN dataset" not in result


def test_left_join_dataset_rewritten():
    sql = "SELECT * FROM dataset LEFT JOIN dataset ON 1=1"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert result.count(PARQUET) == 2


# ===========================================================================
# MUST NOT REWRITE — column aliases, string values, WHERE clauses
# ===========================================================================

def test_dataset_in_column_alias_not_rewritten():
    """'dataset' in a column alias must not be touched."""
    sql = "SELECT COUNT(*) AS dataset_count FROM dataset"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "dataset_count" in result
    assert f"FROM {PARQUET}" in result


def test_dataset_in_select_expression_not_rewritten():
    """'dataset' appearing as a bare identifier in SELECT should not be rewritten."""
    sql = "SELECT 'dataset' AS label FROM dataset"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "'dataset'" in result
    assert f"FROM {PARQUET}" in result


def test_dataset_name_in_where_value_not_rewritten():
    """The dataset name in a WHERE value should not be touched."""
    sql = "SELECT * FROM dataset WHERE source = 'mydata'"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "'mydata'" in result
    assert f"FROM {PARQUET}" in result


# ===========================================================================
# NO FROM CLAUSE — must raise 400
# ===========================================================================

def test_no_from_clause_raises_400():
    sql = "SELECT 1 + 1 AS result"
    with pytest.raises(HTTPException) as exc_info:
        _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert exc_info.value.status_code == 400


def test_from_wrong_table_raises_400():
    sql = "SELECT * FROM other_table LIMIT 10"
    with pytest.raises(HTTPException) as exc_info:
        _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert exc_info.value.status_code == 400


# ===========================================================================
# CTE / SUBQUERY COMBINATIONS
# ===========================================================================

def test_cte_with_from_dataset():
    sql = "WITH top AS (SELECT * FROM dataset LIMIT 5) SELECT * FROM top"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result
    # 'FROM top' should NOT be rewritten — it's a CTE reference
    assert "FROM top" in result


def test_subquery_from_dataset():
    sql = "SELECT * FROM (SELECT drug_name FROM dataset) sub LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert PARQUET in result


# ===========================================================================
# DATASET NAME EDGE CASES
# ===========================================================================

def test_empty_dataset_name_only_rewrites_dataset():
    """If dataset_name is empty string, only 'dataset' keyword is matched."""
    sql = "SELECT * FROM dataset LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "", PARQUET)
    assert PARQUET in result


def test_dataset_name_with_underscores():
    sql = "SELECT * FROM cms_part_b_2024 LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "cms_part_b_2024", PARQUET)
    assert PARQUET in result


def test_dataset_name_with_numbers():
    sql = "SELECT * FROM data2024 LIMIT 10"
    result = _rewrite_sql_dataset_reference(sql, "data2024", PARQUET)
    assert PARQUET in result


# ===========================================================================
# WHERE CLAUSE PRESERVATION
# ===========================================================================

def test_where_clause_preserved_after_rewrite():
    """The WHERE clause must survive rewriting unchanged."""
    sql = "SELECT * FROM dataset WHERE drug_name = 'DrugA' AND total_paid > 100"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "WHERE drug_name = 'DrugA' AND total_paid > 100" in result


def test_complex_where_with_subquery_preserved():
    sql = (
        "SELECT * FROM dataset "
        "WHERE total_paid > (SELECT AVG(total_paid) FROM dataset)"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    # Both FROM dataset references should be rewritten
    assert result.count(PARQUET) == 2
    assert "WHERE total_paid >" in result


def test_order_by_and_group_by_preserved():
    sql = (
        "SELECT drug_name, SUM(total_paid) AS total "
        "FROM dataset "
        "GROUP BY drug_name "
        "ORDER BY total DESC "
        "LIMIT 10"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "GROUP BY drug_name" in result
    assert "ORDER BY total DESC" in result
    assert "LIMIT 10" in result


# ===========================================================================
# BUG #12 REGRESSION — ORDER BY DESC must never be corrupted
# ===========================================================================
# Bug #12: Parser was appending extra characters to DESC keyword (e.g.
# "DESCSC"). This test battery ensures DESC/ASC survive rewriting intact
# across all common SQL patterns. If any of these fail, the _SQL_KW
# keyword list in _rewrite_sql_dataset_reference is likely incomplete.

def test_bug12_desc_not_corrupted():
    """DESC keyword must appear exactly as written after rewriting."""
    sql = "SELECT * FROM dataset ORDER BY total_paid DESC"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "DESC" in result
    assert "DESCSC" not in result
    assert "ORDER BY total_paid DESC" in result


def test_bug12_asc_not_corrupted():
    """ASC keyword must appear exactly as written after rewriting."""
    sql = "SELECT * FROM dataset ORDER BY drug_name ASC"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "ASC" in result
    assert "ORDER BY drug_name ASC" in result


def test_bug12_desc_with_multiline_sql():
    """Multiline SQL with ORDER BY DESC on its own line."""
    sql = (
        "SELECT drug_name, SUM(total_paid) AS total\n"
        "FROM dataset\n"
        "GROUP BY drug_name\n"
        "ORDER BY total DESC\n"
        "LIMIT 20"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "ORDER BY total DESC" in result
    assert "LIMIT 20" in result


def test_bug12_desc_in_subquery():
    """ORDER BY DESC inside a subquery must survive rewriting."""
    sql = (
        "SELECT * FROM "
        "(SELECT drug_name, total_paid FROM dataset ORDER BY total_paid DESC LIMIT 10) sub"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "ORDER BY total_paid DESC" in result


def test_bug12_multiple_order_columns():
    """Multiple ORDER BY columns with DESC/ASC must survive."""
    sql = (
        "SELECT * FROM dataset "
        "ORDER BY total_paid DESC, drug_name ASC"
    )
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "ORDER BY total_paid DESC, drug_name ASC" in result


def test_bug12_nulls_first_last():
    """NULLS FIRST / NULLS LAST must survive rewriting."""
    sql = "SELECT * FROM dataset ORDER BY total_paid DESC NULLS LAST"
    result = _rewrite_sql_dataset_reference(sql, "mydata", PARQUET)
    assert "DESC NULLS LAST" in result
