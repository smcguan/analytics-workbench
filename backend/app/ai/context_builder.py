from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import duckdb
from fastapi import HTTPException


def _sql_escape_path(p: str) -> str:
    return p.replace("'", "''")


def _is_numeric_type(type_name: str) -> bool:
    t = type_name.upper()
    return any(
        x in t
        for x in [
            "INT",
            "DOUBLE",
            "FLOAT",
            "DECIMAL",
            "REAL",
            "HUGEINT",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
        ]
    )


def _is_text_type(type_name: str) -> bool:
    t = type_name.upper()
    return any(x in t for x in ["CHAR", "VARCHAR", "STRING", "TEXT"])


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def build_context(
    dataset_name: str,
    dataset_source_path_fn,
    max_sample_rows: int = 5,
    max_categorical_values: int = 10,
) -> dict[str, Any]:
    """
    Build a compact schema-aware context for the AI SQL generator.
    Includes columns, sample rows, numeric stats, and low-cardinality
    categorical values to improve SQL generation quality.
    """

    try:
        src, _is_glob = dataset_source_path_fn(dataset_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception:
        raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset_name}")

    esc = _sql_escape_path(src)

    con = duckdb.connect()

    try:
        schema_cur = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{esc}')"
        )

        schema_rows = schema_cur.fetchall()

        columns: list[dict[str, str]] = []
        for row in schema_rows:
            columns.append(
                {
                    "name": str(row[0]),
                    "type": str(row[1]),
                }
            )

        sample_cur = con.execute(
            f"SELECT * FROM read_parquet('{esc}') LIMIT {int(max_sample_rows)}"
        )

        sample_cols = [d[0] for d in sample_cur.description]
        sample_rows_raw = sample_cur.fetchall()
        sample_rows = [
            {k: _json_safe_value(v) for k, v in zip(sample_cols, row)}
            for row in sample_rows_raw
        ]

        numeric_stats: list[dict[str, Any]] = []
        for col in columns:
            col_name = col["name"]
            col_type = col["type"]

            if not _is_numeric_type(col_type):
                continue

            try:
                stats = con.execute(
                    f'''
                    SELECT
                        MIN("{col_name}"),
                        MAX("{col_name}"),
                        AVG("{col_name}")
                    FROM read_parquet('{esc}')
                    '''
                ).fetchone()

                numeric_stats.append(
                    {
                        "column": col_name,
                        "min": _json_safe_value(stats[0]),
                        "max": _json_safe_value(stats[1]),
                        "avg": _json_safe_value(stats[2]),
                    }
                )
            except Exception:
                continue

        categorical_values: list[dict[str, Any]] = []
        for col in columns:
            col_name = col["name"]
            col_type = col["type"]

            if not _is_text_type(col_type):
                continue

            try:
                distinct_rows = con.execute(
                    f'''
                    SELECT DISTINCT "{col_name}"
                    FROM read_parquet('{esc}')
                    WHERE "{col_name}" IS NOT NULL
                    LIMIT {int(max_categorical_values) + 1}
                    '''
                ).fetchall()

                values = [
                    str(row[0]) for row in distinct_rows
                    if row and row[0] is not None
                ]

                if 0 < len(values) <= int(max_categorical_values):
                    categorical_values.append(
                        {
                            "column": col_name,
                            "values": values,
                        }
                    )
            except Exception:
                continue

        return {
            "dataset_name": dataset_name,
            "table_name": "dataset",
            "source_path": src,
            "columns": columns,
            "sample_rows": sample_rows,
            "numeric_stats": numeric_stats,
            "categorical_values": categorical_values,
        }

    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to inspect dataset '{dataset_name}': {e}",
        )

    finally:
        con.close()