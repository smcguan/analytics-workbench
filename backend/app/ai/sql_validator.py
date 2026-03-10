from __future__ import annotations

import re
from typing import Callable


_BLOCKED_KEYWORDS = [
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "copy",
    "attach",
    "detach",
    "call",
]


def _sql_escape_path(p: str) -> str:
    return p.replace("'", "''")


def validate_generated_sql(sql: str) -> tuple[bool, str]:
    s = (sql or "").strip()
    if not s:
        return False, "Generated SQL is empty."

    lowered = s.lower()

    if ";" in s.rstrip(";"):
        return False, "Multiple SQL statements are not allowed."

    if not (lowered.startswith("select") or lowered.startswith("with")):
        return False, "Only SELECT and WITH queries are allowed."

    for token in _BLOCKED_KEYWORDS:
        if re.search(rf"\b{token}\b", lowered):
            return False, f"Blocked SQL keyword detected: {token}"

    return True, ""


def validate_sql_with_duckdb(
    sql: str,
    dataset_name: str,
    dataset_source_path_fn: Callable[[str], tuple[str, bool]],
) -> tuple[bool, str]:
    try:
        import duckdb

        src, _is_glob = dataset_source_path_fn(dataset_name)
        esc = _sql_escape_path(src)

        con = duckdb.connect()
        try:
            con.execute(f"CREATE OR REPLACE TEMP VIEW dataset AS SELECT * FROM read_parquet('{esc}')")
            con.execute(f"EXPLAIN {sql}")
        finally:
            con.close()

        return True, ""

    except Exception as e:
        return False, f"Generated SQL failed DuckDB validation: {e}"