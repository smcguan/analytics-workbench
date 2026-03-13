from __future__ import annotations

"""
============================================================
FILE: main.py
LOCATION: backend/app/main.py
============================================================

PURPOSE
-------
This is the main FastAPI application entrypoint for
Analytics Workbench.

This file is responsible for:

1. Loading environment configuration
2. Defining runtime paths
3. Creating the FastAPI app
4. Mounting the frontend UI
5. Registering API endpoints
6. Managing datasets and metadata
7. Running SQL safely against local Parquet data
8. Supporting AI-assisted SQL features through the
   separate AI router
9. Building dataset context used by both:
   - dataset inspection
   - schema-aware AI prompting
   - AI-suggested question chips

HIGH-LEVEL ARCHITECTURE
-----------------------

Frontend UI
    ->
FastAPI routes in this file
    ->
DuckDB query execution / dataset inspection
    ->
Local Parquet files

SEPARATE AI FLOW
----------------
The AI-specific routes are defined separately in:

    app.ai.routes

That AI layer handles:

    selected dataset
        ->
    dataset context
        ->
    schema-aware prompt building
        ->
    OpenAI SQL generation
        ->
    OpenAI suggested question generation
        ->
    SQL validation

CURRENT PRODUCT WORKFLOW
------------------------
The intended user workflow is:

    select dataset
        ->
    review AI-suggested question chips
        ->
    ask question or click a suggestion
        ->
    generate SQL with AI
        ->
    review/edit SQL in SQL workspace
        ->
    run SQL manually

This keeps the human in the loop and makes the system
more transparent and trustworthy.

IMPORTANT DESIGN PRINCIPLE
--------------------------
This application is intentionally built as an
AI-assisted analytics workbench, not a fully autonomous
AI agent.

AI can:
- suggest useful questions
- generate SQL drafts

But the user remains in control of execution.

IMPORTANT SQL EXECUTION CONVENTION
----------------------------------
The SQL execution layer ultimately runs queries against a
local Parquet file, not against a persistent DuckDB table.

To keep the workflow simple, the SQL editor / AI layer
should generally write SQL against a logical source table:

    dataset

Example:
    SELECT * FROM dataset LIMIT 5

At execution time, this file rewrites that logical table
reference to:

    read_parquet('...actual path...')

For resilience, the execution layer also supports SQL that
uses the currently selected dataset name in FROM/JOIN
clauses, such as:

    SELECT * FROM sample LIMIT 5

This prevents mismatches between AI-generated SQL and the
backend execution path.

============================================================
"""

# ============================================================
# EARLY ENVIRONMENT LOADING
# ------------------------------------------------------------
# We load .env as early as possible because later imports and
# path calculations may depend on environment variables.
#
# Supported lookup locations:
# - current working directory
# - packaged EXE directory
# - known local dev path fallback
# ============================================================

from pathlib import Path
import sys

from dotenv import load_dotenv

early_env_candidates = [
    Path.cwd() / ".env",
    Path(sys.executable).parent / ".env",
    Path("C:/dev/AnalyticsWorkbench-Claude/.env"),
]

for env_path in early_env_candidates:
    if env_path.exists():
        load_dotenv(env_path)
        break


# ============================================================
# STANDARD IMPORTS
# ============================================================

import getpass
import json
import logging
import os
import platform
import re
import shutil
import time
from datetime import datetime
from typing import Any

import duckdb
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import Response

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from app.presets.doge import PRESETS  # noqa: E402
from app.ai.routes import router as ai_router  # noqa: E402


# ============================================================
# LOGGING / APP INSTANCE
# ============================================================

logger = logging.getLogger("app")
app = FastAPI(title="Analytics Workbench")

APP_VERSION = "1.1.0"


# ============================================================
# BASE DIRECTORY RESOLUTION
# ------------------------------------------------------------
# Packaged mode:
#   use the folder containing the EXE
#
# Development mode:
#   backend/app/main.py -> repo root
# ============================================================

def app_base_dir() -> Path:
    """
    Return the application base directory.

    Packaged:
        folder containing the EXE

    Development:
        repo root directory
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    return Path(__file__).resolve().parents[2]


BASE_DIR = app_base_dir()


# ============================================================
# LOCAL .ENV LOADING
# ------------------------------------------------------------
# This supports both:
# - local development
# - packaged desktop execution
#
# We intentionally do not hardcode secrets here.
# ============================================================

def _load_local_env() -> None:
    """Load a local .env file if one exists."""
    if load_dotenv is None:
        logger.info("python-dotenv not installed; skipping .env load")
        return

    candidates: list[Path] = []

    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).resolve().parent / ".env")
        candidates.append(Path.cwd() / ".env")
    else:
        current = Path(__file__).resolve()
        candidates.extend(
            [
                current.parents[2] / ".env",
                current.parents[1] / ".env",
                current.parent / ".env",
                Path.cwd() / ".env",
            ]
        )

    seen: set[str] = set()

    for env_path in candidates:
        try:
            env_resolved = str(env_path.resolve())
        except Exception:
            env_resolved = str(env_path)

        if env_resolved in seen:
            continue
        seen.add(env_resolved)

        logger.info("checking .env candidate | path=%s", env_path)

        try:
            if env_path.exists():
                load_dotenv(env_path, override=False)
                logger.info("loaded .env | path=%s", env_path)
                logger.info(
                    "OPENAI_API_KEY present after load: %s",
                    bool(os.getenv("OPENAI_API_KEY")),
                )
                return
        except Exception as e:
            logger.warning("failed loading .env | path=%s | reason=%s", env_path, e)

    logger.warning("no .env file loaded from any candidate path")


_load_local_env()


# ============================================================
# RUNTIME PATHS
# ------------------------------------------------------------
# These define where the app expects:
# - frontend files
# - datasets
# - exports
# - saved query metadata
# - cached dataset context
#
# Dataset context is now an important part of the product
# because it supports:
# - the Inspect Dataset panel
# - schema-aware AI prompting
# - suggested question chips in the frontend
# ============================================================

FRONTEND_DIR = Path(os.getenv("AW_FRONTEND_DIR", str(BASE_DIR / "frontend")))
DATA_DIR = Path(os.getenv("AW_DATA_DIR", str(BASE_DIR / "data")))
DATASETS_DIR = Path(os.getenv("AW_DATASETS_DIR", str(DATA_DIR / "datasets")))
EXPORTS_DIR = Path(os.getenv("AW_EXPORTS_DIR", str(BASE_DIR / "exports")))
QUERIES_PATH = Path(os.getenv("AW_QUERIES_PATH", str(DATA_DIR / "queries.json")))
DATASET_CONTEXT_FILENAME = "dataset_context.json"

DATASETS_DIR.mkdir(parents=True, exist_ok=True)
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# PREVIEW / EXPORT SAFETY LIMITS
# ------------------------------------------------------------
# These caps protect the UI and local machine from returning
# excessively large result sets.
# ============================================================

DEFAULT_PREVIEW_ROWS = int(os.getenv("AW_DEFAULT_PREVIEW_ROWS", "50"))
MAX_PREVIEW_ROWS = int(os.getenv("AW_MAX_PREVIEW_ROWS", "200"))
MAX_EXPORT_ROWS = int(os.getenv("AW_MAX_EXPORT_ROWS", "200000"))


# ============================================================
# STARTUP LOGGING
# ============================================================

logger.info(
    "app started | mode=%s | exports_dir=%s",
    "packaged" if getattr(sys, "frozen", False) else "dev",
    EXPORTS_DIR,
)


# ============================================================
# REGISTER AI ROUTER
# ------------------------------------------------------------
# This attaches the separate AI module endpoints, such as:
#
#   /api/ai/generate_sql
#   /api/ai/suggest_questions
#
# Those routes handle:
# - schema-aware AI SQL generation
# - AI-suggested question generation for the frontend chips
# ============================================================

app.include_router(ai_router)


# ============================================================
# PRESET LOADING
# ------------------------------------------------------------
# Presets are reusable query templates.
#
# The app tries to load presets.json first, and if that
# fails, falls back to the Python PRESETS constant.
# ============================================================

_REQUIRED_PRESET_FIELDS = {"id", "name", "sql"}


def _validate_presets(raw: list[Any]) -> list[dict[str, Any]]:
    """Keep only valid preset dicts."""
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if not _REQUIRED_PRESET_FIELDS.issubset(item.keys()):
            continue
        out.append(item)
    return out


def _load_presets() -> list[dict[str, Any]]:
    """
    Load presets from JSON if available.

    Fallback:
        use Python PRESETS constant
    """
    explicit = os.getenv("AW_PRESETS_PATH")
    candidates: list[Path] = []

    if explicit:
        candidates.append(Path(explicit))

    candidates.extend(
        [
            BASE_DIR / "presets.json",
            DATA_DIR / "presets.json",
            DATASETS_DIR / "presets.json",
        ]
    )

    for fp in candidates:
        try:
            if fp.exists() and fp.is_file():
                raw = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    validated = _validate_presets(raw)
                    if validated:
                        logger.info(
                            "loaded presets.json | path=%s count=%d",
                            fp,
                            len(validated),
                        )
                        return validated
        except Exception as e:
            logger.warning("failed to load presets.json | path=%s | reason=%s", fp, e)

    return PRESETS


ACTIVE_PRESETS = _load_presets()


# ============================================================
# REQUEST MODELS
# ------------------------------------------------------------
# These define the input structure for FastAPI endpoints.
# ============================================================

class ScanRequest(BaseModel):
    path: str
    recursive: bool = False


class RegisterRequest(BaseModel):
    dataset_name: str
    parquet_path: str
    mode: str = "reference"  # "reference" or "copy"


class SaveQueryRequest(BaseModel):
    name: str
    dataset: str
    type: str = "preset"
    preset: str | None = None
    sql: str | None = None
    params: dict[str, Any] = {}


class DeleteQueryRequest(BaseModel):
    name: str


class SqlRequest(BaseModel):
    dataset: str
    sql: str

class SqlExportRequest(BaseModel):
    dataset: str
    sql: str
    format: str = "xlsx"


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def _safe_name(name: str) -> str:
    """Convert a dataset name into a safe filesystem name."""
    name = (name or "").strip()
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name[:64] if name else "Dataset"


def list_datasets() -> list[str]:
    """
    Return all valid registered datasets.

    A dataset is considered valid if:
    - its folder contains parquet files, or
    - it contains a valid _reference.txt pointer
    """
    if not DATASETS_DIR.exists():
        return []

    out: list[str] = []

    for ds in DATASETS_DIR.iterdir():
        if not ds.is_dir():
            continue

        if any(ds.glob("*.parquet")):
            out.append(ds.name)
            continue

        ref = ds / "_reference.txt"
        if ref.exists():
            ref_path = ref.read_text(encoding="utf-8").strip()
            if ref_path and Path(ref_path).exists():
                out.append(ds.name)

    return sorted(out)


def dataset_source_path(dataset: str) -> tuple[str, bool]:
    """
    Return (path_or_glob, is_glob) for a dataset.

    Reference mode:
        returns absolute parquet file path

    Copy mode:
        returns datasets/<dataset>/*.parquet glob
    """
    ds_dir = (DATASETS_DIR / dataset).resolve()
    ref = ds_dir / "_reference.txt"

    if ref.exists():
        target = ref.read_text(encoding="utf-8").strip()
        if not target:
            raise FileNotFoundError(
                f"Reference dataset '{dataset}' has empty _reference.txt"
            )

        p = Path(target)
        if not p.exists():
            raise FileNotFoundError(
                f"Reference dataset '{dataset}' points to missing file: {target}"
            )

        return (str(p.resolve()).replace("\\", "/"), False)

    glob_path = str((ds_dir / "*.parquet").resolve()).replace("\\", "/")
    return (glob_path, True)


def _dataset_dir(dataset: str) -> Path:
    return (DATASETS_DIR / dataset).resolve()


def _dataset_mode(dataset: str) -> str:
    ds_dir = _dataset_dir(dataset)
    if (ds_dir / "_reference.txt").exists():
        return "reference"
    return "copy"


def get_preset(preset_id: str) -> dict[str, Any] | None:
    for p in ACTIVE_PRESETS:
        if p.get("id") == preset_id:
            return p
    return None


def _connect() -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection.

    This app uses fileless embedded connections, which is
    fine for local query/extract workloads.
    """
    return duckdb.connect()


def _sql_escape_path(p: str) -> str:
    """Escape single quotes before embedding a path into SQL."""
    return p.replace("'", "''")


def _is_writable_dir(p: Path) -> tuple[bool, str | None]:
    """Return whether a directory is writable."""
    try:
        p.mkdir(parents=True, exist_ok=True)
        test = p / ".__aw_write_test__"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
        return True, None
    except Exception as e:
        return False, str(e)


def _duckdb_ok() -> tuple[bool, str | None]:
    """Basic DuckDB health check."""
    try:
        con = _connect()
        try:
            v = con.execute("SELECT 1").fetchone()
            if not v or v[0] != 1:
                return False, "DuckDB SELECT 1 returned unexpected result"
            return True, None
        finally:
            con.close()
    except Exception as e:
        return False, str(e)


# ============================================================
# DATASET METADATA SUMMARY
# ------------------------------------------------------------
# This is used to populate dataset lists and summary panels.
#
# We prefer cached _meta.json when available because that is
# faster than recomputing live counts every time.
# ============================================================

def _dataset_meta_summary(dataset: str) -> dict[str, Any]:
    """
    Return a lightweight metadata summary for a dataset.

    Prefers cached _meta.json.
    Falls back to live computation.
    """
    ds_dir = _dataset_dir(dataset)
    meta_path = ds_dir / "_meta.json"

    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            return {
                "name": dataset,
                "row_count": meta.get("row_count"),
                "column_count": meta.get("column_count"),
                "file_size_bytes": meta.get("file_size_bytes"),
                "meta_source": "cached",
            }
        except Exception:
            pass

    src, is_glob = dataset_source_path(dataset)
    esc = _sql_escape_path(src)

    con = _connect()
    try:
        row_count = int(
            con.execute(f"SELECT COUNT(*) FROM read_parquet('{esc}')").fetchone()[0]
        )
        col_count = int(
            con.execute(f"SELECT COUNT(*) FROM parquet_schema('{esc}')").fetchone()[0]
        )
    finally:
        con.close()

    if is_glob:
        file_size_bytes = sum(
            f.stat().st_size for f in ds_dir.glob("*.parquet") if f.is_file()
        )
    else:
        ref = ds_dir / "_reference.txt"
        ref_path = Path(ref.read_text(encoding="utf-8").strip())
        file_size_bytes = ref_path.stat().st_size if ref_path.exists() else None

    return {
        "name": dataset,
        "row_count": row_count,
        "column_count": col_count,
        "file_size_bytes": file_size_bytes,
        "meta_source": "live",
    }


# ============================================================
# AUDIT LOGGING
# ------------------------------------------------------------
# The audit log is append-only JSONL and is intentionally
# non-fatal: failure to write the audit log must never crash
# the user workflow.
# ============================================================

def _audit_log(event: dict[str, Any]) -> None:
    """Append an event to the JSONL audit log."""
    try:
        audit_path = DATASETS_DIR / "_audit.jsonl"

        try:
            user = getpass.getuser()
        except Exception:
            user = "unknown"

        try:
            machine = platform.node()
        except Exception:
            machine = "unknown"

        payload = dict(event)
        payload.setdefault("ts", datetime.now().isoformat(timespec="seconds"))
        payload.setdefault("user", user)
        payload.setdefault("machine", machine)

        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning("audit log write failed | reason=%s", e)


# ============================================================
# DYNAMIC PRESET PARAM HELPERS
# ============================================================

def _extract_dynamic_params(
    request: Request,
    reserved: set[str],
    threshold_fallback: int | None,
) -> dict[str, Any]:
    """
    Pull non-reserved query params from the request.

    These remain strings because SQL template formatting later
    uses them directly.
    """
    provided: dict[str, Any] = {}

    for k, v in request.query_params.items():
        if k in reserved:
            continue
        provided[k] = v

    if threshold_fallback is not None and "threshold" not in provided:
        provided["threshold"] = threshold_fallback

    return provided


def _build_final_params(
    preset_def: dict[str, Any],
    provided_params: dict[str, Any],
) -> dict[str, Any]:
    """
    Merge preset defaults with provided params and validate
    that required placeholders exist.
    """
    final_params = dict(preset_def.get("params", {}) or {})
    final_params.update({k: v for k, v in provided_params.items() if v is not None})

    try:
        preset_def["sql"].format(**final_params)
    except KeyError as e:
        missing = str(e).strip("'")
        raise HTTPException(
            status_code=400,
            detail=f"Missing required preset param: {missing}",
        )

    return final_params


def _sql_for(
    dataset: str,
    preset_id: str,
    provided_params: dict[str, Any],
) -> tuple[str, str]:
    """
    Build final SQL for a preset and dataset.

    This replaces the logical token 'dataset' with a real
    read_parquet(...) expression.
    """
    preset_def = get_preset(preset_id)
    if not preset_def:
        raise HTTPException(status_code=400, detail=f"Unknown preset: {preset_id}")

    src, _is_glob = dataset_source_path(dataset)
    final_params = _build_final_params(preset_def, provided_params)
    sql = preset_def["sql"].format(**final_params)

    esc = _sql_escape_path(src)
    sql = sql.replace("parquet_schema(dataset)", f"parquet_schema('{esc}')")
    sql = sql.replace("parquet_metadata(dataset)", f"parquet_metadata('{esc}')")
    sql = sql.replace("dataset", f"read_parquet('{esc}')")

    return sql, src


def _normalize_saved_query_name(name: str) -> str:
    return (name or "").strip()


def _load_saved_queries() -> list[dict[str, Any]]:
    """Load saved queries from queries.json."""
    if not QUERIES_PATH.exists():
        return []

    try:
        raw = json.loads(QUERIES_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("queries.json load failed | path=%s", QUERIES_PATH)
        return []

    items = raw.get("queries", []) if isinstance(raw, dict) else []
    out: list[dict[str, Any]] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        name = _normalize_saved_query_name(str(item.get("name", "")))
        qtype = str(item.get("type", "preset")).strip().lower() or "preset"
        dataset = str(item.get("dataset", "")).strip()
        preset = str(item.get("preset", "")).strip()
        sql = str(item.get("sql", "")).strip()
        params = item.get("params", {})

        if not name or not dataset:
            continue
        if qtype == "preset" and not preset:
            continue
        if qtype == "sql" and not sql:
            continue
        if not isinstance(params, dict):
            params = {}

        record = {
            "name": name,
            "type": qtype,
            "dataset": dataset,
            "params": params,
        }

        if qtype == "preset":
            record["preset"] = preset
        else:
            record["sql"] = sql

        out.append(record)

    return out


def _save_saved_queries(items: list[dict[str, Any]]) -> None:
    """Write saved queries to disk."""
    QUERIES_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"queries": items}
    QUERIES_PATH.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _validate_readonly_sql(sql: str) -> str:
    """
    Allow only read-only SQL.

    Allowed:
        SELECT
        WITH ... SELECT

    Blocked:
        INSERT / UPDATE / DELETE / DROP / etc.
    """
    s = (sql or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="SQL is required.")

    lowered = s.lower()

    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise HTTPException(
            status_code=400,
            detail="Only SELECT and WITH queries are allowed.",
        )

    blocked = [
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "create",
        "copy",
        "attach",
        "detach",
    ]
    for token in blocked:
        if re.search(rf"\b{token}\b", lowered):
            raise HTTPException(
                status_code=400,
                detail=f"Blocked SQL keyword: {token}",
            )

    return s


def _strip_trailing_semicolon(sql: str) -> str:
    """
    Normalize SQL before we wrap it inside another query.

    WHY THIS EXISTS
    ---------------
    The SQL execution endpoint wraps user SQL like this:

        SELECT * FROM (<user_sql>) t LIMIT 200

    If the user SQL ends with a semicolon, DuckDB will reject
    the wrapped version:

        SELECT * FROM (SELECT ...;) t LIMIT 200

    So we strip trailing semicolons before wrapping.
    """
    cleaned = (sql or "").strip()
    while cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


def _rewrite_sql_dataset_reference(
    sql: str,
    dataset_name: str,
    parquet_sql: str,
) -> str:
    """
    Rewrite logical dataset references to read_parquet(...).

    SUPPORTED INPUT PATTERNS
    ------------------------
    We support two ways SQL may refer to the selected dataset:

    1. Preferred logical placeholder:
           FROM dataset
           JOIN dataset

    2. Backward-compatible selected dataset name:
           FROM sample
           JOIN sample

    This avoids runtime failures when older generated SQL uses
    the selected dataset name directly instead of the logical
    placeholder.

    IMPORTANT
    ---------
    We only rewrite dataset references in FROM/JOIN clauses.
    That avoids accidental replacements in column names,
    aliases, or free text.
    """
    rewritten = sql
    replaced_any = False

    identifiers_to_match = ["dataset", dataset_name]

    for ident in identifiers_to_match:
        if not ident:
            continue

        # Match:
        #   FROM dataset
        #   JOIN dataset
        #   FROM "dataset"
        #   JOIN "sample"
        #
        # We replace only the relation token following FROM/JOIN.
        pattern = re.compile(
            rf'(?i)\b(from|join)\s+(")?{re.escape(ident)}(")?\b'
        )

        def _repl(match: re.Match[str]) -> str:
            nonlocal replaced_any
            replaced_any = True
            keyword = match.group(1)
            return f"{keyword} {parquet_sql}"

        rewritten = pattern.sub(_repl, rewritten)

    if not replaced_any:
        raise HTTPException(
            status_code=400,
            detail=(
                "SQL must reference the selected dataset using "
                "FROM dataset / JOIN dataset "
                f"or FROM {dataset_name} / JOIN {dataset_name}."
            ),
        )

    return rewritten


# ============================================================
# DATASET CONTEXT BUILDING
# ------------------------------------------------------------
# This is one of the most important parts of the app for the
# AI workflow.
#
# The AI layer can use this context to become schema-aware.
#
# We build a structured description of the dataset including:
# - row count
# - column count
# - column names/types
# - semantic kind guesses
# - numeric stats
# - categorical top values
# - sample rows
#
# This context is useful in two major places:
#
# 1. Frontend dataset inspection
# 2. AI layer support:
#    - SQL generation
#    - suggested question chips
# ============================================================

def _dataset_context_path(dataset: str) -> Path:
    return (DATASETS_DIR / dataset / DATASET_CONTEXT_FILENAME).resolve()


def _classify_column_kind(col_type: str) -> str:
    """Classify a DuckDB column type into a coarse semantic kind."""
    t = (col_type or "").upper()

    if any(x in t for x in ["TIMESTAMP", "DATE", "TIME"]):
        return "datetime"
    if any(
        x in t
        for x in [
            "INT",
            "DECIMAL",
            "DOUBLE",
            "FLOAT",
            "REAL",
            "BIGINT",
            "SMALLINT",
            "HUGEINT",
        ]
    ):
        return "numeric"
    if "BOOL" in t:
        return "boolean"
    return "categorical"


def _preview_value(v: Any) -> Any:
    """Make preview values more compact for stored context."""
    if isinstance(v, float):
        return round(v, 4)
    return v


def _build_dataset_context(dataset: str) -> dict[str, Any]:
    """
    Build and save a rich dataset context JSON file.

    This supports:
    - schema-aware AI prompting
    - AI-suggested question generation
    - richer dataset inspection in the UI
    """
    src, _is_glob = dataset_source_path(dataset)
    esc = _sql_escape_path(src)
    ds_summary = _dataset_meta_summary(dataset)

    con = _connect()
    try:
        schema_cur = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{esc}')")
        schema_rows = schema_cur.fetchall()

        columns: list[dict[str, Any]] = []

        for row in schema_rows:
            col_name = row[0]
            col_type = row[1]
            kind = _classify_column_kind(col_type)

            entry: dict[str, Any] = {
                "name": col_name,
                "type": col_type,
                "kind": kind,
            }

            quoted = '"' + str(col_name).replace('"', '""') + '"'

            if kind == "numeric":
                try:
                    stats = con.execute(
                        f"""
                        SELECT
                            MIN({quoted}) AS min_value,
                            MAX({quoted}) AS max_value,
                            AVG({quoted}) AS avg_value,
                            SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS null_count
                        FROM read_parquet('{esc}')
                        """
                    ).fetchone()

                    entry["stats"] = {
                        "min": _preview_value(stats[0]),
                        "max": _preview_value(stats[1]),
                        "avg": _preview_value(stats[2]),
                        "null_count": int(stats[3] or 0),
                    }
                except Exception:
                    entry["stats"] = {}

            elif kind in {"categorical", "boolean"}:
                try:
                    top_vals = con.execute(
                        f"""
                        SELECT {quoted} AS value, COUNT(*) AS cnt
                        FROM read_parquet('{esc}')
                        GROUP BY {quoted}
                        ORDER BY cnt DESC
                        LIMIT 5
                        """
                    ).fetchall()

                    null_count = con.execute(
                        f"""
                        SELECT SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END)
                        FROM read_parquet('{esc}')
                        """
                    ).fetchone()[0]

                    entry["top_values"] = [
                        {"value": _preview_value(v), "count": int(c)}
                        for v, c in top_vals
                    ]
                    entry["null_count"] = int(null_count or 0)
                except Exception:
                    entry["top_values"] = []

            elif kind == "datetime":
                try:
                    stats = con.execute(
                        f"""
                        SELECT
                            MIN({quoted}) AS min_value,
                            MAX({quoted}) AS max_value,
                            SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS null_count
                        FROM read_parquet('{esc}')
                        """
                    ).fetchone()

                    entry["stats"] = {
                        "min": _preview_value(stats[0]),
                        "max": _preview_value(stats[1]),
                        "null_count": int(stats[2] or 0),
                    }
                except Exception:
                    entry["stats"] = {}

            columns.append(entry)

        sample_cur = con.execute(f"SELECT * FROM read_parquet('{esc}') LIMIT 5")
        sample_cols = [d[0] for d in sample_cur.description]
        sample_rows_raw = sample_cur.fetchall()
        sample_rows = [
            {k: _preview_value(v) for k, v in zip(sample_cols, row)}
            for row in sample_rows_raw
        ]

    finally:
        con.close()

    context = {
        "dataset": dataset,
        "row_count": ds_summary.get("row_count"),
        "column_count": ds_summary.get("column_count"),
        "file_size_bytes": ds_summary.get("file_size_bytes"),
        "meta_source": ds_summary.get("meta_source"),
        "columns": columns,
        "sample_rows": sample_rows,
    }

    ctx_path = _dataset_context_path(dataset)
    ctx_path.parent.mkdir(parents=True, exist_ok=True)
    ctx_path.write_text(
        json.dumps(context, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return context


def _load_dataset_context(dataset: str, refresh: bool = False) -> dict[str, Any]:
    """
    Load cached dataset context, rebuilding it when needed.
    """
    ctx_path = _dataset_context_path(dataset)

    if not refresh and ctx_path.exists():
        try:
            return json.loads(ctx_path.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("dataset context load failed | dataset=%s", dataset)

    return _build_dataset_context(dataset)


# ============================================================
# UI MOUNT
# ============================================================

@app.get("/")
def root():
    return RedirectResponse(url="/ui/")


if not (FRONTEND_DIR / "index.html").exists():
    raise RuntimeError(
        f"frontend/index.html not found at: {FRONTEND_DIR}\n"
        f"Expected frontend next to EXE (dist\\AnalyticsWorkbench\\frontend)."
    )

app.mount("/ui", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="ui")


@app.get("/ui/favicon.ico")
def favicon():
    """Silence favicon 404 noise."""
    return Response(status_code=204)


# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/api/version")
def api_version():
    return {
        "name": os.getenv("APP_NAME", "Analytics Workbench"),
        "version": os.getenv("APP_VERSION", APP_VERSION),
        "base_dir": str(BASE_DIR),
        "datasets_dir": str(DATASETS_DIR),
        "exports_dir": str(EXPORTS_DIR),
        "frontend_dir": str(FRONTEND_DIR),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "duckdb_version": duckdb.__version__,
        "frozen": bool(getattr(sys, "frozen", False)),
        "pid": os.getpid(),
    }


@app.get("/api/health")
def api_health():
    duck_ok, duck_err = _duckdb_ok()
    datasets_ok, datasets_err = _is_writable_dir(DATASETS_DIR)
    exports_ok, exports_err = _is_writable_dir(EXPORTS_DIR)

    frontend_ok = (FRONTEND_DIR / "index.html").exists()
    frontend_err = None if frontend_ok else f"Missing {FRONTEND_DIR / 'index.html'}"

    status = (
        "ok" if (duck_ok and datasets_ok and exports_ok and frontend_ok) else "degraded"
    )

    return {
        "status": status,
        "checks": {
            "duckdb": {"ok": duck_ok, "error": duck_err},
            "datasets_dir": {
                "ok": datasets_ok,
                "path": str(DATASETS_DIR),
                "error": datasets_err,
            },
            "exports_dir": {
                "ok": exports_ok,
                "path": str(EXPORTS_DIR),
                "error": exports_err,
            },
            "frontend": {
                "ok": frontend_ok,
                "path": str(FRONTEND_DIR),
                "error": frontend_err,
            },
        },
        "runtime": {
            "pid": os.getpid(),
            "frozen": bool(getattr(sys, "frozen", False)),
        },
    }


@app.get("/api/datasets")
def api_datasets():
    items: list[dict[str, Any]] = []

    for dataset in list_datasets():
        try:
            items.append(_dataset_meta_summary(dataset))
        except FileNotFoundError:
            items.append(
                {
                    "name": dataset,
                    "row_count": None,
                    "column_count": None,
                    "file_size_bytes": None,
                    "meta_source": "unavailable",
                    "error": "dataset source not found",
                }
            )
        except Exception as e:
            items.append(
                {
                    "name": dataset,
                    "row_count": None,
                    "column_count": None,
                    "file_size_bytes": None,
                    "meta_source": "error",
                    "error": str(e),
                }
            )

    return {"datasets": items}


@app.get("/api/datasets/{name}/meta")
def api_dataset_meta(name: str):
    """Return dataset metadata, preferring cached values."""
    ds_dir = _dataset_dir(name)
    if not ds_dir.exists() or not ds_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {name}")

    mode = _dataset_mode(name)

    try:
        src, is_glob = dataset_source_path(name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    meta_path = ds_dir / "_meta.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

        out = {
            "dataset": name,
            "mode": mode,
            "source_path": src,
            "is_glob": is_glob,
            "row_count": meta.get("row_count"),
            "column_count": meta.get("column_count"),
            "file_size_bytes": meta.get("file_size_bytes"),
            "last_scanned": meta.get("last_scanned"),
            "meta_source": "cached",
        }
        _audit_log(
            {
                "event": "meta",
                "status": "success",
                "dataset": name,
                "meta_source": "cached",
            }
        )
        return out

    t0 = time.perf_counter()
    con = _connect()
    try:
        esc = _sql_escape_path(src)

        try:
            col_count = int(
                con.execute(
                    f"SELECT COUNT(*) FROM parquet_schema('{esc}')"
                ).fetchone()[0]
            )
        except Exception:
            col_count = None

        try:
            row_count = int(
                con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{esc}')"
                ).fetchone()[0]
            )
        except Exception:
            row_count = None

        try:
            if mode == "reference":
                file_size_bytes = Path(src).stat().st_size
            else:
                file_size_bytes = sum(p.stat().st_size for p in ds_dir.glob("*.parquet"))
        except Exception:
            file_size_bytes = None

        elapsed = round(time.perf_counter() - t0, 4)

        out = {
            "dataset": name,
            "mode": mode,
            "source_path": src,
            "is_glob": is_glob,
            "row_count": row_count,
            "column_count": col_count,
            "file_size_bytes": file_size_bytes,
            "last_scanned": datetime.now().isoformat(timespec="seconds"),
            "meta_source": "live",
            "elapsed_seconds": elapsed,
        }

        _audit_log(
            {
                "event": "meta",
                "status": "success",
                "dataset": name,
                "meta_source": "live",
                "elapsed_seconds": elapsed,
            }
        )
        return out
    finally:
        con.close()


@app.get("/api/presets")
def api_presets():
    return {
        "presets": [
            {"id": p["id"], "name": p["name"], "params": p.get("params", {})}
            for p in PRESETS
        ]
    }


@app.get("/api/queries")
def api_queries():
    return {"queries": _load_saved_queries()}


@app.post("/api/queries/save")
def api_queries_save(req: SaveQueryRequest):
    name = _normalize_saved_query_name(req.name)
    if not name:
        raise HTTPException(status_code=400, detail="Query name is required.")

    if req.dataset not in list_datasets():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {req.dataset}")

    qtype = (req.type or "preset").strip().lower()
    params = req.params if isinstance(req.params, dict) else {}
    items = _load_saved_queries()

    if qtype == "preset":
        preset = (req.preset or "").strip()
        if not get_preset(preset):
            raise HTTPException(status_code=400, detail=f"Unknown preset: {preset}")

        record = {
            "name": name,
            "type": "preset",
            "dataset": req.dataset,
            "preset": preset,
            "params": params,
        }

    elif qtype == "sql":
        sql = _validate_readonly_sql(req.sql or "")
        record = {
            "name": name,
            "type": "sql",
            "dataset": req.dataset,
            "sql": sql,
            "params": {},
        }

    else:
        raise HTTPException(
            status_code=400,
            detail="Query type must be 'preset' or 'sql'.",
        )

    replaced = False
    for i, item in enumerate(items):
        if item.get("name") == name:
            items[i] = record
            replaced = True
            break
    if not replaced:
        items.append(record)

    _save_saved_queries(items)

    _audit_log(
        {
            "event": "query_saved",
            "status": "success",
            "name": name,
            "dataset": req.dataset,
            "query_type": record["type"],
        }
    )

    return {"ok": True, "saved_query": record, "replaced": replaced}


@app.post("/api/queries/delete")
def api_queries_delete(req: DeleteQueryRequest):
    name = _normalize_saved_query_name(req.name)
    if not name:
        raise HTTPException(status_code=400, detail="Query name is required.")

    items = _load_saved_queries()
    kept = [item for item in items if item.get("name") != name]

    if len(kept) == len(items):
        raise HTTPException(status_code=404, detail=f"Saved query not found: {name}")

    _save_saved_queries(kept)

    _audit_log({"event": "query_deleted", "status": "success", "name": name})

    return {"ok": True, "deleted": name}


@app.get("/api/profile")
def api_profile(dataset: str = Query(...), refresh: bool = False):
    """
    Return dataset context for the Inspect Dataset workflow.

    This same underlying context can also support the AI layer.
    """
    if dataset not in list_datasets():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset}")

    try:
        context = _load_dataset_context(dataset, refresh=refresh)
        _audit_log(
            {
                "event": "profile",
                "status": "success",
                "dataset": dataset,
                "refresh": refresh,
            }
        )
        return context
    except HTTPException:
        raise
    except Exception as e:
        _audit_log(
            {
                "event": "profile",
                "status": "error",
                "dataset": dataset,
                "refresh": refresh,
                "error": str(e),
            }
        )
        raise


@app.post("/api/sql")
def api_sql(req: SqlRequest):
    """
    Execute the SQL currently in the SQL workspace.

    IMPORTANT
    ---------
    This endpoint does not ask AI anything.
    It simply runs the SQL that the user has chosen to execute.

    That means the SQL may have come from:
    - manual typing
    - editing
    - AI generation
    - a saved query

    This design intentionally supports the current frontend:
        Generate SQL with AI
            ->
        user review/edit
            ->
        Run SQL

    EXECUTION RULE
    --------------
    The SQL is expected to reference the selected dataset
    logically, either as:

        FROM dataset

    or, for backward compatibility:

        FROM <selected_dataset_name>

    Before execution, this route rewrites that logical
    reference to read_parquet('...actual path...').
    """
    t0 = time.perf_counter()

    try:
        if req.dataset not in list_datasets():
            raise HTTPException(
                status_code=404,
                detail=f"Dataset not found: {req.dataset}",
            )

        # ----------------------------------------------------
        # STEP 1
        # Validate that the SQL is read-only.
        # ----------------------------------------------------
        validated_sql = _validate_readonly_sql(req.sql)

        # ----------------------------------------------------
        # STEP 2
        # Normalize the SQL before we wrap it. This strips any
        # trailing semicolon so wrapped preview queries do not
        # fail with parser errors.
        # ----------------------------------------------------
        cleaned_sql = _strip_trailing_semicolon(validated_sql)

        # ----------------------------------------------------
        # STEP 3
        # Resolve the selected dataset to its actual parquet
        # source path.
        # ----------------------------------------------------
        src, _is_glob = dataset_source_path(req.dataset)
        esc = _sql_escape_path(src)
        parquet_sql = f"read_parquet('{esc}')"

        # ----------------------------------------------------
        # STEP 4
        # Rewrite logical dataset references in FROM / JOIN
        # clauses to the actual parquet source.
        #
        # Supported inputs:
        #   FROM dataset
        #   FROM sample
        #   JOIN dataset
        #   JOIN sample
        #
        # where "sample" is the selected dataset id.
        # ----------------------------------------------------
        sql = _rewrite_sql_dataset_reference(
            sql=cleaned_sql,
            dataset_name=req.dataset,
            parquet_sql=parquet_sql,
        )

        # ----------------------------------------------------
        # STEP 5
        # Wrap the user SQL in an outer SELECT so we can safely
        # limit the preview result shown in the UI.
        # ----------------------------------------------------
        limited_sql = f"SELECT * FROM ({sql}) t LIMIT {MAX_PREVIEW_ROWS}"

        con = _connect()
        try:
            cur = con.execute(limited_sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            rowcount = int(
                con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0]
            )
        finally:
            con.close()

        preview = [dict(zip(cols, r)) for r in rows]
        elapsed = round(time.perf_counter() - t0, 4)

        _audit_log(
            {
                "event": "sql",
                "status": "success",
                "dataset": req.dataset,
                "rowcount": rowcount,
                "preview_rows_returned": len(preview),
                "elapsed_seconds": elapsed,
            }
        )

        return {
            "columns": cols,
            "rows": preview,
            "rowcount": rowcount,
            "elapsed_seconds": elapsed,
        }

    except HTTPException as e:
        _audit_log(
            {
                "event": "sql",
                "status": "error",
                "dataset": req.dataset,
                "error": str(e.detail),
            }
        )
        raise

    except duckdb.Error as e:
        # ----------------------------------------------------
        # IMPORTANT UX IMPROVEMENT
        # ----------------------------------------------------
        # DuckDB errors should come back as readable 400 errors
        # to the frontend instead of generic 500 failures.
        # ----------------------------------------------------
        logger.exception("sql failed | dataset=%s", req.dataset)
        _audit_log(
            {
                "event": "sql",
                "status": "error",
                "dataset": req.dataset,
                "error": str(e),
            }
        )
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.exception("sql failed | dataset=%s", req.dataset)
        _audit_log(
            {
                "event": "sql",
                "status": "error",
                "dataset": req.dataset,
                "error": str(e),
            }
        )
        raise

@app.post("/api/sql/export")
def api_sql_export(req: SqlExportRequest):
    """
    Export the FULL result of the SQL workspace query.

    IMPORTANT
    ---------
    This endpoint is intentionally different from /api/sql.

    /api/sql
        returns only a preview limited by MAX_PREVIEW_ROWS

    /api/sql/export
        exports the full validated query result, subject only
        to MAX_EXPORT_ROWS safety protection

    SUPPORTED FORMATS
    -----------------
    - xlsx
    - tsv

    IMPLEMENTATION NOTE
    -------------------
    We intentionally export through Python/pandas instead of
    DuckDB COPY FORMAT XLSX because XLSX support is not always
    available in every local DuckDB runtime.
    """
    t0 = time.perf_counter()

    try:
        import pandas as pd

        # ----------------------------------------------------
        # STEP 1
        # Confirm the selected dataset exists.
        # ----------------------------------------------------
        if req.dataset not in list_datasets():
            raise HTTPException(
                status_code=404,
                detail=f"Dataset not found: {req.dataset}",
            )

        # ----------------------------------------------------
        # STEP 2
        # Validate and normalize SQL.
        # ----------------------------------------------------
        validated_sql = _validate_readonly_sql(req.sql)
        cleaned_sql = _strip_trailing_semicolon(validated_sql)

        # ----------------------------------------------------
        # STEP 3
        # Resolve the dataset source and rewrite logical
        # dataset references to read_parquet(...).
        # ----------------------------------------------------
        src, _is_glob = dataset_source_path(req.dataset)
        esc = _sql_escape_path(src)
        parquet_sql = f"read_parquet('{esc}')"

        sql = _rewrite_sql_dataset_reference(
            sql=cleaned_sql,
            dataset_name=req.dataset,
            parquet_sql=parquet_sql,
        )

        # ----------------------------------------------------
        # STEP 4
        # Validate export format.
        # ----------------------------------------------------
        export_format = (req.format or "").strip().lower()
        if export_format not in {"xlsx", "tsv"}:
            raise HTTPException(
                status_code=400,
                detail="Export format must be 'xlsx' or 'tsv'.",
            )

        # ----------------------------------------------------
        # STEP 5
        # Run the FULL SQL and enforce export safety limits.
        # ----------------------------------------------------
        con = _connect()
        try:
            rowcount = int(
                con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0]
            )

            if rowcount > MAX_EXPORT_ROWS:
                raise HTTPException(
                    status_code=400,
                    detail=f"Export too large: {rowcount} rows (limit {MAX_EXPORT_ROWS})",
                )

            cur = con.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

        finally:
            con.close()

        # ----------------------------------------------------
        # STEP 6
        # Build DataFrame and write output file.
        # ----------------------------------------------------
        df = pd.DataFrame(rows, columns=cols)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_dataset = _safe_name(req.dataset)
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

        if export_format == "xlsx":
            out_path = EXPORTS_DIR / f"{safe_dataset}_sql_export_{ts}.xlsx"
            df.to_excel(out_path, index=False)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            out_path = EXPORTS_DIR / f"{safe_dataset}_sql_export_{ts}.tsv"
            df.to_csv(out_path, sep="\t", index=False)
            media_type = "text/tab-separated-values"

        elapsed = round(time.perf_counter() - t0, 4)

        _audit_log(
            {
                "event": "sql_export",
                "status": "success",
                "dataset": req.dataset,
                "format": export_format,
                "rowcount": rowcount,
                "exported_filename": out_path.name,
                "elapsed_seconds": elapsed,
            }
        )

        return FileResponse(
            path=str(out_path),
            filename=out_path.name,
            media_type=media_type,
        )

    except HTTPException as e:
        _audit_log(
            {
                "event": "sql_export",
                "status": "error",
                "dataset": req.dataset,
                "format": getattr(req, "format", None),
                "error": str(e.detail),
            }
        )
        raise

    except duckdb.Error as e:
        logger.exception("sql export failed | dataset=%s", req.dataset)
        _audit_log(
            {
                "event": "sql_export",
                "status": "error",
                "dataset": req.dataset,
                "format": getattr(req, "format", None),
                "error": str(e),
            }
        )
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        logger.exception("sql export failed | dataset=%s", req.dataset)
        _audit_log(
            {
                "event": "sql_export",
                "status": "error",
                "dataset": req.dataset,
                "format": getattr(req, "format", None),
                "error": str(e),
            }
        )
        raise HTTPException(status_code=500, detail=str(e)) from e

@app.get("/api/audit")
def api_audit(limit: int = Query(200, ge=1, le=5000)):
    """Return last N audit events (newest first)."""
    audit_path = DATASETS_DIR / "_audit.jsonl"
    if not audit_path.exists():
        return {"events": []}

    lines = audit_path.read_text(encoding="utf-8", errors="replace").splitlines()
    tail = lines[-limit:]

    events: list[dict[str, Any]] = []
    for line in reversed(tail):
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except Exception:
            continue

    return {"events": events}


@app.get("/api/schema")
def api_schema(dataset: str = Query(...)):
    """Return column names and DuckDB types for a dataset."""
    try:
        src, _is_glob = dataset_source_path(dataset)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    con = _connect()
    try:
        esc = _sql_escape_path(src)
        cur = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{esc}')")
        cols = cur.fetchall()
        columns = [{"name": r[0], "type": r[1]} for r in cols]
        _audit_log(
            {
                "event": "schema",
                "status": "success",
                "dataset": dataset,
                "column_count": len(columns),
            }
        )
        return {"dataset": dataset, "columns": columns}
    except Exception as e:
        _audit_log(
            {
                "event": "schema",
                "status": "error",
                "dataset": dataset,
                "error": str(e),
            }
        )
        raise
    finally:
        con.close()


@app.get("/api/preview")
def api_preview(dataset: str = Query(...), limit: int = Query(DEFAULT_PREVIEW_ROWS, ge=1)):
    """Return first N rows of a dataset."""
    used_limit = min(limit, MAX_PREVIEW_ROWS)

    try:
        src, _is_glob = dataset_source_path(dataset)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    con = _connect()
    t0 = time.perf_counter()
    try:
        esc = _sql_escape_path(src)
        sql = f"SELECT * FROM read_parquet('{esc}') LIMIT {int(used_limit)}"

        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        out_rows = [dict(zip(cols, r)) for r in rows]

        elapsed = round(time.perf_counter() - t0, 4)
        _audit_log(
            {
                "event": "preview",
                "status": "success",
                "dataset": dataset,
                "limit": used_limit,
                "rows_returned": len(out_rows),
                "elapsed_seconds": elapsed,
            }
        )

        return {
            "dataset": dataset,
            "limit": used_limit,
            "rows_returned": len(out_rows),
            "columns": cols,
            "rows": out_rows,
            "elapsed_seconds": elapsed,
        }

    except Exception as e:
        msg = str(e)
        if "No files found" in msg or "no files" in msg.lower():
            raise HTTPException(
                status_code=404,
                detail=f"No parquet files found for dataset: {dataset}",
            ) from e
        if "incompatible" in msg.lower() or "schema" in msg.lower():
            raise HTTPException(
                status_code=400,
                detail="Inconsistent schemas across parquet files in dataset",
            ) from e

        _audit_log(
            {
                "event": "preview",
                "status": "error",
                "dataset": dataset,
                "error": msg,
            }
        )
        raise
    finally:
        con.close()


@app.get("/api/dialog/folder")
def api_dialog_folder():
    """Open a native Windows folder picker."""
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        path = filedialog.askdirectory(title="Select folder to scan")
        root.destroy()
        return {"path": path or ""}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Folder dialog failed: {e}") from e


@app.get("/api/run")
def api_run(
    request: Request,
    dataset: str = Query(...),
    preset: str = Query(...),
    threshold: int | None = None,
):
    """
    Run a preset query.

    This is separate from the SQL workspace flow because
    presets are reusable pre-authored templates.
    """
    params = _extract_dynamic_params(
        request,
        reserved={"dataset", "preset"},
        threshold_fallback=threshold,
    )
    logger.info("query requested | dataset=%s preset=%s params=%s", dataset, preset, params)
    t0 = time.perf_counter()

    try:
        sql, _src = _sql_for(dataset, preset, params)

        con = _connect()
        try:
            limited_sql = f"SELECT * FROM ({sql}) t LIMIT {MAX_PREVIEW_ROWS}"
            cur = con.execute(limited_sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()

            rowcount = int(con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0])
            elapsed = time.perf_counter() - t0

            preview = [dict(zip(cols, r)) for r in rows]
            logger.info(
                "query success | dataset=%s preset=%s rowcount=%d preview=%d elapsed=%s",
                dataset,
                preset,
                rowcount,
                len(preview),
                round(elapsed, 4),
            )

            _audit_log(
                {
                    "event": "run",
                    "status": "success",
                    "dataset": dataset,
                    "preset": preset,
                    "params": params,
                    "rowcount": rowcount,
                    "preview_rows_returned": len(preview),
                    "elapsed_seconds": round(elapsed, 4),
                }
            )

            return {
                "columns": cols,
                "rows": preview,
                "rowcount": rowcount,
                "elapsed_seconds": round(elapsed, 4),
            }
        finally:
            con.close()

    except HTTPException as e:
        _audit_log(
            {
                "event": "run",
                "status": "error",
                "dataset": dataset,
                "preset": preset,
                "params": params,
                "error": str(e.detail),
            }
        )
        raise

    except Exception as e:
        logger.exception("query failed | dataset=%s preset=%s", dataset, preset)
        _audit_log(
            {
                "event": "run",
                "status": "error",
                "dataset": dataset,
                "preset": preset,
                "params": params,
                "error": str(e),
            }
        )
        raise


@app.get("/api/export")
def api_export(
    request: Request,
    dataset: str = Query(...),
    preset: str = Query(...),
    threshold: int | None = None,
):
    """
    Export a preset query result.

    Preferred format:
        XLSX

    Fallback:
        CSV
    """
    params = _extract_dynamic_params(
        request,
        reserved={"dataset", "preset"},
        threshold_fallback=threshold,
    )
    logger.info("export requested | dataset=%s preset=%s params=%s", dataset, preset, params)
    t0 = time.perf_counter()

    try:
        sql, _src = _sql_for(dataset, preset, params)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_preset = preset.replace("/", "_").replace("\\", "_")
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

        out_xlsx = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.xlsx"
        out_csv = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.csv"

        con = _connect()
        try:
            try:
                rowcount = int(con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0])
            except Exception:
                rowcount = None

            if rowcount is not None and rowcount > MAX_EXPORT_ROWS:
                raise HTTPException(
                    status_code=400,
                    detail=f"Export too large: {rowcount} rows (limit {MAX_EXPORT_ROWS})",
                )


            try:
                out_str = str(out_xlsx.resolve()).replace("\\", "/")
                con.execute(
                    f"COPY __export_view TO '{_sql_escape_path(out_str)}' "
                    f"(FORMAT XLSX, HEADER TRUE)"
                )
                elapsed = round(time.perf_counter() - t0, 4)

                _audit_log(
                    {
                        "event": "export",
                        "status": "success",
                        "dataset": dataset,
                        "preset": preset,
                        "params": params,
                        "exported_filename": out_xlsx.name,
                        "elapsed_seconds": elapsed,
                    }
                )

                return FileResponse(
                    path=str(out_xlsx),
                    filename=out_xlsx.name,
                    media_type=(
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ),
                )

            except Exception:
                out_str = str(out_csv.resolve()).replace("\\", "/")
                con.execute(
                    f"COPY __export_view TO '{_sql_escape_path(out_str)}' "
                    f"(FORMAT CSV, HEADER TRUE)"
                )
                elapsed = round(time.perf_counter() - t0, 4)

                _audit_log(
                    {
                        "event": "export",
                        "status": "success",
                        "dataset": dataset,
                        "preset": preset,
                        "params": params,
                        "exported_filename": out_csv.name,
                        "elapsed_seconds": elapsed,
                    }
                )

                return FileResponse(
                    path=str(out_csv),
                    filename=out_csv.name,
                    media_type="text/csv",
                )

        finally:
            con.close()

    except HTTPException as e:
        _audit_log(
            {
                "event": "export",
                "status": "error",
                "dataset": dataset,
                "preset": preset,
                "params": params,
                "error": str(e.detail),
            }
        )
        raise

    except Exception as e:
        logger.exception("export failed | dataset=%s preset=%s", dataset, preset)
        _audit_log(
            {
                "event": "export",
                "status": "error",
                "dataset": dataset,
                "preset": preset,
                "params": params,
                "error": str(e),
            }
        )
        raise


@app.post("/api/datasets/scan")
def scan_for_parquet(req: ScanRequest):
    """Scan a folder for parquet files and return basic file metadata."""
    t0 = time.perf_counter()
    logger.info("scan requested | path=%s recursive=%s", req.path, req.recursive)

    p = Path(req.path).expanduser()
    if not p.exists() or not p.is_dir():
        logger.warning("scan failed | path=%s | reason=not a directory", req.path)

        _audit_log(
            {
                "event": "scan",
                "status": "error",
                "path": req.path,
                "recursive": req.recursive,
                "error": "Path must be an existing directory.",
            }
        )

        return {"error": "Path must be an existing directory."}

    pattern = "**/*.parquet" if req.recursive else "*.parquet"
    files = [f for f in p.glob(pattern) if f.is_file()]

    con = _connect()
    results: list[dict[str, Any]] = []

    try:
        for f in files:
            stat = f.stat()
            f_str = str(f.resolve()).replace("\\", "/")
            row_count: int | None = None

            try:
                row_count = int(
                    con.execute(
                        f"SELECT COUNT(*) FROM read_parquet('{_sql_escape_path(f_str)}')"
                    ).fetchone()[0]
                )
            except Exception:
                row_count = None

            results.append(
                {
                    "path": str(f),
                    "name": f.name,
                    "size_bytes": stat.st_size,
                    "row_count": row_count,
                }
            )
    finally:
        con.close()

    elapsed = round(time.perf_counter() - t0, 4)
    logger.info("scan complete | path=%s count=%d elapsed=%s", req.path, len(results), elapsed)

    _audit_log(
        {
            "event": "scan",
            "status": "success",
            "path": req.path,
            "recursive": req.recursive,
            "file_count": len(results),
            "elapsed_seconds": elapsed,
        }
    )

    return {"count": len(results), "files": results}


@app.post("/api/datasets/register")
def register_dataset(req: RegisterRequest):
    """
    Register a dataset in either:
    - copy mode
    - reference mode

    After registration we also try to build dataset context
    immediately so that the app is ready for:

    - profile/inspection views
    - schema-aware AI prompting
    - suggested question chips in the frontend
    """
    t0 = time.perf_counter()
    logger.info(
        "register requested | dataset=%s parquet_path=%s mode=%s",
        req.dataset_name,
        req.parquet_path,
        req.mode,
    )

    src = Path(req.parquet_path).expanduser()
    if not src.exists() or not src.is_file():
        logger.warning(
            "register failed | dataset=%s | reason=parquet path not found",
            req.dataset_name,
        )

        _audit_log(
            {
                "event": "register",
                "status": "error",
                "dataset": req.dataset_name,
                "parquet_path": req.parquet_path,
                "mode": req.mode,
                "error": "Parquet path must be an existing file.",
            }
        )

        return {"error": "Parquet path must be an existing file."}

    requested_name = (req.dataset_name or "").strip()
    if not requested_name or requested_name.lower() == "newdataset":
        requested_name = src.stem

    ds_name = _safe_name(requested_name).replace("-", "_").replace(".", "_").lower()
    ds_dir = DATASETS_DIR / ds_name
    ds_dir.mkdir(parents=True, exist_ok=True)

    if req.mode == "copy":
        dest = ds_dir / src.name
        shutil.copy2(src, dest)
        storage = "copied"

    elif req.mode == "reference":
        pointer = ds_dir / "_reference.txt"
        pointer.write_text(str(src), encoding="utf-8")
        storage = "referenced"

    else:
        logger.warning(
            "register failed | dataset=%s | reason=invalid mode=%s",
            req.dataset_name,
            req.mode,
        )

        _audit_log(
            {
                "event": "register",
                "status": "error",
                "dataset": req.dataset_name,
                "parquet_path": req.parquet_path,
                "mode": req.mode,
                "error": "mode must be 'copy' or 'reference'.",
            }
        )

        return {"error": "mode must be 'copy' or 'reference'."}

    elapsed = round(time.perf_counter() - t0, 4)
    logger.info("register success | dataset=%s storage=%s elapsed=%s", ds_name, storage, elapsed)

    _audit_log(
        {
            "event": "register",
            "status": "success",
            "dataset": ds_name,
            "parquet_path": req.parquet_path,
            "mode": req.mode,
            "storage": storage,
            "elapsed_seconds": elapsed,
        }
    )

    try:
        _build_dataset_context(ds_name)
        context_built = True
    except Exception as e:
        logger.warning("dataset context build failed | dataset=%s | reason=%s", ds_name, e)
        context_built = False

    return {"dataset": ds_name, "storage": storage, "context_built": context_built}


@app.post("/api/shutdown")
def api_shutdown(bg: BackgroundTasks):
    """
    Hard-stop the process so the desktop app can be launched
    again immediately.

    This uses os._exit(0) because Windows + PyInstaller +
    no-console packaging can be unreliable with soft exits.
    """
    logger.info("shutdown requested")

    def _stop():
        time.sleep(0.25)
        os._exit(0)

    bg.add_task(_stop)
    return {"ok": True}


@app.get("/api/debug/env")
def api_debug_env():
    """Debug endpoint for confirming .env loading behavior."""
    exe_dir = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else None
    raw_key = os.environ.get("OPENAI_API_KEY")

    return {
        "frozen": bool(getattr(sys, "frozen", False)),
        "cwd": str(Path.cwd()),
        "exe_dir": str(exe_dir) if exe_dir else None,
        "base_dir": str(BASE_DIR),
        "openai_key_present": bool(raw_key),
        "openai_key_is_none": raw_key is None,
        "openai_key_length": len(raw_key) if raw_key is not None else None,
        "cwd_env_exists": (Path.cwd() / ".env").exists(),
        "exe_env_exists": (exe_dir / ".env").exists() if exe_dir else False,
        "base_env_exists": (BASE_DIR / ".env").exists(),
    }