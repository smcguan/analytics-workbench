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
from dataclasses import asdict

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
import stat
import time
from datetime import datetime, timezone
from typing import Any

import duckdb
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import Response
from fastapi import UploadFile, File, Form


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

from app.version import APP_VERSION


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
DATASETS_DIR = Path(os.getenv("AW_DATASETS_DIR", str(DATA_DIR / "datasets"))).resolve()
EXPORTS_DIR = Path(os.getenv("AW_EXPORTS_DIR", str(BASE_DIR / "exports"))).resolve()
QUERIES_PATH = Path(os.getenv("AW_QUERIES_PATH", str(DATA_DIR / "queries.json"))).resolve()
DATASET_CONTEXT_FILENAME = "dataset_context.json"

REFERENCES_DIR = Path(os.getenv("AW_REFERENCES_DIR", str(DATA_DIR / "references"))).resolve()
REFERENCE_LIBRARY_DIR = Path(os.getenv("AW_REFERENCE_LIBRARY_DIR", str(DATA_DIR / "reference_library"))).resolve()
SESSIONS_DIR = Path(os.getenv("AW_SESSIONS_DIR", str(DATA_DIR / "sessions"))).resolve()
EXAMPLE_CASES_DIR = Path(os.getenv("AW_EXAMPLE_CASES_DIR", str(DATA_DIR / "example_cases"))).resolve()

DATASETS_DIR.mkdir(parents=True, exist_ok=True)
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
REFERENCES_DIR.mkdir(parents=True, exist_ok=True)
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# PREVIEW / EXPORT SAFETY LIMITS
# ------------------------------------------------------------
# These caps protect the UI and local machine from returning
# excessively large result sets.
# ============================================================

DEFAULT_PREVIEW_ROWS = int(os.getenv("AW_DEFAULT_PREVIEW_ROWS", "50"))
MAX_PREVIEW_ROWS = int(os.getenv("AW_MAX_PREVIEW_ROWS", "200"))
MAX_EXPORT_ROWS = int(os.getenv("AW_MAX_EXPORT_ROWS", "200000"))

# Row sample size used when building dataset context and passport analysis.
# Keeps stats queries fast on large datasets (100M+ rows) without full scans.
_CONTEXT_SAMPLE_ROWS = int(os.getenv("AW_CONTEXT_SAMPLE_ROWS", "100000"))


# ============================================================
# STARTUP LOGGING
# ============================================================

from app.services.session_log import start_session, set_sessions_dir, log_event, end_session, export_session, get_current_session, session_summary, SessionEventType
set_sessions_dir(SESSIONS_DIR)
start_session()

logger.info(
    "app started | mode=%s | base_dir=%s | datasets_dir=%s | exports_dir=%s | references_dir=%s | reference_library_dir=%s | sessions_dir=%s | example_cases_dir=%s",
    "packaged" if getattr(sys, "frozen", False) else "dev",
    BASE_DIR,
    DATASETS_DIR,
    EXPORTS_DIR,
    REFERENCES_DIR,
    REFERENCE_LIBRARY_DIR,
    SESSIONS_DIR,
    EXAMPLE_CASES_DIR,
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


class SaveResultAsDatasetRequest(BaseModel):
    name: str
    dataset: str
    sql: str
    reference: str | None = None


class SqlRequest(BaseModel):
    dataset: str
    sql: str
    reference: str | None = None
    internal: bool = False  # When True, skip session log (e.g. insight card previews)

class SqlExportRequest(BaseModel):
    dataset: str
    sql: str
    format: str = "xlsx"
    reference: str | None = None


class ResultPassportRequest(BaseModel):
    columns: list[str]
    rows: list[dict]
    sql: str = ""
    total_rowcount: int | None = None  # full result count (not display-capped)


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
    - its folder contains source.parquet (canonical import format), or
    - its folder contains _meta.json (imported dataset metadata), or
    - it contains a valid _reference.txt pointer
    Directories with arbitrary .parquet files but no source.parquet/_meta.json
    are NOT treated as imported datasets — they may be raw data storage.
    """
    if not DATASETS_DIR.exists():
        return []

    out: list[str] = []

    for ds in DATASETS_DIR.iterdir():
        if not ds.is_dir():
            continue

        # Only recognise properly imported/registered datasets
        if (ds / "source.parquet").exists() or (ds / "_meta.json").exists() or (ds / "metadata.json").exists() or (ds / "dataset_context.json").exists():
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
        returns absolute parquet file path (resolved to system absolute)

    Copy mode:
        returns datasets/<dataset>/*.parquet glob (resolved to system absolute)

    IMPORTANT: both branches always return absolute paths with forward slashes.
    This is required because the AI validation layer (validate_sql_with_duckdb)
    creates its own DuckDB connection in a potentially different working directory.
    A relative path would cause "file not found" errors in that context even
    though the same path works fine from the main process working directory.
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
        # Resolve relative stored paths against ds_dir so they work regardless
        # of which directory the process is currently running from.
        if not p.is_absolute():
            p = (ds_dir / p).resolve()

        if not p.exists():
            raise FileNotFoundError(
                f"Reference dataset '{dataset}' points to missing file: {target}"
            )

        return (str(p.resolve()).replace("\\", "/"), False)

    # Copy mode: dataset lives as source.parquet (written by import pipeline)
    # or as *.parquet files. Check for source.parquet first (import pipeline
    # canonical name), fall back to glob for legacy/registered datasets.
    source_parquet = ds_dir / "source.parquet"
    if source_parquet.exists():
        return (str(source_parquet.resolve()).replace("\\", "/"), False)

    glob_path = str((ds_dir / "*.parquet").resolve()).replace("\\", "/")
    return (glob_path, True)


def _dataset_dir(dataset: str) -> Path:
    return (DATASETS_DIR / dataset).resolve()


def _rmtree_robust(path: Path) -> None:
    """
    Remove a directory tree, handling Windows-specific failure modes.

    Windows can set read-only flags on files (common with files extracted
    from archives) and briefly holds file locks after DuckDB closes a
    Parquet connection. A bare shutil.rmtree raises PermissionError in
    both cases, leaving the directory behind and causing subsequent imports
    to hit an "already exists" conflict.

    Strategy:
    1. onerror callback clears read-only flags and retries the failed op.
    2. Retry the entire rmtree up to 5 times with a short delay for
       PermissionError/OSError — covers the brief lock-release window
       after DuckDB closes a connection on Windows.
    3. After each rmtree call, verify the directory is actually gone.
       The onerror callback may silently skip locked files (per Python docs,
       if onerror doesn't raise, rmtree continues), so rmtree can return
       without error while files remain. Verification catches this.
    """
    def _on_error(func, failed_path, exc_info):
        # Clear read-only flag (common on Windows for extracted files) and retry.
        try:
            os.chmod(failed_path, stat.S_IWRITE)
            func(failed_path)
        except Exception:
            pass  # let the outer retry loop handle persistent failures

    max_attempts = 5
    last_err: Exception | None = None

    for attempt in range(max_attempts):
        try:
            shutil.rmtree(path, onerror=_on_error)

            # Verify the directory is actually gone.  onerror silently skips
            # locked files, so rmtree can return "successfully" while files
            # remain.  Without this check the delete endpoint returns ok=True
            # even though the directory still exists.
            if not path.exists():
                return

            # Directory still exists after rmtree returned without error —
            # some files were skipped by the onerror callback.
            last_err = PermissionError(
                f"rmtree completed but directory still exists "
                f"(locked files skipped): {path}"
            )
            logger.warning(
                "rmtree incomplete | attempt=%d/%d | path=%s",
                attempt + 1, max_attempts, path,
            )

        except (PermissionError, OSError) as exc:
            last_err = exc
            logger.warning(
                "rmtree failed | attempt=%d/%d | path=%s | error=%s",
                attempt + 1, max_attempts, path, exc,
            )

        if attempt < max_attempts - 1:
            time.sleep(0.3)

    if last_err:
        raise last_err


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

    # Check both _meta.json (legacy registered datasets) and metadata.json
    # (written by the import pipeline). Try both so imported datasets also
    # benefit from the fast cached path.
    for meta_filename in ("_meta.json", "metadata.json"):
        meta_path = ds_dir / meta_filename
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                row_count = meta.get("row_count")
                col_count = meta.get("column_count")
                # metadata.json uses "columns" list — derive count from it
                if col_count is None and isinstance(meta.get("columns"), list):
                    col_count = len(meta["columns"])
                return {
                    "name": dataset,
                    "row_count": row_count,
                    "column_count": col_count,
                    "file_size_bytes": meta.get("file_size_bytes"),
                    "dataset_type": meta.get("dataset_type"),
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
        # parquet_schema() works on globs and single files, but the column count
        # query using it counts schema *rows* not columns — use DESCRIBE instead
        # which works reliably on both single-file and glob paths.
        try:
            col_count = len(
                con.execute(f"DESCRIBE SELECT * FROM read_parquet('{esc}')").fetchall()
            )
        except Exception:
            col_count = None
    finally:
        con.close()

    # File size: sum all parquet files in the dataset directory regardless of
    # whether this is a glob or single-file dataset. This handles imported
    # copy-mode datasets (source.parquet) without requiring _reference.txt.
    try:
        file_size_bytes = sum(
            f.stat().st_size for f in ds_dir.glob("*.parquet") if f.is_file()
        )
        if file_size_bytes == 0:
            file_size_bytes = None
    except Exception:
        file_size_bytes = None

    return {
        "name": dataset,
        "row_count": row_count,
        "column_count": col_count,
        "file_size_bytes": file_size_bytes,
        "dataset_type": None,
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

    IMPORTANT: blocked-keyword scanning runs on a version of the SQL that has
    had single-quoted string literals replaced with empty strings first.  This
    prevents false positives when LIKE / IN values happen to contain a blocked
    word (e.g. WHERE col NOT LIKE '%update%' or NOT IN ('drop', 'Alteplase')).
    Without this step, queries with ~26 conditions in pharmaceutical datasets
    fail silently because one of the drug names or CMS values matches a keyword
    pattern inside the quoted string.
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

    # Strip single-quoted literals before keyword scanning so that values like
    # '%update%' or 'Alteplase' don't trigger a false positive.  The regex
    # replaces 'anything' (including SQL-escaped '' pairs) with ''.
    lowered_no_literals = re.sub(r"'[^']*'", "''", lowered)

    for token in blocked:
        if re.search(rf"\b{token}\b", lowered_no_literals):
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


def _resolve_reference_for_sql(
    explicit_reference: str | None,
) -> tuple[str | None, str | None]:
    """
    Resolve the reference table for SQL rewriting.

    If the caller provides an explicit reference name, use it.
    Otherwise, auto-detect a loaded reference table from REFERENCES_DIR.
    This handles the case where the frontend doesn't pass 'reference'
    (e.g. after app restart when currentReference is reset).

    Returns (reference_parquet_sql, reference_name) or (None, None).
    """
    ref_name = explicit_reference

    # Auto-detect if not explicitly provided
    if not ref_name and REFERENCES_DIR.exists():
        for d in REFERENCES_DIR.iterdir():
            if d.is_dir() and (d / "source.parquet").exists():
                ref_name = d.name
                break  # One reference at a time

    if not ref_name:
        return None, None

    ref_pq = (REFERENCES_DIR / ref_name / "source.parquet").resolve()
    if not ref_pq.exists():
        return None, None

    ref_esc = _sql_escape_path(str(ref_pq))
    return f"read_parquet('{ref_esc}')", ref_name


def _build_additional_references() -> dict[str, str]:
    """Build a dict of all registered reference tables → read_parquet(...)."""
    refs: dict[str, str] = {}
    if REFERENCES_DIR.exists():
        for d in REFERENCES_DIR.iterdir():
            if d.is_dir():
                pq = d / "source.parquet"
                if pq.exists():
                    refs[d.name] = f"read_parquet('{_sql_escape_path(str(pq.resolve()))}')"
    return refs


def _rewrite_sql_dataset_reference(
    sql: str,
    dataset_name: str,
    parquet_sql: str,
    reference_parquet_sql: str | None = None,
    reference_name: str | None = None,
    additional_datasets: dict[str, str] | None = None,
    additional_references: dict[str, str] | None = None,
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

    When a reference table is loaded, also rewrites:
        FROM reference  →  read_parquet('...reference path...')
        JOIN reference  →  read_parquet('...reference path...')

    When additional_datasets is provided (a dict of {name: parquet_sql}),
    any other registered dataset names found in FROM/JOIN clauses are also
    rewritten. This enables multi-dataset UNION/JOIN queries — e.g.:
        SELECT ... FROM dataset
        UNION ALL
        SELECT ... FROM fl_medicaid_claims

    IMPORTANT
    ---------
    We only rewrite dataset references in FROM/JOIN clauses.
    That avoids accidental replacements in column names,
    aliases, or free text.
    """
    rewritten = sql
    replaced_any = False

    identifiers_to_match = ["dataset", dataset_name]

    # SQL keywords that can follow a table name — must NOT be mistaken
    # for an alias when deciding whether to add AS <name>.
    _SQL_KW = (
        r'on|where|join|left|right|inner|outer|cross|full|natural|'
        r'group|order|limit|having|union|using|select|from|set|into|'
        r'intersect|except|window|qualify|fetch|offset|returning|when|'
        r'desc|asc|nulls|first|last|case|end|as|and|or|not|in|is|'
        r'between|like|exists|all|any|distinct|top|over|partition|by'
    )

    for ident in identifiers_to_match:
        if not ident:
            continue

        # Match:
        #   FROM dataset          — no alias
        #   FROM dataset d        — explicit alias (d)
        #   FROM dataset AS d     — explicit alias with AS keyword
        #   JOIN "dataset"        — quoted identifier
        #
        # The optional alias group captures an existing alias so we
        # can preserve it. If no alias exists, we add the original
        # table name as an alias so qualified column references
        # (e.g. dataset.col_name) survive the rewrite.
        pattern = re.compile(
            rf'(?i)\b(from|join)\s+(")?{re.escape(ident)}(")?\b'
            rf'(\s+as\s+\w+|\s+(?!{_SQL_KW}\b)\w+)?'
        )

        def _repl(match: re.Match[str], _ident=ident) -> str:
            nonlocal replaced_any
            replaced_any = True
            keyword = match.group(1)
            existing_alias = match.group(4)
            if existing_alias:
                alias = existing_alias.strip().split()[-1]
            else:
                alias = _ident
            return f"{keyword} {parquet_sql} AS {alias}"

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

    # Rewrite reference table if loaded and SQL uses it.
    # Match both the literal "reference" keyword AND the actual reference
    # table name (e.g. "ira_negotiated_drugs") — same backward-compat
    # pattern as the primary dataset.
    ref_identifiers = ["reference"]
    if reference_name and reference_name != "reference":
        ref_identifiers.append(reference_name)

    for ref_ident in ref_identifiers:
        ref_pattern = re.compile(
            rf'(?i)\b(from|join)\s+(")?{re.escape(ref_ident)}(")?\b'
            rf'(\s+as\s+\w+|\s+(?!{_SQL_KW}\b)\w+)?'
        )
        if ref_pattern.search(rewritten):
            if not reference_parquet_sql:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"SQL references '{ref_ident}' table, but no reference "
                        "table is loaded. Import a reference table first."
                    ),
                )

            def _ref_repl(m: re.Match[str], _ri=ref_ident) -> str:
                keyword = m.group(1)
                existing_alias = m.group(4)
                if existing_alias:
                    alias = existing_alias.strip().split()[-1]
                else:
                    alias = _ri
                return f"{keyword} {reference_parquet_sql} AS {alias}"

            rewritten = ref_pattern.sub(_ref_repl, rewritten)

    # Rewrite any other registered datasets referenced in the SQL.
    # This allows multi-dataset UNION/JOIN queries where the analyst
    # names another registered dataset directly (e.g. fl_medicaid_claims).
    # Each name is resolved to its own read_parquet(...) path exactly like
    # the primary dataset.
    if additional_datasets:
        for add_name, add_parquet_sql in additional_datasets.items():
            add_pattern = re.compile(
                rf'(?i)\b(from|join)\s+(")?{re.escape(add_name)}(")?\b'
                rf'(\s+as\s+\w+|\s+(?!{_SQL_KW}\b)\w+)?'
            )
            if add_pattern.search(rewritten):
                def _add_repl(m: re.Match[str], _n=add_name, _p=add_parquet_sql) -> str:
                    keyword = m.group(1)
                    existing_alias = m.group(4)
                    alias = existing_alias.strip().split()[-1] if existing_alias else _n
                    return f"{keyword} {_p} AS {alias}"
                rewritten = add_pattern.sub(_add_repl, rewritten)

    # Rewrite any other registered reference tables referenced in the SQL.
    # This allows multi-reference queries where the analyst names a
    # reference table directly (e.g. JOIN medicaid_schema_map) even when
    # a different reference is the "active" one.
    if additional_references:
        for add_ref_name, add_ref_pq_sql in additional_references.items():
            # Skip if already handled as the primary reference
            if add_ref_name == reference_name:
                continue
            add_ref_pattern = re.compile(
                rf'(?i)\b(from|join)\s+(")?{re.escape(add_ref_name)}(")?\b'
                rf'(\s+as\s+\w+|\s+(?!{_SQL_KW}\b)\w+)?'
            )
            if add_ref_pattern.search(rewritten):
                def _add_ref_repl(m: re.Match[str], _n=add_ref_name, _p=add_ref_pq_sql) -> str:
                    keyword = m.group(1)
                    existing_alias = m.group(4)
                    alias = existing_alias.strip().split()[-1] if existing_alias else _n
                    return f"{keyword} {_p} AS {alias}"
                rewritten = add_ref_pattern.sub(_add_ref_repl, rewritten)

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


def _sanitize_json_row(row: dict) -> dict:
    """Replace float inf/nan values with None so the row is JSON-serializable.

    DuckDB can return float('inf') for operations like integer division by zero,
    which FastAPI's json.dumps rejects with 'Out of range float values are not
    JSON compliant'. Replacing with None is the safest, lossless fallback.
    """
    import math
    result: dict = {}
    for k, v in row.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            result[k] = None
        else:
            result[k] = v
    return result


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
                        FROM (SELECT * FROM read_parquet('{esc}') USING SAMPLE {_CONTEXT_SAMPLE_ROWS} ROWS)
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
                        FROM (SELECT * FROM read_parquet('{esc}') USING SAMPLE {_CONTEXT_SAMPLE_ROWS} ROWS)
                        GROUP BY {quoted}
                        ORDER BY cnt DESC
                        LIMIT 5
                        """
                    ).fetchall()

                    null_count = con.execute(
                        f"""
                        SELECT SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END)
                        FROM (SELECT * FROM read_parquet('{esc}') USING SAMPLE {_CONTEXT_SAMPLE_ROWS} ROWS)
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
                        FROM (SELECT * FROM read_parquet('{esc}') USING SAMPLE {_CONTEXT_SAMPLE_ROWS} ROWS)
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
# PASSPORT HELPERS
# ------------------------------------------------------------
# These support the GET /api/datasets/{name}/passport endpoint.
# The passport is a single-download JSON document giving a
# complete picture of a dataset: schema, samples, distributions,
# numeric ranges, quality flags, grain description, and quick-
# start SQL.
# ============================================================

_PASSPORT_MEASURE_KEYWORDS = {
    "paid", "spend", "cost", "amount", "revenue", "count", "total",
    "payment", "price", "sales", "sum", "dollars", "value",
    "claims", "beneficiaries",
}

# Column name suffixes that indicate identifier/classifier columns even when
# a measure keyword also appears in the name (e.g. "payment_code" should not
# be treated as a numeric measure).
_EXCLUDE_MEASURE_SUFFIXES = {"_type", "_id", "_code", "_flag"}

# Preferred group-by columns for the quickstart aggregation query.
# Checked as case-insensitive substrings, in priority order.
_PASSPORT_GROUP_PRIORITY = ["hcpcs_code", "category", "type", "code"]


def _passport_read_identity(name: str, ds_dir: Path) -> dict[str, Any]:
    """Read dataset identity fields from cached metadata files."""
    row_count: int | None = None
    column_count: int | None = None
    original_type: str | None = None
    created_at: str | None = None

    # metadata.json has original_type and created_at (written by import pipeline)
    meta_path = ds_dir / "metadata.json"
    if meta_path.exists():
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            row_count = m.get("row_count")
            col_count_raw = m.get("column_count")
            if col_count_raw is None:
                cols = m.get("columns")
                col_count_raw = len(cols) if isinstance(cols, list) else None
            column_count = col_count_raw
            original_type = m.get("original_type")
            created_at = m.get("created_at")
        except Exception:
            pass

    # _meta.json fallback for row/col counts
    if row_count is None:
        try:
            m = json.loads((ds_dir / "_meta.json").read_text(encoding="utf-8"))
            row_count = m.get("row_count")
            if column_count is None:
                column_count = m.get("column_count")
        except Exception:
            pass

    try:
        file_size_bytes: int | None = sum(
            f.stat().st_size for f in ds_dir.glob("*.parquet") if f.is_file()
        ) or None
    except Exception:
        file_size_bytes = None

    return {
        "dataset_name": name,
        "row_count": row_count,
        "column_count": column_count,
        "source_file_type": original_type,
        "import_date": created_at,
        "file_size_bytes": file_size_bytes,
    }


def _passport_duckdb_analysis(
    con: duckdb.DuckDBPyConnection,
    esc: str,
    total_rows: int | None,
) -> dict[str, Any]:
    """
    Run DuckDB queries to build schema, sample values, distributions,
    numeric ranges, and quality flags for the passport.

    Uses USING SAMPLE for large-dataset safety — identical to the pattern
    in _build_dataset_context.  Each column is analysed individually so
    failures in one column do not block the rest.
    """
    SAMPLE = _CONTEXT_SAMPLE_ROWS

    # DESCRIBE returns: (col_name, col_type, null_str, key, default, extra)
    try:
        desc_rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{esc}')"
        ).fetchall()
    except duckdb.Error as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Could not read dataset schema: {exc}",
        ) from exc

    schema: list[dict[str, Any]] = []
    quality_flags: list[dict[str, Any]] = []

    for row in desc_rows:
        col_name = row[0]
        col_type = row[1]
        nullable_str = str(row[2]).upper() if row[2] is not None else "YES"
        nullable = nullable_str != "NO"
        kind = _classify_column_kind(col_type)
        # Double-quote column name for safe SQL embedding
        quoted = '"' + col_name.replace('"', '""') + '"'

        col_entry: dict[str, Any] = {
            "column_name": col_name,
            "data_type": col_type,
            "nullable": nullable,
            "null_count": 0,
            "null_pct": 0.0,
            "sample_values": [],
        }

        # --- Null count (sampled) ---
        null_count = 0
        sampled_total = SAMPLE
        try:
            result = con.execute(
                f"SELECT "
                f"  SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) AS nc, "
                f"  COUNT(*) AS tot "
                f"FROM (SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS)"
            ).fetchone()
            null_count = int(result[0] or 0)
            sampled_total = int(result[1] or SAMPLE)
        except Exception:
            pass

        null_pct = round(null_count / sampled_total * 100, 2) if sampled_total > 0 else 0.0
        col_entry["null_count"] = null_count
        col_entry["null_pct"] = null_pct

        # --- Sample values: 5 representative non-null values drawn randomly ---
        # Use an intermediate random sample so values come from across the full
        # dataset rather than the first rows of the first Parquet row group.
        try:
            samp = con.execute(
                f"SELECT {quoted} "
                f"FROM (SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS) "
                f"WHERE {quoted} IS NOT NULL LIMIT 5"
            ).fetchall()
            col_entry["sample_values"] = [str(r[0]) for r in samp if r[0] is not None]
        except Exception:
            pass

        # --- Distributions for string / categorical columns ---
        if kind in {"categorical", "boolean"}:
            dist: dict[str, Any] = {"top_values": [], "distinct_count": 0}

            try:
                top_vals = con.execute(
                    f"SELECT {quoted} AS value, COUNT(*) AS cnt "
                    f"FROM (SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS) "
                    f"GROUP BY {quoted} ORDER BY cnt DESC LIMIT 15"
                ).fetchall()
                dist["top_values"] = [
                    {"value": str(v) if v is not None else None, "count": int(c)}
                    for v, c in top_vals
                ]
            except Exception:
                pass

            try:
                dc = con.execute(
                    f"SELECT COUNT(DISTINCT {quoted}) "
                    f"FROM (SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS)"
                ).fetchone()[0]
                dist["distinct_count"] = int(dc or 0)
            except Exception:
                pass

            col_entry["distribution"] = dist

            # Quality flag: suspiciously low distinct count
            dc_val = dist["distinct_count"]
            effective_rows = min(total_rows, SAMPLE) if total_rows else sampled_total
            if dc_val > 0 and effective_rows > 100 and dc_val / effective_rows < 0.001:
                quality_flags.append({
                    "flag": "low_distinct_count",
                    "column": col_name,
                    "detail": f"{dc_val} distinct values in {effective_rows:,} sampled rows",
                })

            # Quality flag: rollup/subtotal rows
            # CMS and government datasets often contain aggregation rows
            # (e.g. Mftr_Name = 'Overall') mixed with detail rows. These
            # cause silent double-counting if not filtered out.
            _ROLLUP_TERMS = {
                "overall", "total", "all", "summary", "subtotal", "sub-total",
                "grand total", "aggregate", "combined", "all manufacturers",
                "all drugs", "all providers", "all states", "nationwide",
            }
            top_vals_list = dist.get("top_values", [])
            if len(top_vals_list) >= 2:
                top_val = top_vals_list[0]
                second_val = top_vals_list[1]
                top_str = (top_val.get("value") or "").strip().lower()
                top_count = top_val.get("count", 0)
                second_count = second_val.get("count", 0)
                # Flag if the top value is a rollup term AND appears
                # disproportionately often (>= 3x the second value)
                if (top_str in _ROLLUP_TERMS
                        and second_count > 0
                        and top_count >= 3 * second_count):
                    pct = round(top_count / sampled_total * 100, 1) if sampled_total > 0 else 0
                    quality_flags.append({
                        "flag": "possible_rollup_rows",
                        "column": col_name,
                        "detail": (
                            f"Value '{top_vals_list[0]['value']}' appears in "
                            f"{pct}% of rows — likely a rollup/subtotal row. "
                            f"Filter to a specific value before applying "
                            f"spending thresholds or aggregating."
                        ),
                    })

            # Quality flag: looks numeric but stored as text
            try:
                total_nn = int(con.execute(
                    f"SELECT COUNT(*) FROM "
                    f"(SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS) "
                    f"WHERE {quoted} IS NOT NULL"
                ).fetchone()[0] or 0)
                if total_nn > 0:
                    numeric_like = int(con.execute(
                        f"SELECT COUNT(*) FROM "
                        f"(SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS) "
                        f"WHERE TRY_CAST({quoted} AS DOUBLE) IS NOT NULL"
                    ).fetchone()[0] or 0)
                    if numeric_like / total_nn > 0.90:
                        quality_flags.append({
                            "flag": "looks_numeric_but_stored_as_text",
                            "column": col_name,
                            "detail": f"{int(numeric_like / total_nn * 100)}% of non-null values parse as numbers",
                        })
            except Exception:
                pass

            # Quality flag: trailing special characters (*, †, #, etc.)
            try:
                total_nn2 = int(con.execute(
                    f"SELECT COUNT(*) FROM "
                    f"(SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS) "
                    f"WHERE {quoted} IS NOT NULL"
                ).fetchone()[0] or 0)
                if total_nn2 > 0:
                    trailing_count = int(con.execute(
                        f"SELECT COUNT(*) FROM "
                        f"(SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS) "
                        f"WHERE {quoted} IS NOT NULL "
                        f"AND regexp_extract(CAST({quoted} AS VARCHAR), '[^A-Za-z0-9 ]$', 0) != ''"
                    ).fetchone()[0] or 0)
                    if trailing_count / total_nn2 > 0.05:
                        examples = [
                            str(r[0]) for r in con.execute(
                                f"SELECT {quoted} FROM "
                                f"(SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS) "
                                f"WHERE {quoted} IS NOT NULL "
                                f"AND regexp_extract(CAST({quoted} AS VARCHAR), '[^A-Za-z0-9 ]$', 0) != '' "
                                f"LIMIT 3"
                            ).fetchall()
                        ]
                        quality_flags.append({
                            "flag": "trailing_special_chars",
                            "column": col_name,
                            "detail": f"{int(trailing_count / total_nn2 * 100)}% of values have trailing special characters",
                            "example_values": examples,
                        })
            except Exception:
                pass

        # --- Numeric ranges ---
        elif kind == "numeric":
            num_range: dict[str, Any] = {}

            # MIN / MAX / AVG — all standard aggregates that ignore NULLs by
            # default, so no WHERE filter is needed inside the subquery.
            # Keeping the subquery pattern consistent with the null-count query
            # (which is known to work) avoids any DuckDB parsing edge-case with
            # USING SAMPLE + WHERE in a subquery.
            try:
                base_stats = con.execute(
                    f"SELECT MIN({quoted}), MAX({quoted}), AVG({quoted}) "
                    f"FROM (SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS)"
                ).fetchone()
            except Exception:
                base_stats = None

            if base_stats:
                min_val = _preview_value(base_stats[0])
                max_val = _preview_value(base_stats[1])
                mean_val = _preview_value(base_stats[2])

                # Median — separate query so a failure here doesn't lose min/max/mean
                median_val = None
                try:
                    med_row = con.execute(
                        f"SELECT APPROX_QUANTILE({quoted}, 0.5) "
                        f"FROM (SELECT {quoted} FROM read_parquet('{esc}') USING SAMPLE {SAMPLE} ROWS)"
                    ).fetchone()
                    if med_row:
                        median_val = _preview_value(med_row[0])
                except Exception:
                    pass

                has_negatives = False
                try:
                    has_negatives = min_val is not None and float(min_val) < 0
                except (TypeError, ValueError):
                    pass

                is_year = False
                try:
                    if min_val is not None and max_val is not None:
                        is_year = (
                            1900 <= float(min_val) <= 2100
                            and 1900 <= float(max_val) <= 2100
                        )
                except (TypeError, ValueError):
                    pass

                num_range = {
                    "min": min_val,
                    "max": max_val,
                    "mean": mean_val,
                    "median": median_val,
                    "has_negatives": has_negatives,
                    "is_year_column": is_year,
                }

                # Quality flag: extreme outlier (max > 100× median)
                # Only fires when median is positive — a positive max is always
                # > 100× a negative median, which is not a meaningful outlier.
                try:
                    if (
                        median_val is not None
                        and float(median_val) > 0
                        and max_val is not None
                        and float(max_val) > 100 * float(median_val)
                    ):
                        quality_flags.append({
                            "flag": "extreme_outlier",
                            "column": col_name,
                            "detail": (
                                f"max ({max_val}) is more than 100× "
                                f"the median ({median_val})"
                            ),
                        })
                except (TypeError, ValueError):
                    pass

            col_entry["numeric_range"] = num_range

        # Quality flag: high null rate (applies to all column kinds)
        if null_pct > 10.0:
            quality_flags.append({
                "flag": "high_null_rate",
                "column": col_name,
                "detail": f"{null_pct}% null values in sampled rows",
            })

        schema.append(col_entry)

    return {"schema": schema, "quality_flags": quality_flags}


def _detect_time_series_families(schema: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Detect groups of columns that share a base name with a year suffix.

    Example: Tot_Spndng_2019, Tot_Spndng_2020, Tot_Spndng_2021
    → {base_pattern: "Tot_Spndng", years: [2019, 2020, 2021], ...}

    Reports only families with 2+ year columns.
    """
    year_re = re.compile(r"^(.+)_(\d{4})$")
    families: dict[str, list[int]] = {}

    for col in schema:
        m = year_re.match(col["column_name"])
        if m:
            base = m.group(1)
            year = int(m.group(2))
            if 1900 <= year <= 2100:
                families.setdefault(base, []).append(year)

    result = []
    for base, years in families.items():
        if len(years) >= 2:
            sorted_years = sorted(years)
            result.append({
                "base_pattern": base,
                "years": sorted_years,
                "column_count": len(years),
                "example_columns": [f"{base}_{y}" for y in sorted_years[:3]],
            })
    return result


def _passport_sql_quickstart(schema: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Build ready-to-run SQL quick-start queries for the passport.

    Always includes a SELECT * query.

    If any column name contains a measure keyword (paid, total, claims, etc.),
    also includes an aggregate_by_top_category query that SUMs all measure
    columns grouped by the best categorical column.  Group-by column selection
    prefers HCPCS_CODE, category, type, or code columns before falling back to
    the first non-measure column in the schema.

    Measure detection is purely name-based so it works even when numeric_range
    stats could not be computed for a column.
    """
    col_names = [col["column_name"] for col in schema]
    cols_quoted = ['"' + c.replace('"', '""') + '"' for c in col_names]

    select_all = (
        "SELECT " + ", ".join(cols_quoted) + "\n"
        "FROM dataset\n"
        "LIMIT 100"
    )

    # Detect measure columns by keyword match in the column name.
    # Excludes columns whose name ends with an identifier/classifier suffix
    # (_type, _id, _code, _flag) even if a measure keyword also appears
    # (e.g. "payment_code" → excluded; "total_paid" → included).
    measure_cols: list[str] = [
        col["column_name"]
        for col in schema
        if any(kw in col["column_name"].lower() for kw in _PASSPORT_MEASURE_KEYWORDS)
        and not any(col["column_name"].lower().endswith(sfx) for sfx in _EXCLUDE_MEASURE_SUFFIXES)
    ]

    quickstart: dict[str, Any] = {"select_all": select_all}

    if measure_cols:
        measure_set = set(measure_cols)
        non_measure = [col["column_name"] for col in schema if col["column_name"] not in measure_set]

        # Find the best group-by column: check priority keywords first (case-insensitive
        # substring), then fall back to the first non-measure column.
        group_col: str | None = None
        non_measure_lower = {c.lower(): c for c in non_measure}
        for priority in _PASSPORT_GROUP_PRIORITY:
            for lc, orig in non_measure_lower.items():
                if priority in lc:
                    group_col = orig
                    break
            if group_col:
                break
        if group_col is None and non_measure:
            group_col = non_measure[0]

        if group_col:
            q_g = '"' + group_col.replace('"', '""') + '"'

            # Sort measure columns so paid/spend/cost/amount come first.
            # This ensures ORDER BY uses the most financially meaningful column.
            _PRIMARY_MEASURE_KW = {"paid", "spend", "cost", "amount"}
            def _measure_sort_key(name: str) -> int:
                lc = name.lower()
                return 0 if any(kw in lc for kw in _PRIMARY_MEASURE_KW) else 1
            ordered_measures = sorted(measure_cols, key=_measure_sort_key)

            sum_parts = []
            for mc in ordered_measures:
                q_m = '"' + mc.replace('"', '""') + '"'
                # Use the original column name as the alias — avoids redundant
                # "total_" prefix when the source column already starts with
                # "TOTAL_" (e.g. SUM("TOTAL_PAID") AS TOTAL_PAID, not
                # AS total_total_paid).  Quote the alias to handle any special
                # characters in the column name.
                alias = '"' + mc.replace('"', '""') + '"'
                sum_parts.append(f"  SUM({q_m}) AS {alias}")

            # ORDER BY the first (highest-priority) measure column
            order_alias = '"' + ordered_measures[0].replace('"', '""') + '"'
            quickstart["aggregate_by_top_category"] = (
                f"SELECT {q_g},\n"
                + ",\n".join(sum_parts) + "\n"
                f"FROM dataset\n"
                f"GROUP BY {q_g}\n"
                f"ORDER BY {order_alias} DESC\n"
                f"LIMIT 20"
            )
            quickstart["measure_columns"] = ordered_measures
            quickstart["group_column"] = group_col

    return quickstart


def _generate_grain_description(name: str, schema: list[dict[str, Any]]) -> str:
    """Call the AI provider to generate a 1-2 sentence grain description."""
    try:
        from app.ai.provider_openai import generate_grain_description_for_dataset
    except ImportError:
        try:
            from ai.provider_openai import generate_grain_description_for_dataset
        except ImportError:
            return ""

    try:
        return generate_grain_description_for_dataset(dataset_name=name, schema=schema)
    except Exception as exc:
        logger.warning("grain description AI call failed | dataset=%s | reason=%s", name, exc)
        return ""


def _passport_grain_description(
    name: str,
    ds_dir: Path,
    schema: list[dict[str, Any]],
) -> str:
    """
    Return a grain description, using the cached value when available.

    Caching follows the exact same pattern as suggest_questions:
    the result is stored in dataset_context.json under "grain_description"
    and returned instantly on subsequent calls without an OpenAI round-trip.
    """
    ctx_path = ds_dir / DATASET_CONTEXT_FILENAME

    # Cache hit
    try:
        cached = json.loads(ctx_path.read_text(encoding="utf-8"))
        grain = cached.get("grain_description")
        if grain and isinstance(grain, str) and grain.strip():
            logger.info("passport grain_description cache hit | dataset=%s", name)
            return grain.strip()
    except Exception:
        pass

    # Cache miss — generate with AI
    grain = _generate_grain_description(name, schema)

    # Persist result to cache, merging into the existing context file
    if grain:
        try:
            existing: dict[str, Any] = {}
            try:
                existing = json.loads(ctx_path.read_text(encoding="utf-8"))
            except Exception:
                pass
            existing["grain_description"] = grain
            existing["grain_description_generated_at"] = datetime.now().isoformat()
            ctx_path.write_text(
                json.dumps(existing, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning(
                "grain description cache write failed | dataset=%s | reason=%s", name, exc
            )

    return grain


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
        "version": APP_VERSION,
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
            "references_dir": {
                "ok": REFERENCES_DIR.exists(),
                "path": str(REFERENCES_DIR),
            },
            "reference_library_dir": {
                "ok": REFERENCE_LIBRARY_DIR.exists(),
                "path": str(REFERENCE_LIBRARY_DIR),
            },
            "sessions_dir": {
                "ok": SESSIONS_DIR.exists(),
                "path": str(SESSIONS_DIR),
            },
            "example_cases_dir": {
                "ok": EXAMPLE_CASES_DIR.exists(),
                "path": str(EXAMPLE_CASES_DIR),
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

    # Check both _meta.json (legacy registered) and metadata.json (import pipeline).
    # _meta.json is written by import pipeline now too — prefer it as it's lighter.
    meta = None
    for meta_filename in ("_meta.json", "metadata.json"):
        candidate = ds_dir / meta_filename
        if candidate.exists():
            try:
                meta = json.loads(candidate.read_text(encoding="utf-8"))
                break
            except Exception:
                continue

    if meta is not None:
        col_count = meta.get("column_count")
        # metadata.json stores columns as a list — derive count from it
        if col_count is None and isinstance(meta.get("columns"), list):
            col_count = len(meta["columns"])

        # file_size_bytes: recompute from actual parquet file rather than relying
        # on stored value (which may be None from _meta.json written at import time)
        try:
            file_size_bytes = sum(
                p.stat().st_size for p in ds_dir.glob("*.parquet") if p.is_file()
            ) or None
        except Exception:
            file_size_bytes = meta.get("file_size_bytes")

        out = {
            "dataset": name,
            "mode": mode,
            "source_path": src,
            "is_glob": is_glob,
            "row_count": meta.get("row_count"),
            "column_count": col_count,
            "file_size_bytes": file_size_bytes,
            "last_scanned": meta.get("last_scanned"),
            "meta_source": "cached",
            "ai_consent": meta.get("ai_consent", True),
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

        # Use DESCRIBE instead of parquet_schema() for column count —
        # parquet_schema() returns schema *rows* not columns and behaves
        # differently on single files vs globs.
        try:
            col_count = len(
                con.execute(f"DESCRIBE SELECT * FROM read_parquet('{esc}')").fetchall()
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
            file_size_bytes = sum(
                p.stat().st_size for p in ds_dir.glob("*.parquet") if p.is_file()
            ) or None
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




class AiConsentRequest(BaseModel):
    ai_consent: bool = True


@app.post("/api/datasets/{name}/ai_consent")
def set_ai_consent(name: str, req: AiConsentRequest):
    """
    Store per-dataset AI consent decision in _meta.json.

    Called once after import. The frontend checks this flag before
    triggering insights or suggestions for the dataset.
    """
    ds_dir = _dataset_dir(name)
    if not ds_dir.exists():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {name}")

    meta_path = ds_dir / "_meta.json"
    meta: dict = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    meta["ai_consent"] = req.ai_consent

    try:
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to save consent: {exc}") from exc

    return {"dataset": name, "ai_consent": req.ai_consent}


@app.get("/api/datasets/{name}/passport")
def api_dataset_passport(name: str):
    """
    Generate and return an Export Passport JSON for a dataset.

    The passport is a structured document containing:
    - Dataset identity (name, row count, columns, file size, import date)
    - Full schema with nullable, null rates, and sample values per column
    - Distributions (top 15 values + distinct count) for string columns
    - Numeric ranges (min/max/mean/median) for numeric columns
    - AI-generated grain description (1-2 sentences, cached in dataset_context.json)
    - Automatic data quality flags (high nulls, trailing chars, looks-numeric, low distinct)
    - Time-series column family detection (year-suffix column groups)
    - Ready-to-run SQL quick-start queries

    The grain description is cached after the first call — subsequent calls return instantly
    without an OpenAI round-trip, following the same pattern as suggest_questions.
    """
    ds_dir = _dataset_dir(name)
    if not ds_dir.exists() or not ds_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {name}")

    try:
        src, _is_glob = dataset_source_path(name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    esc = _sql_escape_path(src)

    # Identity from cached metadata files (no DuckDB required)
    identity = _passport_read_identity(name, ds_dir)

    # DuckDB analysis: schema, distributions, numeric ranges, quality flags
    con = _connect()
    try:
        analysis = _passport_duckdb_analysis(con, esc, identity.get("row_count"))
    except HTTPException:
        raise
    except duckdb.Error as exc:
        logger.exception("passport duckdb analysis failed | dataset=%s", name)
        raise HTTPException(
            status_code=400,
            detail=f"Dataset analysis failed: {exc}",
        ) from exc
    except Exception as exc:
        logger.exception("passport analysis failed | dataset=%s", name)
        raise HTTPException(
            status_code=400,
            detail=f"Dataset analysis failed: {exc}",
        ) from exc
    finally:
        con.close()

    schema = analysis["schema"]

    # Grain description (AI-generated, cached in dataset_context.json)
    grain_description = _passport_grain_description(name, ds_dir, schema)

    # Time-series column families (year-suffix pattern detection)
    time_series_families = _detect_time_series_families(schema)

    # Ready-to-run SQL quick-start queries
    sql_quickstart = _passport_sql_quickstart(schema)

    logger.info(
        "passport generated | dataset=%s | columns=%d | quality_flags=%d",
        name, len(schema), len(analysis["quality_flags"]),
    )
    _audit_log({"event": "passport", "status": "success", "dataset": name})

    try:
        log_event(SessionEventType.PASSPORT_EXPORT, {"dataset": name})
    except Exception:
        logger.warning("Failed to log session event", exc_info=True)

    return {
        "identity": identity,
        "schema": schema,
        "grain_description": grain_description,
        "data_quality_flags": analysis["quality_flags"],
        "time_series_column_families": time_series_families,
        "sql_quickstart": sql_quickstart,
    }


@app.post("/api/datasets/{name}/delete")
def api_dataset_delete(name: str):
    """
    Deregister and remove a dataset from the application.

    This deletes the dataset directory and all its contents
    (the canonical Parquet file, metadata, and context cache).

    The frontend Refresh Datasets button calls this endpoint for
    each visible dataset so that re-importing the same name does
    not conflict with a stale backend registration.

    Returns 404 if the dataset does not exist.
    Returns 200 with ok=True on success.
    """
    ds_dir = _dataset_dir(name)

    if not ds_dir.exists() or not ds_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {name}")

    try:
        _rmtree_robust(ds_dir)
        logger.info("dataset deleted | dataset=%s | path=%s", name, ds_dir)
        _audit_log({"event": "dataset_delete", "status": "success", "dataset": name})
        try:
            log_event(SessionEventType.DATASET_DELETE, {"dataset": name})
        except Exception:
            logger.warning("Failed to log session event", exc_info=True)
        return {"ok": True, "deleted": name}
    except Exception as e:
        logger.exception("dataset delete failed | dataset=%s", name)
        _audit_log({"event": "dataset_delete", "status": "error", "dataset": name, "error": str(e)})
        raise HTTPException(status_code=500, detail=f"Failed to delete dataset: {e}") from e


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

    try:
        log_event(SessionEventType.QUERY_SAVE, {"name": name, "dataset": req.dataset})
    except Exception:
        logger.warning("Failed to log session event", exc_info=True)

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
        # Raise as 400 with the actual error text so the frontend can
        # display it in the toast instead of a generic "Internal Server Error".
        raise HTTPException(
            status_code=400,
            detail=f"Profile failed for dataset '{dataset}': {e}",
        ) from e


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
        # Also rewrites "reference" if a reference table is loaded.
        # Also rewrites any other registered dataset names so that
        # multi-dataset UNION/JOIN queries work (e.g. for multi-state
        # Medicaid normalization where TX/FL/OH are each a dataset).
        # ----------------------------------------------------
        reference_parquet_sql, reference_name = _resolve_reference_for_sql(
            req.reference
        )

        additional_datasets: dict[str, str] = {}
        for other_name in list_datasets():
            if other_name == req.dataset:
                continue
            try:
                other_src, _ = dataset_source_path(other_name)
                additional_datasets[other_name] = f"read_parquet('{_sql_escape_path(other_src)}')"
            except Exception:
                pass

        additional_references = _build_additional_references()

        sql = _rewrite_sql_dataset_reference(
            sql=cleaned_sql,
            dataset_name=req.dataset,
            parquet_sql=parquet_sql,
            reference_parquet_sql=reference_parquet_sql,
            reference_name=reference_name,
            additional_datasets=additional_datasets,
            additional_references=additional_references,
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

        preview = [_sanitize_json_row(dict(zip(cols, r))) for r in rows]
        elapsed = round(time.perf_counter() - t0, 4)

        # ----------------------------------------------------
        # STEP 6
        # Recommend a chart based on result shape.
        # Deterministic rules — no AI involved.
        # Returns {"recommended": False} if no chart suits the
        # result, so the frontend can always read this field.
        # ----------------------------------------------------
        try:
            visualization = recommend_chart(cols, preview)
        except Exception as _chart_err:
            logger.warning("chart recommendation failed | reason=%s", _chart_err)
            visualization = {"recommended": False, "reason": str(_chart_err)}

        _audit_log(
            {
                "event": "sql",
                "status": "success",
                "dataset": req.dataset,
                "rowcount": rowcount,
                "preview_rows_returned": len(preview),
                "elapsed_seconds": elapsed,
                "chart_recommended": visualization.get("recommended", False),
            }
        )

        response = {
            "columns": cols,
            "rows": preview,
            "rowcount": rowcount,
            "elapsed_seconds": elapsed,
            "visualization": visualization,
        }

        # Reference table diagnostic: when a reference table was used,
        # count its rows so the analyst can verify the JOIN effect.
        if req.reference and reference_parquet_sql:
            try:
                ref_con = _connect()
                try:
                    ref_count = int(ref_con.execute(
                        f"SELECT COUNT(*) FROM {reference_parquet_sql}"
                    ).fetchone()[0])
                finally:
                    ref_con.close()
                response["reference_info"] = {
                    "name": req.reference,
                    "ref_rows": ref_count,
                    "result_rows": rowcount,
                }
            except Exception:
                response["reference_info"] = {
                    "name": req.reference,
                    "ref_rows": None,
                    "result_rows": rowcount,
                }

        # Skip session logging for internal/preview queries (e.g. insight card
        # mini-previews) so they don't pollute the Session Log with phantom
        # query_run events.  Bug #13: insight card previews were logging one
        # QUERY_RUN per card, inflating the count (e.g. 9 events for 1 user query).
        if not req.internal:
            try:
                log_event(SessionEventType.QUERY_RUN, {
                    "dataset": req.dataset,
                    "sql": req.sql,
                    "row_count": rowcount,
                    "elapsed_seconds": elapsed,
                })
            except Exception:
                logger.warning("Failed to log session event", exc_info=True)

        return response

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
        # Surface the real error message instead of letting FastAPI return the
        # generic "Internal Server Error" 500 body.  duckdb.Error is caught
        # above; this catches any remaining Python-level exception (e.g. a
        # DuckDB wrapper inconsistency) and still delivers it as a 400 so
        # the frontend toast shows the actual problem.
        raise HTTPException(status_code=400, detail=str(e)) from e

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

        reference_parquet_sql, reference_name = _resolve_reference_for_sql(
            req.reference
        )

        additional_datasets_export: dict[str, str] = {}
        for other_name in list_datasets():
            if other_name == req.dataset:
                continue
            try:
                other_src, _ = dataset_source_path(other_name)
                additional_datasets_export[other_name] = f"read_parquet('{_sql_escape_path(other_src)}')"
            except Exception:
                pass

        additional_references_export = _build_additional_references()

        sql = _rewrite_sql_dataset_reference(
            sql=cleaned_sql,
            dataset_name=req.dataset,
            parquet_sql=parquet_sql,
            reference_parquet_sql=reference_parquet_sql,
            reference_name=reference_name,
            additional_datasets=additional_datasets_export,
            additional_references=additional_references_export,
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

        try:
            log_event(SessionEventType.EXPORT, {
                "dataset": req.dataset,
                "format": export_format,
                "row_count": rowcount,
            })
        except Exception:
            logger.warning("Failed to log session event", exc_info=True)

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

from app.services.dataset_import import (
    import_dataset,
    import_reference_table,
    DatasetImportError,
)

try:
    from app.services.chart_recommender import recommend_chart
except Exception:
    from services.chart_recommender import recommend_chart


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
        msg = str(e)
        _audit_log(
            {
                "event": "schema",
                "status": "error",
                "dataset": dataset,
                "error": msg,
            }
        )
        if "No files found" in msg or "no files" in msg.lower():
            raise HTTPException(
                status_code=404,
                detail=f"No parquet files found for dataset: {dataset}",
            ) from e
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

@app.post("/api/datasets/import")
async def import_uploaded_dataset(
    file: UploadFile = File(...),
    dataset_name: str | None = Form(None),
    overwrite: bool = Query(False),
    strip_trailing_chars: bool = Form(False),
):
    """
    Import an uploaded dataset and normalize it into the app's canonical
    dataset storage structure.

    overwrite=true: if the dataset directory already exists, remove it first
    before importing. This supports the Refresh → re-import workflow where
    the backend delete may have raced or the user explicitly wants to replace.
    """

    clean_dataset_name = dataset_name
    if clean_dataset_name is not None:
        clean_dataset_name = clean_dataset_name.strip()
        if not clean_dataset_name or clean_dataset_name.lower() == "string":
            clean_dataset_name = None

    try:
        result = import_dataset(
            uploaded_file=file.file,
            original_filename=file.filename,
            display_name=clean_dataset_name,
            registered_root=DATASETS_DIR,
            overwrite=overwrite,
            strip_trailing_special_chars=strip_trailing_chars,
        )
    except DatasetImportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Dataset import failed: {exc}",
        ) from exc

    metadata = result.metadata

    try:
        log_event(SessionEventType.DATASET_IMPORT, {
            "dataset": metadata.registered_name,
            "row_count": metadata.row_count,
            "column_count": metadata.column_count,
            "source_filename": metadata.original_filename,
        })
    except Exception:
        logger.warning("Failed to log session event", exc_info=True)

    return {
        "dataset": metadata.registered_name,
        "dataset_id": metadata.dataset_id,
        "display_name": metadata.display_name,
        "original_filename": metadata.original_filename,
        "original_type": metadata.original_type,
        "parquet_path": metadata.parquet_path,
        "row_count": metadata.row_count,
        "column_count": metadata.column_count,
        "columns": [asdict(col) for col in metadata.columns],
        "created_at": metadata.created_at,
    }


# ============================================================
# RESULT PASSPORT ENDPOINT
# ============================================================


@app.post("/api/results/passport")
def result_passport(req: ResultPassportRequest):
    """
    Generate a structured summary of a query result set.

    This is the query-result equivalent of Export Passport. It produces
    a per-column profile (top values, numeric stats, null rates, data
    quality flags) from the rows passed in the request body — no raw
    row data is included in the output.

    Designed to be copied to clipboard and shared with an external AI
    assistant instead of exporting raw rows.
    """
    import statistics

    columns = req.columns
    rows = req.rows
    sql = req.sql
    # Use the full result count if provided, not the display-capped row count.
    # AW caps display at 200 rows but the full query may return thousands.
    sampled_rows = len(rows)
    row_count = req.total_rowcount if req.total_rowcount is not None else sampled_rows

    if not columns or not rows:
        raise HTTPException(status_code=400, detail="No result data to profile.")

    per_column: list[dict] = []

    for col in columns:
        values = [r.get(col) for r in rows]
        non_null = [v for v in values if v is not None and v != ""]
        null_count = row_count - len(non_null)
        null_pct = round(null_count / row_count * 100, 1) if row_count > 0 else 0

        profile: dict = {
            "column": col,
            "null_count": null_count,
            "null_pct": null_pct,
        }

        # Try to detect numeric values
        numeric_vals: list[float] = []
        for v in non_null:
            try:
                numeric_vals.append(float(v))
            except (ValueError, TypeError):
                break
        else:
            # All non-null values are numeric
            if numeric_vals:
                profile["type"] = "numeric"
                profile["min"] = min(numeric_vals)
                profile["max"] = max(numeric_vals)
                profile["mean"] = round(statistics.mean(numeric_vals), 4)
                profile["median"] = round(statistics.median(numeric_vals), 4)
                per_column.append(profile)
                continue

        # String column: distinct count and top values
        profile["type"] = "string"
        str_vals = [str(v) for v in non_null]
        profile["distinct_count"] = len(set(str_vals))

        # Top values with counts (up to 15)
        from collections import Counter
        counts = Counter(str_vals).most_common(15)
        profile["top_values"] = [{"value": v, "count": c} for v, c in counts]

        per_column.append(profile)

    # Data quality flags
    quality_flags: list[dict] = []
    for p in per_column:
        if p["null_pct"] > 10:
            quality_flags.append({
                "column": p["column"],
                "flag": "high_null_rate",
                "detail": f"{p['null_pct']}% null",
            })
        # Detect looks-numeric-but-typed-as-string
        if p.get("type") == "string" and p.get("distinct_count", 0) > 5:
            vals = [tv["value"] for tv in p.get("top_values", [])]
            try:
                [float(v) for v in vals[:5] if v]
                quality_flags.append({
                    "column": p["column"],
                    "flag": "looks_numeric_but_stored_as_text",
                    "detail": "Top values parse as numbers",
                })
            except (ValueError, TypeError):
                pass

    result = {
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "per_column_profile": per_column,
        "data_quality_flags": quality_flags,
        "grain_hint": sql,
    }

    # Note when stats are computed from a display-capped sample
    if row_count > sampled_rows:
        result["note"] = (
            f"Statistics computed from {sampled_rows} displayed rows "
            f"out of {row_count} total. Top values and distributions "
            f"reflect the displayed sample, not the full result set."
        )

    try:
        log_event(SessionEventType.RESULT_PASSPORT, {
            "row_count": row_count,
            "column_count": len(columns),
        })
    except Exception:
        logger.warning("Failed to log session event", exc_info=True)

    return result


# ============================================================
# REFERENCE TABLE ENDPOINTS
# ============================================================


@app.post("/api/references/import")
async def import_reference_endpoint(
    file: UploadFile = File(...),
    reference_name: str | None = Form(None),
):
    """
    Import a small reference/lookup table for JOIN operations.

    Reference tables are lightweight — no profiling, no insights.
    One reference table at a time; importing a new one with the same
    name overwrites the previous one.
    """
    clean_name = reference_name
    if clean_name is not None:
        clean_name = clean_name.strip()
        if not clean_name or clean_name.lower() == "string":
            clean_name = None

    try:
        result = import_reference_table(
            uploaded_file=file.file,
            original_filename=file.filename,
            display_name=clean_name,
            registered_root=REFERENCES_DIR,
            overwrite=True,
        )
    except DatasetImportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Reference table import failed: {exc}",
        ) from exc

    try:
        log_event(SessionEventType.REFERENCE_LOAD, {
            "reference_name": result.reference_name,
            "row_count": result.row_count,
            "columns": len(result.columns),
        })
    except Exception:
        logger.warning("Failed to log session event", exc_info=True)

    return {
        "reference": result.reference_name,
        "parquet_path": result.parquet_path,
        "row_count": result.row_count,
        "column_count": len(result.columns),
        "columns": [{"name": c.name, "type": c.type} for c in result.columns],
    }


@app.get("/api/references")
def list_reference_tables():
    """List all imported reference tables."""
    refs = []
    if REFERENCES_DIR.exists():
        for d in REFERENCES_DIR.iterdir():
            if d.is_dir() and (d / "source.parquet").exists():
                meta_path = d / "_meta.json"
                meta: dict = {}
                if meta_path.exists():
                    try:
                        meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    except Exception:
                        pass
                refs.append({"name": d.name, **meta})
    return {"references": refs}


@app.post("/api/references/{name}/delete")
def delete_reference_table(name: str):
    """Delete a reference table."""
    ref_dir = (REFERENCES_DIR / name).resolve()
    if ref_dir.exists():
        _rmtree_robust(ref_dir)
    try:
        log_event(SessionEventType.REFERENCE_DELETE, {"reference_name": name})
    except Exception:
        logger.warning("Failed to log session event", exc_info=True)
    return {"deleted": name}


# ============================================================
# REFERENCE LIBRARY ENDPOINTS
# ============================================================


@app.get("/api/reference_library")
def list_reference_library():
    """
    List reference CSV files available in the library.

    Merges _library.json manifest entries with auto-discovered CSVs.
    Any CSV in the library directory that is not in the manifest is
    auto-registered by reading its headers and counting rows.  This
    lets users drop a CSV into the folder and have it appear in the
    UI without editing _library.json.
    """
    if not REFERENCE_LIBRARY_DIR.exists():
        return {"library": []}

    # Load manifest entries (keyed by filename for merge)
    manifest_path = REFERENCE_LIBRARY_DIR / "_library.json"
    manifest_items: list[dict] = []
    manifest_filenames: set[str] = set()
    if manifest_path.exists():
        try:
            manifest_items = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest_filenames = {item["filename"] for item in manifest_items}
        except Exception as exc:
            logger.warning("Failed to read reference library manifest: %s", exc)

    # Auto-discover CSVs not in manifest
    for csv_path in sorted(REFERENCE_LIBRARY_DIR.glob("*.csv")):
        if csv_path.name in manifest_filenames:
            continue
        try:
            import csv as csv_mod
            with open(csv_path, encoding="utf-8", newline="") as f:
                reader = csv_mod.reader(f)
                headers = next(reader, [])
                row_count = sum(1 for _ in reader)
            display_name = csv_path.stem.replace("_", " ").replace("-", " ").title()
            manifest_items.append({
                "filename": csv_path.name,
                "name": display_name,
                "description": f"Auto-discovered reference table ({row_count} rows)",
                "columns": headers,
                "row_count": row_count,
                "version": "auto",
                "join_hint": "",
            })
        except Exception as exc:
            logger.warning("Failed to auto-discover library CSV %s: %s", csv_path.name, exc)

    return {"library": manifest_items}


@app.post("/api/reference_library/{filename}/load")
def load_library_reference(filename: str):
    """
    Load a pre-built library CSV as the active reference table.

    Copies the library file through the reference table import pipeline
    so it becomes available as 'reference' in SQL queries.
    """
    csv_path = REFERENCE_LIBRARY_DIR / filename
    if not csv_path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Library file not found: {filename}",
        )

    try:
        with csv_path.open("rb") as f:
            result = import_reference_table(
                uploaded_file=f,
                original_filename=filename,
                registered_root=REFERENCES_DIR,
                overwrite=True,
            )
    except DatasetImportError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        log_event(SessionEventType.REFERENCE_LOAD, {
            "reference_name": result.reference_name,
            "row_count": result.row_count,
            "source": filename,
        })
    except Exception:
        logger.warning("Failed to log session event", exc_info=True)

    return {
        "reference": result.reference_name,
        "parquet_path": result.parquet_path,
        "row_count": result.row_count,
        "column_count": len(result.columns),
        "columns": [{"name": c.name, "type": c.type} for c in result.columns],
    }


class SqlGenerateRequest(BaseModel):
    """Request body for /api/sql/generate (schema-aware SQL starter)."""
    dataset: str
    question: str | None = None
    prompt: str | None = None
    file_type: str | None = None
    dataset_type: str | None = None


@app.post("/api/sql/generate")
def api_sql_generate(req: SqlGenerateRequest):
    """
    Schema-aware SQL starter endpoint.

    This is the fallback for the frontend's Generate SQL chain when the
    AI routes (/api/ai/generate_sql) are unavailable or fail.

    Unlike a hard-coded SELECT * FROM dataset LIMIT 100, this endpoint
    reads the actual dataset schema and returns SQL that references real
    column names, giving the user a more useful starting point.

    It does NOT call the AI — it generates deterministic starter SQL.
    The AI layer is in /api/ai/generate_sql (routes.py).
    """
    dataset = (req.dataset or "").strip()

    if not dataset:
        raise HTTPException(status_code=400, detail="dataset is required.")

    if dataset not in list_datasets():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {dataset}")

    try:
        src, _is_glob = dataset_source_path(dataset)
        esc = _sql_escape_path(src)

        con = _connect()
        try:
            cur = con.execute(f"DESCRIBE SELECT * FROM read_parquet(\'{esc}\')")
            cols = [row[0] for row in cur.fetchall()]
        finally:
            con.close()

        # This endpoint is a non-AI fallback — it cannot interpret the question.
        # Return SELECT * with the dataset's actual column count noted in the
        # message so the user knows what they're working with and why.
        # The message makes clear the AI route was unavailable so they understand
        # the SQL is a starter template, not an answer to their question.

        import re as _re
        question_text = (req.question or req.prompt or "").lower()
        limit = 100
        m = _re.search(r'\b(\d+)\s*(rows?|records?|results?|entries|lines)?\b', question_text)
        if m:
            candidate = int(m.group(1))
            if 1 <= candidate <= 100000:
                limit = candidate

        col_summary = f"{len(cols)} columns" if cols else "unknown columns"
        sql = f"SELECT *\nFROM dataset\nLIMIT {limit}"

        return {
            "sql": sql,
            "dataset": dataset,
            "source": "schema_starter",
            "message": (
                f"AI SQL generation was unavailable — loaded a starter query instead "
                f"(dataset has {col_summary}: {', '.join(cols[:8])}{'...' if len(cols) > 8 else ''}). "
                f"Edit the SQL above to answer your question."
            ),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.warning("sql/generate fallback failed | dataset=%s | reason=%s", dataset, e)
        # Still return something usable rather than 500
        return {
            "sql": "SELECT *\nFROM dataset\nLIMIT 100",
            "dataset": dataset,
            "source": "generic_starter",
            "message": f"Could not read schema ({e}). Generic starter SQL loaded.",
        }
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


# ============================================================
# SAVE QUERY RESULT AS DATASET
# ------------------------------------------------------------
# Materializes the current query result (re-executed against
# the source parquet) into a new derived dataset on disk.
# The derived dataset is then available like any imported
# dataset — Schema, Preview, SQL queries, and export all work.
# ============================================================

@app.post("/api/datasets/save_result")
def api_save_result_as_dataset(req: SaveResultAsDatasetRequest):
    """Re-execute SQL and save the full result as a new named dataset."""
    t0 = time.perf_counter()

    # 1. Sanitize and validate the requested name
    raw_name = (req.name or "").strip()
    sanitized = re.sub(r"[^A-Za-z0-9_]+", "_", raw_name).strip("_")[:50]
    if not sanitized:
        raise HTTPException(status_code=400, detail="Dataset name must contain at least one alphanumeric character.")
    sanitized = sanitized.lower()

    # 2. Resolve a unique name — auto-suffix if collision
    base = sanitized
    candidate = base
    suffix = 2
    while (DATASETS_DIR / candidate).exists():
        candidate = f"{base}_{suffix}"
        suffix += 1
        if suffix > 99:
            raise HTTPException(status_code=400, detail=f"Too many datasets named '{base}'; choose a different name.")
    ds_name = candidate

    # 3. Validate source dataset
    if req.dataset not in list_datasets():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {req.dataset}")

    # 4. Validate SQL is read-only
    try:
        cleaned_sql = _strip_trailing_semicolon(_validate_readonly_sql(req.sql))
    except HTTPException:
        raise

    # 5. Rewrite SQL references (same pipeline as /api/sql)
    try:
        src, _ = dataset_source_path(req.dataset)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    esc = _sql_escape_path(src)
    parquet_sql = f"read_parquet('{esc}')"

    reference_parquet_sql, reference_name = _resolve_reference_for_sql(req.reference)

    additional_datasets: dict[str, str] = {}
    for other_name in list_datasets():
        if other_name == req.dataset:
            continue
        try:
            other_src, _ = dataset_source_path(other_name)
            additional_datasets[other_name] = f"read_parquet('{_sql_escape_path(other_src)}')"
        except Exception:
            pass

    additional_references = _build_additional_references()

    sql = _rewrite_sql_dataset_reference(
        sql=cleaned_sql,
        dataset_name=req.dataset,
        parquet_sql=parquet_sql,
        reference_parquet_sql=reference_parquet_sql,
        reference_name=reference_name,
        additional_datasets=additional_datasets,
        additional_references=additional_references,
    )

    # 6. Create directory and write parquet using DuckDB COPY
    ds_dir = DATASETS_DIR / ds_name
    ds_dir.mkdir(parents=True, exist_ok=True)
    dest = (ds_dir / "source.parquet").resolve()
    dest_str = str(dest).replace("\\", "/")

    con = _connect()
    try:
        con.execute(f"COPY ({sql}) TO '{dest_str}' (FORMAT PARQUET)")
        row_count = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{dest_str}')").fetchone()[0])
        col_count = len(con.execute(f"DESCRIBE SELECT * FROM read_parquet('{dest_str}')").fetchall())
    except duckdb.Error as e:
        # Clean up the empty dir if write failed
        try:
            _rmtree_robust(ds_dir)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Failed to save result: {e}") from e
    finally:
        con.close()

    # 7. Write _meta.json so the dataset is recognized by list_datasets()
    meta = {
        "dataset_type": "derived",
        "row_count": row_count,
        "column_count": col_count,
        "source_dataset": req.dataset,
        "source_query": req.sql,
    }
    (ds_dir / "_meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    elapsed = round(time.perf_counter() - t0, 4)
    logger.info("save_result success | dataset=%s row_count=%d elapsed=%s", ds_name, row_count, elapsed)

    # 8. Log session event — include "dataset" key so _build_resume_state picks it up
    try:
        log_event(SessionEventType.DATASET_DERIVED, {
            "dataset": ds_name,
            "name": ds_name,
            "row_count": row_count,
            "source_query": req.sql,
        })
    except Exception:
        logger.warning("Failed to log DATASET_DERIVED event", exc_info=True)

    return {
        "status": "ok",
        "dataset": ds_name,
        "row_count": row_count,
        "column_count": col_count,
        "elapsed_seconds": elapsed,
    }


@app.get("/api/session")
def api_session():
    """Return current session log."""
    session = get_current_session()
    if session is None:
        return {"error": "No active session"}
    from dataclasses import asdict
    return asdict(session)


@app.api_route("/api/session/export", methods=["GET", "POST"])
def api_session_export():
    """Export current session log to disk and return it.

    Accepts both GET (legacy) and POST (preferred — explicit write action).
    The export_session() call writes synchronously to disk with an explicit
    flush so the file is guaranteed to exist when the response returns.
    """
    path = export_session(SESSIONS_DIR)
    session = get_current_session()
    if session is None:
        return {"error": "No active session"}
    from dataclasses import asdict
    return {"exported_to": str(path), "session": asdict(session)}


@app.get("/api/session/summary")
def api_session_summary():
    """Return session summary (counts, duration, datasets)."""
    return session_summary()


class SessionNameRequest(BaseModel):
    name: str = ""
    description: str = ""


@app.post("/api/session/name")
def api_session_name(req: SessionNameRequest):
    """Set name and description on the current session."""
    session = get_current_session()
    if session is None:
        raise HTTPException(status_code=404, detail="No active session")
    session.name = req.name
    session.description = req.description
    return {"status": "ok", "name": req.name, "description": req.description}


@app.post("/api/session/reset")
def api_session_reset():
    """Start a fresh session, discarding all events from the current one."""
    session = start_session()
    return {
        "status": "ok",
        "session_id": session.session_id,
        "started_at": session.started_at,
    }


class LogEventRequest(BaseModel):
    event_type: str
    details: dict = {}


@app.post("/api/session/log_event")
def api_session_log_event(req: LogEventRequest):
    """Manually append an event to the current session log.

    Used by the frontend during workflow replay to record dataset/reference
    steps that don't go through the normal import endpoints.
    """
    try:
        et = SessionEventType(req.event_type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid event type: {req.event_type}")
    log_event(et, req.details)
    return {"status": "ok"}


@app.get("/api/session/load/{filename}")
def api_session_load(filename: str):
    """Load and return full session JSON data from a saved file."""
    from app.services.session_replay import SessionReplayEngine
    engine = SessionReplayEngine(DATASETS_DIR, REFERENCES_DIR, REFERENCE_LIBRARY_DIR, SESSIONS_DIR)
    try:
        data = engine.load_session(filename)
        return data
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session file not found: {filename}")


# ---------------------------------------------------------------------------
# Session replay endpoints
# ---------------------------------------------------------------------------

class ResumeRequest(BaseModel):
    filename: str


def _derive_resume_state(events: list[dict]) -> dict:
    """Derive resume state from raw event dicts (for older session files without resume_state)."""
    state: dict = {}
    for event in reversed(events):
        et = event.get("event_type", "")
        details = event.get("details", {})
        if et == "query_run" and "dataset" not in state:
            state["dataset"] = details.get("dataset", "")
            state["last_sql"] = details.get("sql", "")
        if et == "dataset_import" and "dataset" not in state:
            state["dataset"] = details.get("dataset", "")
        if et == "ai_sql_generated" and "last_question" not in state:
            state["last_question"] = details.get("question", "")
        if et == "reference_load" and "reference" not in state:
            state["reference"] = {
                "name": details.get("reference_name", ""),
                "library_source": details.get("source", ""),
            }
        if et == "reference_delete" and "reference" not in state:
            state["reference"] = None
    return state


def _restore_single_reference(ref_info: dict) -> dict:
    """Try to restore a reference table from disk, library, or example cases.

    Returns a dict with 'name', 'loaded' (bool), and optionally row/column counts.
    """
    rname = ref_info.get("name", "")
    rsource = ref_info.get("library_source", "")

    # 1. Already on disk
    if rname:
        ref_pq = (REFERENCES_DIR / rname / "source.parquet").resolve()
        if ref_pq.exists():
            resp: dict = {"name": rname, "loaded": True}
            meta_path = REFERENCES_DIR / rname / "_meta.json"
            if meta_path.exists():
                try:
                    m = json.loads(meta_path.read_text(encoding="utf-8"))
                    resp["row_count"] = m.get("row_count")
                    resp["column_count"] = m.get("column_count")
                except Exception:
                    pass
            return resp

    # 2. Reference library
    if rsource:
        csv_path = REFERENCE_LIBRARY_DIR / rsource
        if csv_path.exists():
            try:
                with csv_path.open("rb") as f:
                    result = import_reference_table(
                        uploaded_file=f,
                        original_filename=rsource,
                        registered_root=REFERENCES_DIR,
                        overwrite=True,
                    )
                return {
                    "name": result.reference_name,
                    "loaded": True,
                    "row_count": result.row_count,
                    "column_count": len(result.columns),
                }
            except Exception as exc:
                logger.warning("Failed to load reference %s from library: %s", rname, exc)

    # 3. Search example case reference directories
    search_name = rsource or (rname + ".csv")
    if EXAMPLE_CASES_DIR.exists():
        for case_dir in EXAMPLE_CASES_DIR.iterdir():
            ref_dir = case_dir / "reference"
            if not ref_dir.is_dir():
                continue
            candidate = ref_dir / search_name
            if candidate.exists():
                try:
                    with candidate.open("rb") as f:
                        result = import_reference_table(
                            uploaded_file=f,
                            original_filename=candidate.name,
                            registered_root=REFERENCES_DIR,
                            overwrite=True,
                        )
                    return {
                        "name": result.reference_name,
                        "loaded": True,
                        "row_count": result.row_count,
                        "column_count": len(result.columns),
                    }
                except Exception as exc:
                    logger.warning("Failed to load reference %s from example case: %s", rname, exc)
                break

    return {"name": rname, "loaded": False, "message": f"Reference '{rname}' not found"}


@app.post("/api/references/restore")
def api_restore_reference(name: str, source: str = ""):
    """Restore a reference table from disk, library, or example cases.

    Used by the frontend during workflow replay to ensure a reference table
    is available before queries that depend on it execute.
    """
    result = _restore_single_reference({"name": name, "library_source": source})
    if result.get("loaded"):
        try:
            log_event(SessionEventType.REFERENCE_LOAD, {
                "reference_name": result["name"],
                "row_count": result.get("row_count"),
                "source": source or (name + ".csv"),
            })
        except Exception:
            logger.warning("Failed to log session event", exc_info=True)
    return result


@app.post("/api/session/resume")
def api_session_resume(req: ResumeRequest):
    """Restore session state for Resume mode."""
    from app.services.session_replay import SessionReplayEngine

    engine = SessionReplayEngine(DATASETS_DIR, REFERENCES_DIR, REFERENCE_LIBRARY_DIR, SESSIONS_DIR)
    try:
        session_data = engine.load_session(req.filename)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session file not found: {req.filename}")

    # Get resume_state (or derive from events for older sessions)
    resume_state = session_data.get("resume_state", {})
    if not resume_state:
        resume_state = _derive_resume_state(session_data.get("events", []))

    dataset_name = resume_state.get("dataset", "")
    last_sql = resume_state.get("last_sql", "")
    last_question = resume_state.get("last_question", "")
    ref_info = resume_state.get("reference")

    # Check dataset exists — use same markers as list_datasets() so a dataset
    # that appears in the sidebar is never reported as missing here.
    dataset_exists = False
    if dataset_name:
        ds_dir = (DATASETS_DIR / dataset_name).resolve()
        dataset_exists = ds_dir.exists() and any(
            (ds_dir / m).exists()
            for m in ("source.parquet", "_meta.json", "metadata.json", "dataset_context.json")
        )

    if dataset_name and not dataset_exists:
        return {
            "status": "dataset_missing",
            "dataset": dataset_name,
            "dataset_exists": False,
            "reference": None,
            "last_sql": last_sql,
            "last_question": last_question,
            "message": f"Dataset '{dataset_name}' not found — import it first, then try Resume again.",
        }

    # Load the active reference table — try disk, library, then example cases
    ref_response = None
    if ref_info and (ref_info.get("name") or ref_info.get("library_source")):
        ref_response = _restore_single_reference(ref_info)

    # Collect all datasets from session for multi-dataset restore
    all_datasets = resume_state.get("all_datasets", [])
    if dataset_name and dataset_name not in all_datasets:
        all_datasets.insert(0, dataset_name)

    # Load ALL references from session — not just the active one.
    # The SQL rewriter resolves any registered reference by name, so all
    # must be on disk for multi-reference queries to work.
    all_references = resume_state.get("all_references", [])
    all_ref_responses: list[dict] = []
    for ref in all_references:
        if not ref.get("name"):
            continue
        all_ref_responses.append(_restore_single_reference(ref))

    return {
        "status": "ready",
        "dataset": dataset_name,
        "dataset_exists": dataset_exists,
        "reference": ref_response,
        "all_datasets": all_datasets,
        "all_references": all_ref_responses,
        "last_sql": last_sql,
        "last_question": last_question,
        "message": None,
    }


class ReplayRequest(BaseModel):
    filename: str
    stop_on_failure: bool = False


class AnnotateRequest(BaseModel):
    filename: str


class ReplayPrepareRequest(BaseModel):
    filename: str


@app.post("/api/session/replay/prepare")
def api_session_replay_prepare(req: ReplayPrepareRequest):
    """Load a saved session and extract its dataset/reference requirements.

    Returns information the Replay Wizard needs to show the dataset mapping UI:
    which datasets the session expects, which are already on disk, and which
    reference tables are needed.
    """
    from app.services.session_replay import SessionReplayEngine
    engine = SessionReplayEngine(DATASETS_DIR, REFERENCES_DIR, REFERENCE_LIBRARY_DIR, SESSIONS_DIR)
    try:
        session_data = engine.load_session(req.filename)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Session file not found: {req.filename}")

    events = session_data.get("events", [])

    # Extract unique datasets from import and query_run events
    required_datasets: list[dict] = []
    seen_datasets: set[str] = set()
    required_references: list[dict] = []
    seen_refs: set[str] = set()
    replayable_count = 0

    for ev in events:
        et = ev.get("event_type", "")
        d = ev.get("details", {})

        if et in ("dataset_import", "query_run", "reference_load", "reference_delete", "export", "session_end"):
            replayable_count += 1

        if et == "dataset_import":
            name = d.get("dataset", "")
            if name and name not in seen_datasets:
                seen_datasets.add(name)
                ds_dir = (DATASETS_DIR / name).resolve()
                exists = ds_dir.exists() and any(
                    (ds_dir / m).exists()
                    for m in ("source.parquet", "_meta.json", "metadata.json")
                )
                required_datasets.append({
                    "name": name,
                    "exists": exists,
                    "row_count": d.get("row_count"),
                    "column_count": d.get("column_count"),
                })

        if et == "reference_load":
            ref_name = d.get("reference_name", "")
            if ref_name and ref_name not in seen_refs:
                seen_refs.add(ref_name)
                ref_pq = (REFERENCES_DIR / ref_name / "source.parquet").resolve()
                exists = ref_pq.exists()
                required_references.append({
                    "name": ref_name,
                    "source": d.get("source", ""),
                    "exists": exists,
                })

    return {
        "session_name": session_data.get("name", ""),
        "event_count": len(events),
        "replayable_count": replayable_count,
        "required_datasets": required_datasets,
        "required_references": required_references,
        "events": events,
    }


@app.get("/api/session/files")
def api_session_files():
    """List available session files for replay."""
    from app.services.session_replay import SessionReplayEngine
    engine = SessionReplayEngine(DATASETS_DIR, REFERENCES_DIR, REFERENCE_LIBRARY_DIR, SESSIONS_DIR)
    return {"files": engine.list_session_files()}


@app.post("/api/session/replay")
def api_session_replay(req: ReplayRequest):
    """Replay a session file and return the replay report."""
    from app.services.session_replay import SessionReplayEngine
    engine = SessionReplayEngine(DATASETS_DIR, REFERENCES_DIR, REFERENCE_LIBRARY_DIR, SESSIONS_DIR)
    report = engine.replay(req.filename, stop_on_failure=req.stop_on_failure)
    from dataclasses import asdict
    return asdict(report)


@app.post("/api/session/annotate")
def api_session_annotate(req: AnnotateRequest):
    """Run a session and record actual row counts as baselines."""
    from app.services.session_replay import SessionReplayEngine
    engine = SessionReplayEngine(DATASETS_DIR, REFERENCES_DIR, REFERENCE_LIBRARY_DIR, SESSIONS_DIR)
    result = engine.annotate_baselines(req.filename)
    return {"status": "annotated", "baselines_added": len(result.get("baselines", []))}


# ============================================================
# EXAMPLE CASES
# ============================================================


@app.get("/api/example_cases")
def api_example_cases():
    """List available example cases from EXAMPLE_CASES_DIR."""
    cases: list[dict] = []
    if not EXAMPLE_CASES_DIR.exists():
        return {"cases": []}
    for case_dir in sorted(EXAMPLE_CASES_DIR.iterdir()):
        if not case_dir.is_dir():
            continue
        meta_path = case_dir / "metadata.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            meta["has_data"] = (case_dir / "data").exists() and any((case_dir / "data").iterdir())
            meta["has_reference"] = (case_dir / "reference").exists() and any((case_dir / "reference").iterdir())
            cases.append(meta)
        except Exception as exc:
            logger.warning("Failed to read example case %s: %s", case_dir.name, exc)
    return {"cases": cases}


class LoadCaseRequest(BaseModel):
    mode: str = "resume"  # "resume", "tutorial", or "runall"


@app.post("/api/example_cases/{case_id}/load")
def api_load_example_case(case_id: str, req: LoadCaseRequest):
    """Load an example case: import its dataset and reference table(s)."""
    case_dir = (EXAMPLE_CASES_DIR / case_id).resolve()
    if not case_dir.exists():
        raise HTTPException(status_code=404, detail=f"Example case not found: {case_id}")

    meta_path = case_dir / "metadata.json"
    if not meta_path.exists():
        raise HTTPException(status_code=404, detail=f"Example case metadata not found: {case_id}")

    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    # Import all sample datasets (multi-dataset cases have several files)
    dataset_name = None
    imported_datasets: list[str] = []
    data_dir = case_dir / "data"

    if data_dir.exists():
        for data_file in data_dir.iterdir():
            if data_file.suffix.lower() in (".csv", ".tsv", ".xlsx", ".parquet"):
                try:
                    with data_file.open("rb") as f:
                        result = import_dataset(
                            uploaded_file=f,
                            original_filename=data_file.name,
                            display_name=None,
                            registered_root=str(DATASETS_DIR),
                            overwrite=True,
                        )
                    imported_datasets.append(result.metadata.registered_name)
                    # First imported dataset becomes the "primary" for response
                    if dataset_name is None:
                        dataset_name = result.metadata.registered_name
                    try:
                        log_event(SessionEventType.DATASET_IMPORT, {
                            "dataset": result.metadata.registered_name,
                            "row_count": result.metadata.row_count,
                            "column_count": result.metadata.column_count,
                            "source_filename": data_file.name,
                        })
                    except Exception:
                        logger.warning("Failed to log session event", exc_info=True)
                except Exception as exc:
                    logger.warning("Failed to import example case dataset %s: %s", data_file.name, exc)

    # Load reference tables
    ref_info: list[dict] = []
    ref_dir = case_dir / "reference"
    if ref_dir.exists():
        for ref_file in ref_dir.iterdir():
            if ref_file.suffix.lower() in (".csv", ".tsv", ".xlsx"):
                try:
                    with ref_file.open("rb") as f:
                        ref_result = import_reference_table(
                            uploaded_file=f,
                            original_filename=ref_file.name,
                            registered_root=REFERENCES_DIR,
                            overwrite=True,
                        )
                    ref_info.append({
                        "name": ref_result.reference_name,
                        "row_count": ref_result.row_count,
                        "column_count": len(ref_result.columns),
                        "columns": len(ref_result.columns),
                    })
                    try:
                        log_event(SessionEventType.REFERENCE_LOAD, {
                            "reference_name": ref_result.reference_name,
                            "row_count": ref_result.row_count,
                            "source": ref_file.name,
                        })
                    except Exception:
                        logger.warning("Failed to log session event", exc_info=True)
                except Exception as exc:
                    logger.warning("Failed to import example case reference: %s", exc)

    return {
        "status": "loaded",
        "case_id": case_id,
        "dataset": dataset_name,
        "references": ref_info,
        "metadata": meta,
    }


@app.post("/api/example_cases/{case_id}/import_dataset")
def api_example_case_import_dataset(case_id: str, filename: str | None = None):
    """Import a dataset from an example case (for tutorial step-by-step replay).

    Unlike /load which imports everything at once, this imports one dataset at a
    time so the tutorial can show each step happening live.

    When *filename* is provided (e.g. ``fl_medicaid_claims.csv``), only that
    specific file is imported.  Without it, the first importable file is used
    (backwards-compatible with single-dataset cases).
    """
    case_dir = (EXAMPLE_CASES_DIR / case_id).resolve()
    if not case_dir.exists():
        raise HTTPException(status_code=404, detail=f"Example case not found: {case_id}")

    meta_path = case_dir / "metadata.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}

    data_dir = case_dir / "data"
    if not data_dir.exists():
        raise HTTPException(status_code=404, detail="No data directory in example case")

    for data_file in data_dir.iterdir():
        if data_file.suffix.lower() not in (".csv", ".tsv", ".xlsx", ".parquet"):
            continue
        # If caller asked for a specific file, skip non-matches
        if filename and data_file.name != filename:
            continue
        try:
            # Use filename stem as display_name so the registered name matches
            # what session.json expects (e.g. "tx_medicaid_claims" not
            # "texas_medicaid_claims_500_row_sample").
            with data_file.open("rb") as f:
                result = import_dataset(
                    uploaded_file=f,
                    original_filename=data_file.name,
                    display_name=None,
                    registered_root=str(DATASETS_DIR),
                    overwrite=True,
                )
            try:
                log_event(SessionEventType.DATASET_IMPORT, {
                    "dataset": result.metadata.registered_name,
                    "row_count": result.metadata.row_count,
                    "column_count": result.metadata.column_count,
                    "source_filename": data_file.name,
                })
            except Exception:
                logger.warning("Failed to log session event", exc_info=True)
            return {
                "status": "imported",
                "dataset": result.metadata.registered_name,
                "row_count": result.metadata.row_count,
                "column_count": result.metadata.column_count,
            }
        except Exception as exc:
            raise HTTPException(
                status_code=500, detail=f"Dataset import failed: {exc}"
            ) from exc

    raise HTTPException(status_code=404, detail="No importable data file found in example case")


@app.post("/api/example_cases/{case_id}/import_reference")
def api_example_case_import_reference(case_id: str, filename: str | None = None):
    """Import reference table(s) from an example case.

    When *filename* is provided (e.g. ``medicaid_schema_map.csv``), only that
    specific file is imported.  Without it, all reference files are imported
    (backwards-compatible with single-reference and bulk-load cases).
    """
    case_dir = (EXAMPLE_CASES_DIR / case_id).resolve()
    if not case_dir.exists():
        raise HTTPException(status_code=404, detail=f"Example case not found: {case_id}")

    ref_dir = case_dir / "reference"
    if not ref_dir.exists():
        raise HTTPException(status_code=404, detail="No reference directory in example case")

    ref_info: list[dict] = []
    for ref_file in ref_dir.iterdir():
        if ref_file.suffix.lower() not in (".csv", ".tsv", ".xlsx"):
            continue
        # If caller asked for a specific file, skip non-matches
        if filename and ref_file.name != filename:
            continue
        try:
            with ref_file.open("rb") as f:
                ref_result = import_reference_table(
                    uploaded_file=f,
                    original_filename=ref_file.name,
                    registered_root=REFERENCES_DIR,
                    overwrite=True,
                )
            ref_info.append({
                "name": ref_result.reference_name,
                "row_count": ref_result.row_count,
                "column_count": len(ref_result.columns),
            })
            try:
                log_event(SessionEventType.REFERENCE_LOAD, {
                    "reference_name": ref_result.reference_name,
                    "row_count": ref_result.row_count,
                    "source": ref_file.name,
                })
            except Exception:
                logger.warning("Failed to log session event", exc_info=True)
        except Exception as exc:
            logger.warning("Failed to import example case reference: %s", exc)

    if not ref_info:
        raise HTTPException(status_code=404, detail="No importable reference files found")

    return {"status": "imported", "references": ref_info}


@app.get("/api/example_cases/{case_id}/session")
def api_example_case_session(case_id: str):
    """Return the session JSON for an example case (for tutorial step-through)."""
    case_dir = (EXAMPLE_CASES_DIR / case_id).resolve()
    session_path = case_dir / "session.json"
    if not session_path.exists():
        raise HTTPException(status_code=404, detail=f"No session file for example case: {case_id}")
    try:
        data = json.loads(session_path.read_text(encoding="utf-8"))
        return data
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to read session file: {exc}"
        ) from exc


# ============================================================
# SAVED SESSIONS
# ============================================================


@app.post("/api/sessions/{filename}/delete")
def api_session_delete(filename: str):
    """Delete a saved session file."""
    filepath = (SESSIONS_DIR / filename).resolve()
    if not filepath.exists():
        raise HTTPException(status_code=404, detail=f"Session file not found: {filename}")
    if not str(filepath).startswith(str(SESSIONS_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid filename")
    filepath.unlink()
    return {"status": "deleted", "filename": filename}


@app.get("/api/sessions/saved")
def api_sessions_saved():
    """List named (saved) sessions from SESSIONS_DIR."""
    saved: list[dict] = []
    if not SESSIONS_DIR.exists():
        return {"sessions": []}
    import re as _re
    _uuid_pattern = _re.compile(
        r"^session_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_\d{8}\.json$"
    )
    for f in sorted(SESSIONS_DIR.glob("*.json"), reverse=True):
        # Skip UUID-based auto-save files — they are crash-recovery files only.
        # Even if they carry a session name (because the user named the session
        # before an auto-save fired), they must not appear alongside the
        # explicitly-exported named file, which would create duplicates.
        if _uuid_pattern.match(f.name):
            continue
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            name = data.get("name", "").strip()
            if not name:
                continue
            saved.append({
                "filename": f.name,
                "session_id": data.get("session_id", ""),
                "name": name,
                "description": data.get("description", ""),
                "started_at": data.get("started_at", ""),
                "event_count": len(data.get("events", [])),
                "ai_mode": data.get("ai_mode", "cloud"),
            })
        except Exception:
            continue
    return {"sessions": saved}


# ============================================================
# WORKSPACE SNAPSHOT — M5 Component 6
# ============================================================
# Persists workspace state across app restarts so the analyst
# can resume exactly where they left off.

WORKSPACE_PATH = (DATA_DIR / "workspace.json").resolve()


def _write_workspace(snapshot: dict) -> None:
    """Write workspace.json with explicit flush."""
    import os as _os
    WORKSPACE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(WORKSPACE_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
        f.flush()
        _os.fsync(f.fileno())
    logger.info("Workspace snapshot written to %s", WORKSPACE_PATH)


def _build_workspace_snapshot(
    dataset: str | None = None,
    references: list[dict] | None = None,
    last_query: str = "",
    last_tab: str = "query",
    session_name: str = "",
) -> dict:
    """Build a workspace snapshot dict from current state."""
    snapshot: dict = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "dataset": None,
        "references": [],
        "last_query": last_query,
        "last_tab": last_tab,
        "session_name": session_name,
        "ai_mode": "cloud",
    }

    if dataset:
        ds_dir = (DATASETS_DIR / dataset).resolve()
        src = ds_dir / "source.parquet"
        meta_path = ds_dir / "_meta.json"
        row_count = None
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                row_count = meta.get("row_count")
            except Exception:
                pass
        snapshot["dataset"] = {
            "name": dataset,
            "source_path": str(src) if src.exists() else None,
            "row_count": row_count,
        }

    if references:
        snapshot["references"] = references
    elif REFERENCES_DIR.exists():
        for d in REFERENCES_DIR.iterdir():
            if d.is_dir() and (d / "source.parquet").exists():
                ref_entry = {"name": d.name, "source_path": str((d / "source.parquet").resolve())}
                ref_meta_path = d / "_meta.json"
                if ref_meta_path.exists():
                    try:
                        rm = json.loads(ref_meta_path.read_text(encoding="utf-8"))
                        ref_entry["row_count"] = rm.get("row_count")
                        ref_entry["column_count"] = rm.get("column_count")
                    except Exception:
                        pass
                snapshot["references"].append(ref_entry)

    session = get_current_session()
    if session and session.name:
        snapshot["session_name"] = session.name
    if session:
        snapshot["ai_mode"] = session.ai_mode

    return snapshot


class WorkspaceSnapshotRequest(BaseModel):
    dataset: str | None = None
    references: list[dict] | None = None
    last_query: str = ""
    last_tab: str = "query"
    session_name: str = ""


@app.get("/api/workspace")
def api_workspace_get():
    """Return current workspace.json contents, or null if none exists."""
    if not WORKSPACE_PATH.exists():
        return {"workspace": None}
    try:
        data = json.loads(WORKSPACE_PATH.read_text(encoding="utf-8"))
        # Verify dataset file still exists
        if data.get("dataset") and data["dataset"].get("source_path"):
            if not Path(data["dataset"]["source_path"]).exists():
                data["dataset"]["file_missing"] = True
        return {"workspace": data}
    except Exception as exc:
        logger.warning("Failed to read workspace.json: %s", exc)
        return {"workspace": None}


@app.post("/api/workspace")
def api_workspace_save(req: WorkspaceSnapshotRequest):
    """Write workspace.json from frontend state."""
    snapshot = _build_workspace_snapshot(
        dataset=req.dataset,
        references=req.references,
        last_query=req.last_query,
        last_tab=req.last_tab,
        session_name=req.session_name,
    )
    _write_workspace(snapshot)
    return {"status": "saved", "path": str(WORKSPACE_PATH)}


@app.post("/api/workspace/restore")
def api_workspace_restore():
    """Restore workspace from workspace.json.

    Re-validates that dataset and reference files exist.
    Returns the workspace data for the frontend to act on.
    """
    if not WORKSPACE_PATH.exists():
        raise HTTPException(status_code=404, detail="No workspace snapshot found")

    try:
        data = json.loads(WORKSPACE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Corrupted workspace.json: {exc}") from exc

    result: dict = {"status": "ok", "warnings": []}

    # Validate dataset
    ds = data.get("dataset")
    if ds and ds.get("name"):
        ds_dir = (DATASETS_DIR / ds["name"]).resolve()
        if (ds_dir / "source.parquet").exists():
            result["dataset"] = ds["name"]
        else:
            result["warnings"].append(f"Dataset '{ds['name']}' not found in datasets directory.")
            result["dataset"] = None
    else:
        result["dataset"] = None

    # Validate references
    result["references"] = []
    for ref in data.get("references", []):
        ref_name = ref.get("name", "")
        ref_dir = (REFERENCES_DIR / ref_name).resolve()
        if ref_dir.exists() and (ref_dir / "source.parquet").exists():
            meta_path = ref_dir / "_meta.json"
            ref_info = {"name": ref_name, "loaded": True}
            if meta_path.exists():
                try:
                    rm = json.loads(meta_path.read_text(encoding="utf-8"))
                    ref_info["row_count"] = rm.get("row_count")
                    ref_info["column_count"] = rm.get("column_count")
                except Exception:
                    pass
            result["references"].append(ref_info)
        else:
            result["warnings"].append(f"Reference '{ref_name}' not found.")

    result["last_query"] = data.get("last_query", "")
    result["last_tab"] = data.get("last_tab", "query")
    result["session_name"] = data.get("session_name", "")

    return result


@app.delete("/api/workspace")
def api_workspace_delete():
    """Delete workspace.json (Start Fresh)."""
    if WORKSPACE_PATH.exists():
        WORKSPACE_PATH.unlink()
        logger.info("Workspace snapshot deleted")
    return {"status": "deleted"}


@app.post("/api/shutdown")
def api_shutdown(bg: BackgroundTasks):
    """
    Hard-stop the process so the desktop app can be launched
    again immediately.

    This uses os._exit(0) because Windows + PyInstaller +
    no-console packaging can be unreliable with soft exits.
    """
    logger.info("shutdown requested")

    try:
        end_session()
        # Session export is intentionally NOT done here — sessions are only
        # written to disk when the user explicitly saves via /api/session/export.
        # Auto-exporting on shutdown created unwanted session files on plain Exit
        # and duplicate files on Save & Exit (named file + unnamed file).
    except Exception:
        logger.warning("Failed to end session on shutdown", exc_info=True)

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