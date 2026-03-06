from __future__ import annotations

import getpass
import json
import logging
import os
import platform
import re
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import FileResponse, Response

from app.presets.doge import PRESETS  # noqa: E402

logger = logging.getLogger("app")

app = FastAPI(title="Analytics Workbench")

def app_base_dir() -> Path:
    """
    Packaged: folder containing the EXE
    Dev: repo root (…/Analytics Workbench)
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    # backend/app/main.py -> repo root
    return Path(__file__).resolve().parents[2]


BASE_DIR = app_base_dir()

FRONTEND_DIR = Path(os.getenv("AW_FRONTEND_DIR", str(BASE_DIR / "frontend")))
DATA_DIR = Path(os.getenv("AW_DATA_DIR", str(BASE_DIR / "data")))
DATASETS_DIR = Path(os.getenv("AW_DATASETS_DIR", str(DATA_DIR / "datasets")))
EXPORTS_DIR = Path(os.getenv("AW_EXPORTS_DIR", str(BASE_DIR / "exports")))

DATASETS_DIR.mkdir(parents=True, exist_ok=True)
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Preview/export safety
DEFAULT_PREVIEW_ROWS = int(os.getenv("AW_DEFAULT_PREVIEW_ROWS", "50"))
MAX_PREVIEW_ROWS = int(os.getenv("AW_MAX_PREVIEW_ROWS", "200"))
MAX_EXPORT_ROWS = int(os.getenv("AW_MAX_EXPORT_ROWS", "200000"))

# Startup confirmation — logged once at import time
logger.info(
    "app started | mode=%s | exports_dir=%s",
    "packaged" if getattr(sys, "frozen", False) else "dev",
    EXPORTS_DIR,
)




# ============================================================
# Presets loading (6B)
# ============================================================
_REQUIRED_PRESET_FIELDS = {"id", "name", "sql"}


def _validate_presets(raw: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        if not _REQUIRED_PRESET_FIELDS.issubset(item.keys()):
            continue
        out.append(item)
    return out


def _load_presets() -> list[dict[str, Any]]:
    """Load presets.json from app data/base dir; fallback to python PRESETS."""
    # Prefer an explicit path if provided
    explicit = os.getenv("AW_PRESETS_PATH")
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    # Common locations
    candidates.extend([
        BASE_DIR / "presets.json",
        DATA_DIR / "presets.json",
        DATASETS_DIR / "presets.json",
    ])
    for fp in candidates:
        try:
            if fp.exists() and fp.is_file():
                raw = json.loads(fp.read_text(encoding="utf-8"))
                if isinstance(raw, list):
                    validated = _validate_presets(raw)
                    if validated:
                        logger.info("loaded presets.json | path=%s count=%d", fp, len(validated))
                        return validated
        except Exception as e:
            logger.warning("failed to load presets.json | path=%s | reason=%s", fp, e)
    return PRESETS


ACTIVE_PRESETS = _load_presets()

# Preview + safety caps
MAX_PREVIEW_ROWS = int(os.getenv("AW_MAX_PREVIEW_ROWS", "200"))
DEFAULT_PREVIEW_ROWS = int(os.getenv("AW_DEFAULT_PREVIEW_ROWS", "50"))


# ============================================================
# Runtime paths (single source of truth)
# ============================================================

# ============================================================
# Models
# ============================================================


class ScanRequest(BaseModel):
    path: str
    recursive: bool = False


class RegisterRequest(BaseModel):
    dataset_name: str
    parquet_path: str
    mode: str = "reference"  # "reference" or "copy"


# ============================================================
# Helpers
# ============================================================

def _safe_name(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name[:64] if name else "Dataset"


def list_datasets() -> list[str]:
    if not DATASETS_DIR.exists():
        return []
    out: list[str] = []
    for ds in DATASETS_DIR.iterdir():
        if not ds.is_dir():
            continue
        # local parquet mode
        if any(ds.glob("*.parquet")):
            out.append(ds.name)
            continue
        # reference mode
        ref = ds / "_reference.txt"
        if ref.exists():
            ref_path = ref.read_text(encoding="utf-8").strip()
            if ref_path and Path(ref_path).exists():
                out.append(ds.name)
    return sorted(out)


def dataset_source_path(dataset: str) -> tuple[str, bool]:
    """
    Returns (path_or_glob, is_glob)
    - reference mode: absolute parquet path
    - copy mode: datasets/<dataset>/*.parquet
    """
    ds_dir = (DATASETS_DIR / dataset).resolve()
    ref = ds_dir / "_reference.txt"
    if ref.exists():
        target = ref.read_text(encoding="utf-8").strip()
        if not target:
            raise FileNotFoundError(f"Reference dataset '{dataset}' has empty _reference.txt")
        p = Path(target)
        if not p.exists():
            raise FileNotFoundError(f"Reference dataset '{dataset}' points to missing file: {target}")
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
    # Fileless connection is fine for query/extract workloads
    return duckdb.connect()


def _sql_escape_path(p: str) -> str:
    # Defensive: single-quote escape for embedding into SQL string
    return p.replace("'", "''")


def _is_writable_dir(p: Path) -> tuple[bool, str | None]:
    """Returns (ok, error_message)."""
    try:
        p.mkdir(parents=True, exist_ok=True)
        test = p / ".__aw_write_test__"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
        return True, None
    except Exception as e:
        return False, str(e)


def _duckdb_ok() -> tuple[bool, str | None]:
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


def _dataset_meta_summary(dataset: str) -> dict[str, Any]:
    """
    Returns a lightweight metadata summary for a dataset.

    Prefers cached _meta.json.
    Falls back to live computation if metadata is missing.
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
            con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{esc}')"
            ).fetchone()[0]
        )


        # parquet_schema() returns one row per column
        column_count = int(
            con.execute(
                f"SELECT COUNT(*) FROM parquet_schema('{esc}')"
            ).fetchone()[0]
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
        "column_count": column_count,
        "file_size_bytes": file_size_bytes,
        "meta_source": "live",
    }


def _audit_log(event: dict[str, Any]) -> None:
    """
    Append-only JSONL audit log.
    Non-fatal: never raises.
    """
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
# Dynamic preset params (4B)
# ============================================================

def _extract_dynamic_params(request: Request, reserved: set[str], threshold_fallback: int | None) -> dict[str, Any]:
    """
    Pull query params from the request excluding reserved keys.
    Values remain strings (DuckDB SQL templates are string formatted).
    """
    provided: dict[str, Any] = {}
    for k, v in request.query_params.items():
        if k in reserved:
            continue
        provided[k] = v
    if threshold_fallback is not None and "threshold" not in provided:
        provided["threshold"] = threshold_fallback
    return provided


def _build_final_params(preset_def: dict[str, Any], provided_params: dict[str, Any]) -> dict[str, Any]:
    """Merge preset default params with provided params; validate required placeholders."""
    final_params = dict(preset_def.get("params", {}) or {})
    final_params.update({k: v for k, v in provided_params.items() if v is not None})

    # Try formatting early to surface missing params nicely
    try:
        preset_def["sql"].format(**final_params)
    except KeyError as e:
        missing = str(e).strip("'")
        raise HTTPException(status_code=400, detail=f"Missing required preset param: {missing}")
    return final_params


def _sql_for(dataset: str, preset_id: str, provided_params: dict[str, Any]) -> tuple[str, str]:
    preset_def = get_preset(preset_id)
    if not preset_def:
        raise HTTPException(status_code=400, detail=f"Unknown preset: {preset_id}")

    src, _is_glob = dataset_source_path(dataset)

    final_params = _build_final_params(preset_def, provided_params)

    sql = preset_def["sql"].format(**final_params)

    # Some DuckDB introspection functions expect a path, not a relation.
    # Preserve legacy presets that use parquet_schema(dataset) / parquet_metadata(dataset).
    esc = _sql_escape_path(src)
    sql = sql.replace("parquet_schema(dataset)", f"parquet_schema('{esc}')")
    sql = sql.replace("parquet_metadata(dataset)", f"parquet_metadata('{esc}')")

    # Replace placeholder token `dataset` with a parquet reader for normal queries
    sql = sql.replace("dataset", f"read_parquet('{esc}')")

    return sql, src


# ============================================================
# UI mount
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


# Optional: silence favicon.ico 404 noise
@app.get("/ui/favicon.ico")
def favicon():
    return Response(status_code=204)


# ============================================================
# API
# ============================================================


@app.get("/api/version")
def api_version():
    return {
        "name": os.getenv("APP_NAME", "Analytics Workbench"),
        "version": os.getenv("APP_VERSION", "1.0.0"),
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

    status = "ok" if (duck_ok and datasets_ok and exports_ok and frontend_ok) else "degraded"

    return {
        "status": status,
        "checks": {
            "duckdb": {"ok": duck_ok, "error": duck_err},
            "datasets_dir": {"ok": datasets_ok, "path": str(DATASETS_DIR), "error": datasets_err},
            "exports_dir": {"ok": exports_ok, "path": str(EXPORTS_DIR), "error": exports_err},
            "frontend": {"ok": frontend_ok, "path": str(FRONTEND_DIR), "error": frontend_err},
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
    """Return cached dataset metadata from _meta.json, with a live fallback."""
    ds_dir = _dataset_dir(name)
    if not ds_dir.exists() or not ds_dir.is_dir():
        raise HTTPException(status_code=404, detail=f"Dataset not found: {name}")

    mode = _dataset_mode(name)

    # Source path (what read_parquet will use)
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
        _audit_log({"event": "meta", "status": "success", "dataset": name, "meta_source": "cached"})
        return out

    # Live fallback
    t0 = time.perf_counter()
    con = _connect()
    try:
        esc = _sql_escape_path(src)

        # Column count via parquet_schema (one row per column)
        try:
            col_count = int(con.execute(f"SELECT COUNT(*) FROM parquet_schema('{esc}')").fetchone()[0])
        except Exception:
            col_count = None

        # Row count via parquet_metadata (sum row groups)
        try:
            row_count = int(
                con.execute(
                    f"SELECT COALESCE(SUM(num_rows), 0) FROM parquet_metadata('{esc}')"
                ).fetchone()[0]
            )
        except Exception:
            row_count = None

        # File size
        file_size_bytes: int | None
        try:
            if mode == "reference":
                file_size_bytes = Path(src).stat().st_size
            else:
                # copy/glob: sum all parquet files in dataset folder
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
        _audit_log({"event": "meta", "status": "success", "dataset": name, "meta_source": "live", "elapsed_seconds": elapsed})
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


@app.get("/api/audit")
def api_audit(limit: int = Query(200, ge=1, le=5000)):
    """
    Returns last N audit events (newest first). Missing file -> empty list.
    """
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
    """Return column names and DuckDB types for the dataset."""
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
        _audit_log({"event": "schema", "status": "success", "dataset": dataset, "column_count": len(columns)})
        return {"dataset": dataset, "columns": columns}
    except Exception as e:
        _audit_log({"event": "schema", "status": "error", "dataset": dataset, "error": str(e)})
        raise
    finally:
        con.close()


@app.get("/api/preview")
def api_preview(dataset: str = Query(...), limit: int = Query(DEFAULT_PREVIEW_ROWS, ge=1)):
    """Return first N rows of a dataset (no preset involved)."""
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
        # Friendlier error when copy-mode dataset folder is empty
        if "No files found" in msg or "no files" in msg.lower():
            raise HTTPException(status_code=404, detail=f"No parquet files found for dataset: {dataset}") from e
        if "incompatible" in msg.lower() or "schema" in msg.lower():
            raise HTTPException(status_code=400, detail="Inconsistent schemas across parquet files in dataset") from e

        _audit_log({"event": "preview", "status": "error", "dataset": dataset, "error": msg})
        raise
    finally:
        con.close()


@app.get("/api/dialog/folder")
def api_dialog_folder():
    """
    Native Windows folder picker (Tkinter). Returns {"path": "..."} or {"path": ""} if cancelled.
    """
    try:
        import tkinter as tk  # noqa: WPS433
        from tkinter import filedialog  # noqa: WPS433

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
    params = _extract_dynamic_params(request, reserved={"dataset", "preset"}, threshold_fallback=threshold)

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
    params = _extract_dynamic_params(request, reserved={"dataset", "preset"}, threshold_fallback=threshold)

    logger.info("export requested | dataset=%s preset=%s params=%s", dataset, preset, params)
    t0 = time.perf_counter()

    try:
        sql, _src = _sql_for(dataset, preset, params)

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_preset = preset.replace("/", "_").replace("\\", "_")
        EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

        # Prefer XLSX
        out_xlsx = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.xlsx"
        out_csv = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.csv"

        con = _connect()
        try:
            # Export row limit protection (5A)
            try:
                rowcount = int(con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0])
            except Exception:
                rowcount = None
            if rowcount is not None and rowcount > MAX_EXPORT_ROWS:
                raise HTTPException(status_code=400, detail=f"Export too large: {rowcount} rows (limit {MAX_EXPORT_ROWS})")

            con.execute("CREATE OR REPLACE TEMP VIEW __export_view AS " + sql)

            try:
                out_str = str(out_xlsx.resolve()).replace("\\", "/")
                con.execute(f"COPY __export_view TO '{_sql_escape_path(out_str)}' (FORMAT XLSX, HEADER TRUE)")
                elapsed = round(time.perf_counter() - t0, 4)
                logger.info("export success | file=%s path=%s elapsed=%s", out_xlsx.name, out_xlsx, elapsed)

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
                    media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except Exception:
                # Fallback: CSV always works
                out_str = str(out_csv.resolve()).replace("\\", "/")
                con.execute(f"COPY __export_view TO '{_sql_escape_path(out_str)}' (FORMAT CSV, HEADER TRUE)")
                elapsed = round(time.perf_counter() - t0, 4)
                logger.info(
                    "export success (csv fallback) | file=%s path=%s elapsed=%s",
                    out_csv.name,
                    out_csv,
                    elapsed,
                )

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

    # Fast row counts via parquet metadata (no full scan)
    con = _connect()
    results: list[dict[str, Any]] = []
    try:
        for f in files:
            stat = f.stat()
            f_str = str(f.resolve()).replace("\\", "/")
            row_count: int | None = None
            try:
                # parquet_metadata provides num_rows per row-group
                row_count = int(
                    con.execute(
                        f"SELECT COALESCE(SUM(num_rows), 0) FROM parquet_metadata('{_sql_escape_path(f_str)}')"
                    ).fetchone()[0]
                )
            except Exception:
                row_count = None

            results.append(
                {
                    "path": str(f),
                    "name": f.name,
                    "size_bytes": stat.st_size,
                    "row_count": row_count,  # may be None if metadata failed
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
    t0 = time.perf_counter()
    logger.info(
        "register requested | dataset=%s parquet_path=%s mode=%s",
        req.dataset_name,
        req.parquet_path,
        req.mode,
    )

    src = Path(req.parquet_path).expanduser()
    if not src.exists() or not src.is_file():
        logger.warning("register failed | dataset=%s | reason=parquet path not found", req.dataset_name)

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

    ds_name = _safe_name(req.dataset_name)
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
        logger.warning("register failed | dataset=%s | reason=invalid mode=%s", req.dataset_name, req.mode)

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

    return {"dataset": ds_name, "storage": storage}


@app.post("/api/shutdown")
def api_shutdown(bg: BackgroundTasks):
    """
    Hard-stop the process so the app can be launched again immediately.
    Windows + PyInstaller + --noconsole: SIGTERM is not reliable, so we force exit.
    """
    logger.info("shutdown requested")

    def _stop():
        time.sleep(0.25)  # let the HTTP response flush
        os._exit(0)  # guaranteed process termination

    bg.add_task(_stop)
    return {"ok": True}