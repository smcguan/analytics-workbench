from __future__ import annotations

import json
import logging
import os
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
from starlette.responses import FileResponse, JSONResponse, Response

from app.presets.doge import PRESETS  # noqa: E402

logger = logging.getLogger("app")

app = FastAPI(title="Analytics Workbench")


# ============================================================
# Global exception handlers — structured error responses
# ============================================================
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # Preserve structured payloads (e.g. export abort dict from 5A).
    if isinstance(exc.detail, dict):
        content = {"error": exc.detail}
    else:
        content = {"error": {"type": "http_error", "message": str(exc.detail)}}
    return JSONResponse(status_code=exc.status_code, content=content)


@app.exception_handler(FileNotFoundError)
async def file_not_found_handler(request: Request, exc: FileNotFoundError):
    logger.warning("not found | path=%s", str(exc))
    return JSONResponse(
        status_code=404,
        content={"error": {"type": "not_found", "message": str(exc)}},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("unhandled exception | path=%s", request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": {"type": "internal_error", "message": "Internal server error"}},
    )


# ============================================================
# Runtime paths (single source of truth)
# ============================================================
def app_base_dir() -> Path:
    """
    Packaged: folder containing the EXE
    Dev: repo root (.../Analytics Workbench)
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

# ============================================================
# Query guardrails (single-point configuration)
# ============================================================
MAX_PREVIEW_ROWS = 200
MAX_EXPORT_ROWS = 2_000_000

# Startup confirmation — logged once at import time
logger.info(
    "app started | mode=%s | exports_dir=%s",
    "packaged" if getattr(sys, "frozen", False) else "dev",
    EXPORTS_DIR,
)


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


def get_preset(preset_id: str) -> dict[str, Any] | None:
    for p in PRESETS:
        if p.get("id") == preset_id:
            return p
    return None


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def _build_dataset_metadata(ds_name: str, parquet_path: Path) -> dict[str, Any]:
    """
    Collect metadata for a single parquet file.
    - Row count: parquet_metadata() (footer-only read)
    - Column count: DESCRIBE SELECT * FROM read_parquet(?) (no row scan)
    - Size: stat().st_size
    """
    p = parquet_path.resolve()
    p_str = str(p).replace("\\", "/")

    con = _connect()
    try:
        # Prefer bind parameters to avoid quoting issues
        row_count = int(
            con.execute(
                "SELECT COALESCE(SUM(num_rows), 0) FROM parquet_metadata(?)",
                [p_str],
            ).fetchone()[0]
        )

        col_rows = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)",
            [p_str],
        ).fetchall()
        column_count = len(col_rows)
    finally:
        con.close()

    return {
        "dataset": ds_name,
        "source_path": str(p),
        "row_count": row_count,
        "column_count": column_count,
        "file_size_bytes": p.stat().st_size,
        "last_scanned": datetime.now().isoformat(timespec="seconds"),
    }


def _normalize_param_schema(raw_params: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """
    Supports:
      - legacy defaults dict: {"threshold": 100000000}
      - schema dict: {"threshold": {"type":"number","default":100000000,...}}
    Returns schema dict keyed by param name.
    """
    raw_params = raw_params or {}
    schema: dict[str, dict[str, Any]] = {}

    for k, v in raw_params.items():
        if isinstance(v, dict):
            schema[k] = v
        else:
            schema[k] = {
                "type": "number" if isinstance(v, (int, float)) else "string",
                "label": k,
                "default": v,
                "required": False,
            }

    return schema


def _coerce_param(name: str, meta: dict[str, Any], value: Any) -> Any:
    ptype = (meta.get("type") or "string").lower()

    if value is None:
        return None

    if ptype == "number":
        if isinstance(value, (int, float)):
            return value
        s = str(value).strip()
        try:
            if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                return int(s)
            return float(s)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Parameter '{name}' must be a number") from e

    if ptype == "boolean":
        if isinstance(value, bool):
            return value
        s = str(value).strip().lower()
        if s in ("true", "1", "yes", "y", "on"):
            return True
        if s in ("false", "0", "no", "n", "off"):
            return False
        raise HTTPException(status_code=400, detail=f"Parameter '{name}' must be a boolean")

    return str(value)


def _build_final_params(preset_id: str, preset_def: dict[str, Any], provided_params: dict[str, Any]) -> dict[str, Any]:
    schema = _normalize_param_schema(preset_def.get("params", {}) or {})

    unknown = [k for k in provided_params.keys() if k not in schema]
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown parameter(s) for preset '{preset_id}': {', '.join(sorted(unknown))}",
        )

    final_params: dict[str, Any] = {}
    for name, meta in schema.items():
        required = bool(meta.get("required", False))
        default = meta.get("default", None)

        has_value = name in provided_params and provided_params[name] is not None and str(provided_params[name]) != ""
        raw_val = provided_params[name] if has_value else default

        if (raw_val is None or str(raw_val) == "") and required:
            raise HTTPException(status_code=400, detail=f"Missing required parameter: {name}")

        if raw_val is None:
            final_params[name] = None
            continue

        final_params[name] = _coerce_param(name, meta, raw_val)

    return final_params


def _sql_for(dataset: str, preset_id: str, provided_params: dict[str, Any] | None = None) -> tuple[str, str]:
    preset_def = get_preset(preset_id)
    if not preset_def:
        raise HTTPException(status_code=400, detail=f"Unknown preset: {preset_id}")

    src, _is_glob = dataset_source_path(dataset)

    provided_params = provided_params or {}
    final_params = _build_final_params(preset_id, preset_def, provided_params)

    try:
        sql = preset_def["sql"].format(**final_params)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Preset SQL missing parameter: {e}") from e

    sql = sql.replace("dataset", f"read_parquet('{src}')")
    return sql, src


def _extract_dynamic_params(request: Request, *, reserved: set[str], threshold_fallback: int | None) -> dict[str, Any]:
    provided: dict[str, Any] = dict(request.query_params)
    for k in reserved:
        provided.pop(k, None)

    if threshold_fallback is not None and "threshold" not in provided:
        provided["threshold"] = threshold_fallback

    return provided


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
    }


@app.get("/api/datasets")
def api_datasets():
    return {"datasets": list_datasets()}


@app.get("/api/presets")
def api_presets():
    return {
        "presets": [{"id": p["id"], "name": p["name"], "params": p.get("params", {})} for p in PRESETS]
    }


@app.get("/api/schema")
def api_schema(dataset: str = Query(...)):
    logger.info("schema requested | dataset=%s", dataset)
    t0 = time.perf_counter()

    src, _is_glob = dataset_source_path(dataset)

    con = _connect()
    try:
        cur = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [src])
        rows = cur.fetchall()
        columns = [{"name": r[0], "type": str(r[1])} for r in rows]
    finally:
        con.close()

    elapsed = round(time.perf_counter() - t0, 4)
    logger.info("schema success | dataset=%s columns=%d elapsed=%s", dataset, len(columns), elapsed)
    return {"dataset": dataset, "columns": columns}


@app.get("/api/dialog/folder")
def api_dialog_folder():
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
    provided_params = _extract_dynamic_params(
        request,
        reserved={"dataset", "preset"},
        threshold_fallback=threshold,
    )

    logger.info("query requested | dataset=%s preset=%s params=%s", dataset, preset, provided_params)
    t0 = time.perf_counter()

    sql, _src = _sql_for(dataset, preset, provided_params)
    limited_sql = f"SELECT * FROM ({sql}) t LIMIT {MAX_PREVIEW_ROWS}"

    con = _connect()
    try:
        cur = con.execute(limited_sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(MAX_PREVIEW_ROWS)

        rowcount = int(con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0])
        elapsed = time.perf_counter() - t0

        preview = [dict(zip(cols, r)) for r in rows]
        logger.info(
            "query success | dataset=%s preset=%s params=%s preview_limit=%d rows_returned=%d rowcount=%d elapsed=%s",
            dataset,
            preset,
            provided_params,
            MAX_PREVIEW_ROWS,
            len(preview),
            rowcount,
            round(elapsed, 4),
        )
        return {
            "columns": cols,
            "rows": preview,
            "rowcount": rowcount,
            "elapsed_seconds": round(elapsed, 4),
        }
    finally:
        con.close()


@app.get("/api/export")
def api_export(
    request: Request,
    dataset: str = Query(...),
    preset: str = Query(...),
    threshold: int | None = None,
):
    provided_params = _extract_dynamic_params(
        request,
        reserved={"dataset", "preset"},
        threshold_fallback=threshold,
    )

    logger.info("export requested | dataset=%s preset=%s params=%s", dataset, preset, provided_params)
    t0 = time.perf_counter()

    sql, _src = _sql_for(dataset, preset, provided_params)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_preset = preset.replace("/", "_").replace("\\", "_")
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    out_xlsx = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.xlsx"
    out_csv = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.csv"

    con = _connect()
    try:
        estimated_rows = int(con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0])
        if estimated_rows > MAX_EXPORT_ROWS:
            elapsed = round(time.perf_counter() - t0, 4)
            logger.warning(
                "export aborted | dataset=%s preset=%s params=%s estimated_rows=%d max_export_rows=%d elapsed=%s",
                dataset,
                preset,
                provided_params,
                estimated_rows,
                MAX_EXPORT_ROWS,
                elapsed,
            )
            raise HTTPException(
                status_code=400,
                detail={
                    "type": "export_limit_exceeded",
                    "message": "Export exceeds maximum allowed rows",
                    "max_export_rows": MAX_EXPORT_ROWS,
                    "estimated_rows": estimated_rows,
                },
            )

        con.execute("CREATE OR REPLACE TEMP VIEW __export_view AS " + sql)

        try:
            out_str = str(out_xlsx.resolve()).replace("\\", "/")
            con.execute(f"COPY __export_view TO '{out_str}' (FORMAT XLSX, HEADER TRUE)")
            elapsed = round(time.perf_counter() - t0, 4)
            logger.info(
                "export success | dataset=%s preset=%s params=%s rows_exported=%d file=%s path=%s elapsed=%s",
                dataset,
                preset,
                provided_params,
                estimated_rows,
                out_xlsx.name,
                out_xlsx,
                elapsed,
            )
            return FileResponse(
                path=str(out_xlsx),
                filename=out_xlsx.name,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception:
            out_str = str(out_csv.resolve()).replace("\\", "/")
            con.execute(f"COPY __export_view TO '{out_str}' (FORMAT CSV, HEADER TRUE)")
            elapsed = round(time.perf_counter() - t0, 4)
            logger.info(
                "export success (csv fallback) | dataset=%s preset=%s params=%s rows_exported=%d file=%s path=%s elapsed=%s",
                dataset,
                preset,
                provided_params,
                estimated_rows,
                out_csv.name,
                out_csv,
                elapsed,
            )
            return FileResponse(
                path=str(out_csv),
                filename=out_csv.name,
                media_type="text/csv",
            )
    finally:
        con.close()


@app.post("/api/datasets/scan")
def scan_for_parquet(req: ScanRequest):
    t0 = time.perf_counter()
    logger.info("scan requested | path=%s recursive=%s", req.path, req.recursive)
    p = Path(req.path).expanduser()
    if not p.exists() or not p.is_dir():
        logger.warning("scan failed | path=%s | reason=not a directory", req.path)
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
                        f"SELECT COALESCE(SUM(num_rows), 0) FROM parquet_metadata('{f_str}')"
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
    return {"count": len(results), "files": results}


@app.post("/api/datasets/register")
def register_dataset(req: RegisterRequest):
    t0 = time.perf_counter()
    logger.info("register requested | dataset=%s parquet_path=%s mode=%s", req.dataset_name, req.parquet_path, req.mode)
    src = Path(req.parquet_path).expanduser()
    if not src.exists() or not src.is_file():
        logger.warning("register failed | dataset=%s | reason=parquet path not found", req.dataset_name)
        return {"error": "Parquet path must be an existing file."}

    ds_name = _safe_name(req.dataset_name)
    ds_dir = DATASETS_DIR / ds_name
    ds_dir.mkdir(parents=True, exist_ok=True)

    stored_path = src
    if req.mode == "copy":
        dest = ds_dir / src.name
        shutil.copy2(src, dest)
        storage = "copied"
        stored_path = dest
    elif req.mode == "reference":
        pointer = ds_dir / "_reference.txt"
        pointer.write_text(str(src), encoding="utf-8")
        storage = "referenced"
        stored_path = src
    else:
        logger.warning("register failed | dataset=%s | reason=invalid mode=%s", req.dataset_name, req.mode)
        return {"error": "mode must be 'copy' or 'reference'."}

    elapsed = round(time.perf_counter() - t0, 4)
    logger.info("register success | dataset=%s storage=%s elapsed=%s", ds_name, storage, elapsed)

    # Build and persist dataset metadata (non-fatal)
    logger.info("metadata build start | dataset=%s", ds_name)
    try:
        meta = _build_dataset_metadata(ds_name, stored_path)
        meta_path = ds_dir / "_meta.json"
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(
            "metadata build success | dataset=%s rows=%d cols=%d size_bytes=%d",
            ds_name,
            int(meta.get("row_count", 0)),
            int(meta.get("column_count", 0)),
            int(meta.get("file_size_bytes", 0)),
        )
    except Exception:
        logger.exception("metadata build failed | dataset=%s", ds_name)

    return {"dataset": ds_name, "storage": storage}


@app.post("/api/shutdown")
def api_shutdown(bg: BackgroundTasks):
    logger.info("shutdown requested")

    def _stop():
        time.sleep(0.25)
        os._exit(0)

    bg.add_task(_stop)
    return {"ok": True}