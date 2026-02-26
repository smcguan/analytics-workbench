from __future__ import annotations

import os
import re
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from starlette.responses import FileResponse, Response

from app.presets.doge import PRESETS  # noqa: E402


app = FastAPI(title="Analytics Workbench")


# ============================================================
# Runtime paths (single source of truth)
# ============================================================
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


def _sql_for(dataset: str, preset_id: str, threshold: int | None) -> tuple[str, str]:
    preset_def = get_preset(preset_id)
    if not preset_def:
        raise HTTPException(status_code=400, detail=f"Unknown preset: {preset_id}")

    src, _is_glob = dataset_source_path(dataset)

    final_params = dict(preset_def.get("params", {}))
    if threshold is not None:
        final_params["threshold"] = threshold

    sql = preset_def["sql"].format(**final_params)
    # Replace placeholder token `dataset` with a parquet reader
    sql = sql.replace("dataset", f"read_parquet('{src}')")
    return sql, src


def _connect() -> duckdb.DuckDBPyConnection:
    # Fileless connection is fine for query/extract workloads
    return duckdb.connect()


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
    }


@app.get("/api/datasets")
def api_datasets():
    return {"datasets": list_datasets()}


@app.get("/api/presets")
def api_presets():
    return {
        "presets": [
            {"id": p["id"], "name": p["name"], "params": p.get("params", {})}
            for p in PRESETS
        ]
    }


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
    dataset: str = Query(...),
    preset: str = Query(...),
    threshold: int | None = None,
):
    t0 = time.perf_counter()
    sql, _src = _sql_for(dataset, preset, threshold)

    con = _connect()
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(200)

        rowcount = int(con.execute(f"SELECT COUNT(*) FROM ({sql}) t").fetchone()[0])
        elapsed = time.perf_counter() - t0

        preview = [dict(zip(cols, r)) for r in rows]
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
    dataset: str = Query(...),
    preset: str = Query(...),
    threshold: int | None = None,
):
    sql, _src = _sql_for(dataset, preset, threshold)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_preset = preset.replace("/", "_").replace("\\", "_")
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Prefer XLSX
    out_xlsx = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.xlsx"
    out_csv = EXPORTS_DIR / f"{dataset}_{safe_preset}_{ts}.csv"

    con = _connect()
    try:
        con.execute("CREATE OR REPLACE TEMP VIEW __export_view AS " + sql)

        try:
            out_str = str(out_xlsx.resolve()).replace("\\", "/")
            con.execute(f"COPY __export_view TO '{out_str}' (FORMAT XLSX, HEADER TRUE)")
            return FileResponse(
                path=str(out_xlsx),
                filename=out_xlsx.name,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception:
            # Fallback: CSV always works
            out_str = str(out_csv.resolve()).replace("\\", "/")
            con.execute(f"COPY __export_view TO '{out_str}' (FORMAT CSV, HEADER TRUE)")
            return FileResponse(
                path=str(out_csv),
                filename=out_csv.name,
                media_type="text/csv",
            )
    finally:
        con.close()


@app.post("/api/datasets/scan")
def scan_for_parquet(req: ScanRequest):
    p = Path(req.path).expanduser()
    if not p.exists() or not p.is_dir():
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
                    "row_count": row_count,  # may be None if metadata failed
                }
            )
    finally:
        con.close()

    return {"count": len(results), "files": results}


@app.post("/api/datasets/register")
def register_dataset(req: RegisterRequest):
    src = Path(req.parquet_path).expanduser()
    if not src.exists() or not src.is_file():
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
        return {"error": "mode must be 'copy' or 'reference'."}

    return {"dataset": ds_name, "storage": storage}

from fastapi import BackgroundTasks
import os
import time

@app.post("/api/shutdown")
def api_shutdown(bg: BackgroundTasks):
    """
    Hard-stop the process so the app can be launched again immediately.
    Windows + PyInstaller + --noconsole: SIGTERM is not reliable, so we force exit.
    """
    def _stop():
        time.sleep(0.25)  # let the HTTP response flush
        os._exit(0)       # guaranteed process termination

    bg.add_task(_stop)
    return {"ok": True}
