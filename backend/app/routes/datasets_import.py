from __future__ import annotations
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from pathlib import Path
import os
import shutil
import re

from app.paths import datasets_root

router = APIRouter(prefix="/api", tags=["datasets"])

class ScanRequest(BaseModel):
    path: str
    recursive: bool = False

class RegisterRequest(BaseModel):
    dataset_name: str
    parquet_path: str
    mode: str = "reference"  # "reference" or "copy"

def _safe_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return name[:64] if name else "Dataset"

@router.post("/datasets/scan")
def scan_for_parquet(req: ScanRequest):
    p = Path(req.path).expanduser()
    if not p.exists() or not p.is_dir():
        raise HTTPException(status_code=400, detail="Path must be an existing directory.")

    pattern = "**/*.parquet" if req.recursive else "*.parquet"
    results = []
    for f in p.glob(pattern):
        if f.is_file():
            stat = f.stat()
            results.append({
                "path": str(f),
                "name": f.name,
                "size_bytes": stat.st_size,
                "modified_ts": int(stat.st_mtime),
            })

    results.sort(key=lambda x: x["name"].lower())
    return {"count": len(results), "files": results}

@router.post("/datasets/register")
def register_dataset(req: RegisterRequest):
    src = Path(req.parquet_path).expanduser()
    if not src.exists() or not src.is_file():
        raise HTTPException(status_code=400, detail="Parquet path must be an existing file.")

    ds_name = _safe_name(req.dataset_name)
    ds_dir = datasets_root() / ds_name
    ds_dir.mkdir(parents=True, exist_ok=True)

    # Create a canonical filename inside dataset folder
    dest = ds_dir / src.name

    if req.mode == "copy":
        shutil.copy2(src, dest)
        storage = "copied"
    elif req.mode == "reference":
        # Store a pointer file instead of copying huge files
        pointer = ds_dir / "_reference.txt"
        pointer.write_text(str(src), encoding="utf-8")
        storage = "referenced"
    else:
        raise HTTPException(status_code=400, detail="mode must be 'copy' or 'reference'.")

    return {"dataset": ds_name, "storage": storage}
