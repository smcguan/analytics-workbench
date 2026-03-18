"""
File: services/dataset_import.py

Purpose
-------
Provide a single, well-documented import pipeline for Analytics Workbench datasets.

This module accepts user-uploaded dataset files, detects the supported file type,
normalizes the dataset into the project's canonical internal Parquet format, writes
standard metadata, and returns a structured result that can be used by the existing
registration flow.

Responsibilities
----------------
- detect supported dataset file types
- validate uploaded files before import
- import Parquet files directly into normalized storage
- convert CSV files into normalized Parquet storage
- convert Excel (.xlsx) files into normalized Parquet storage
- capture dataset metadata needed by downstream endpoints
- provide a stable service boundary for the registration endpoint

Execution Flow
--------------
Upload -> validate file -> detect type -> derive dataset name -> create dataset directory ->
convert or normalize to Parquet -> inspect schema/stats -> write metadata ->
return import result for registration

Important Notes
---------------
- Parquet is the canonical internal dataset format for Analytics Workbench.
- CSV and Excel uploads are converted automatically into Parquet.
- This module currently supports .parquet, .csv, .tsv, and .xlsx only.
- Excel import intentionally reads the first worksheet only for Milestone 3.
- Imported datasets are written into the normal app dataset directory structure.
- Swagger placeholder values like "string" are treated as empty input.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# -----------------------------------------------------------------------------
# Data models
# -----------------------------------------------------------------------------


@dataclass
class DatasetColumn:
    """Represents a single dataset column in persisted metadata."""

    name: str
    type: str


@dataclass
class DatasetImportMetadata:
    """Operational metadata written alongside the imported dataset."""

    dataset_id: str
    display_name: str
    registered_name: str
    original_filename: str
    original_type: str
    parquet_path: str
    row_count: int
    column_count: int
    columns: list[DatasetColumn]
    created_at: str


@dataclass
class DatasetImportResult:
    """Return value for callers that need both paths and metadata."""

    dataset_id: str
    dataset_dir: str
    parquet_path: str
    metadata_path: str
    metadata: DatasetImportMetadata


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------


class DatasetImportError(Exception):
    """Base error for all dataset import failures."""


class UnsupportedDatasetTypeError(DatasetImportError):
    """Raised when the uploaded file type is not supported."""


class DatasetValidationError(DatasetImportError):
    """Raised when a file is invalid or unreadable."""


class DatasetConversionError(DatasetImportError):
    """Raised when conversion into canonical Parquet fails."""


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------


def _rmtree_robust(path: Path) -> None:
    """
    Remove a directory tree, handling Windows-specific failure modes.

    See the same helper in main.py for full rationale. Duplicated here
    so dataset_import.py stays self-contained with no circular imports.
    """
    def _on_error(func, failed_path, exc_info):
        try:
            os.chmod(failed_path, stat.S_IWRITE)
            func(failed_path)
        except Exception:
            pass

    last_err: Exception | None = None
    for attempt in range(3):
        try:
            shutil.rmtree(path, onerror=_on_error)
            return
        except PermissionError as exc:
            last_err = exc
            if attempt < 2:
                time.sleep(0.5)

    if last_err:
        raise last_err


# -----------------------------------------------------------------------------
# Public service entrypoint
# -----------------------------------------------------------------------------


def import_dataset(
    uploaded_file: BinaryIO,
    original_filename: str,
    display_name: str | None = None,
    registered_root: str | Path = "data/datasets",
    overwrite: bool = False,
    strip_trailing_special_chars: bool = False,
) -> DatasetImportResult:
    """
    Import a user-uploaded dataset into normalized Parquet storage.

    overwrite=True: if the dataset directory already exists, remove it and
    replace it. Used by the frontend Refresh → re-import workflow so users
    can bring in the same file again without renaming.

    strip_trailing_special_chars=True: after conversion, strip trailing
    non-alphanumeric characters (e.g. asterisks, daggers) from every string
    column in the dataset.  CMS and other government datasets routinely append
    footnote markers to values such as "Stelara*" or "12345†".  These markers
    cause exact-match SQL filters to miss rows silently.

    This option is opt-in and defaults to False.  It is not applied to
    Parquet source files that are copied verbatim (only CSV / TSV / Excel
    imports go through a DataFrame conversion step where stripping is safe).
    """

    # Step 1: Validate the most basic upload assumptions before touching disk.
    # This catches empty filenames and unsupported types early.
    if not original_filename or not original_filename.strip():
        raise DatasetValidationError("Uploaded file must include a filename.")

    original_type = detect_file_type(original_filename)

    # Step 2: Derive dataset naming.
    # display_name is user-facing and can remain readable.
    # registered_name is SQL-safe and also becomes the dataset folder name so
    # the rest of the application can discover imported datasets naturally.
    resolved_display_name = normalize_display_name(display_name, original_filename)
    registered_name = make_registered_name(resolved_display_name)

    # Use the registered dataset name as both the visible dataset identifier
    # and the folder name on disk. This keeps imports aligned with the app's
    # existing dataset discovery logic.
    dataset_id = registered_name

    # Step 3: Create the normalized storage directory for this dataset.
    # Imported datasets should live under a human-readable dataset folder name,
    # not an opaque generated ID.
    dataset_dir = Path(registered_root).resolve() / registered_name

    if dataset_dir.exists():
        if overwrite:
            # Remove the existing dataset directory so we can replace it cleanly.
            # _rmtree_robust handles Windows read-only flags and brief file locks.
            _rmtree_robust(dataset_dir)
        else:
            raise DatasetValidationError(
                f"Dataset '{registered_name}' already exists. Choose a different dataset name."
            )

    dataset_dir.mkdir(parents=True, exist_ok=False)

    source_upload_path = dataset_dir / f"upload.{original_type}"
    parquet_path = dataset_dir / "source.parquet"
    metadata_path = dataset_dir / "metadata.json"

    # Step 4: Persist the uploaded bytes first.
    # Saving the uploaded source file before conversion makes failures easier to
    # reproduce locally and gives us a stable handoff between pipeline stages.
    write_uploaded_file(uploaded_file, source_upload_path)

    # Step 5: Normalize the uploaded file into the canonical Parquet artifact.
    if original_type == "parquet":
        normalize_parquet(source_upload_path, parquet_path)
    elif original_type == "csv":
        convert_csv_to_parquet(source_upload_path, parquet_path,
                               strip_trailing_special_chars=strip_trailing_special_chars)
    elif original_type == "tsv":
        # TSV uses the same path as CSV but with tab delimiter
        convert_tsv_to_parquet(source_upload_path, parquet_path,
                               strip_trailing_special_chars=strip_trailing_special_chars)
    elif original_type == "xlsx":
        convert_xlsx_to_parquet(source_upload_path, parquet_path,
                                strip_trailing_special_chars=strip_trailing_special_chars)
    else:
        raise UnsupportedDatasetTypeError(f"Unsupported dataset type: {original_type}")

    # Step 6: Inspect the normalized Parquet file.
    # Downstream endpoints rely on row counts and schema information, so we
    # derive them once here and persist them into metadata.
    row_count, columns = inspect_parquet(parquet_path)

    if row_count <= 0:
        raise DatasetValidationError("Imported dataset is empty.")

    metadata = DatasetImportMetadata(
        dataset_id=dataset_id,
        display_name=resolved_display_name,
        registered_name=registered_name,
        original_filename=original_filename,
        original_type=original_type,
        parquet_path=str(parquet_path),
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
        created_at=utc_now_iso(),
    )

    # Step 7: Persist metadata next to the canonical dataset artifact.
    write_metadata(metadata, metadata_path)

    return DatasetImportResult(
        dataset_id=dataset_id,
        dataset_dir=str(dataset_dir),
        parquet_path=str(parquet_path),
        metadata_path=str(metadata_path),
        metadata=metadata,
    )


# -----------------------------------------------------------------------------
# File type detection and naming helpers
# -----------------------------------------------------------------------------


def detect_file_type(filename: str) -> str:
    """
    Detect the supported dataset type from the filename extension.

    Supported types and their canonical internal label:
        .parquet        -> "parquet"
        .csv            -> "csv"
        .tsv            -> "tsv"   (tab-separated values)
        .xlsx / .xls    -> "xlsx"  (both old and new Excel formats)
    """

    suffix = Path(filename).suffix.lower()

    if suffix == ".parquet":
        return "parquet"
    if suffix == ".csv":
        return "csv"
    if suffix == ".tsv":
        return "tsv"
    if suffix in (".xlsx", ".xls"):
        return "xlsx"

    raise UnsupportedDatasetTypeError(
        "Unsupported file type. Supported formats: .parquet, .csv, .tsv, .xlsx"
    )


def derive_display_name(filename: str) -> str:
    """Create a readable dataset name from the original filename."""

    return Path(filename).stem.strip() or "dataset"


def normalize_display_name(display_name: str | None, original_filename: str) -> str:
    """
    Resolve the user-facing dataset name.

    Swagger often submits the literal placeholder value "string" for optional
    form fields. Treat that as empty input and fall back to the filename.
    """

    if display_name is None:
        return derive_display_name(original_filename)

    cleaned = display_name.strip()

    if not cleaned or cleaned.lower() == "string":
        return derive_display_name(original_filename)

    return cleaned


def make_registered_name(name: str) -> str:
    """
    Convert a display name into a SQL-safe registered dataset name.
    """

    normalized = name.strip().lower()
    normalized = re.sub(r"[^a-z0-9_\s-]", "", normalized)
    normalized = re.sub(r"[\s-]+", "_", normalized)
    normalized = normalized.strip("_")

    if not normalized:
        normalized = "dataset"

    if normalized[0].isdigit():
        normalized = f"dataset_{normalized}"

    return normalized


def utc_now_iso() -> str:
    """Return a consistent UTC timestamp for metadata persistence."""

    return datetime.now(timezone.utc).isoformat()


# -----------------------------------------------------------------------------
# Storage helpers
# -----------------------------------------------------------------------------


def write_uploaded_file(uploaded_file: BinaryIO, destination: Path) -> None:
    """
    Persist the uploaded file object to disk.
    """

    try:
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(0)

        with destination.open("wb") as output_file:
            shutil.copyfileobj(uploaded_file, output_file)
    except Exception as exc:
        raise DatasetValidationError(f"Failed to save uploaded file: {exc}") from exc

    if not destination.exists() or destination.stat().st_size == 0:
        raise DatasetValidationError("Uploaded file is empty.")


def write_metadata(metadata: DatasetImportMetadata, destination: Path) -> None:
    """
    Write dataset metadata in a human-readable JSON format.

    Writes two files:
    1. metadata.json  — full import pipeline metadata (primary)
    2. _meta.json     — lightweight summary in the format _dataset_meta_summary()
                        reads, so profile/schema/preview work immediately after
                        import without requiring a live DuckDB computation.
    """

    payload = asdict(metadata)
    payload["columns"] = [asdict(column) for column in metadata.columns]

    with destination.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2)

    # Write _meta.json alongside so the main app's caching layer picks it up.
    meta_path = destination.parent / "_meta.json"
    meta_summary = {
        "row_count": metadata.row_count,
        "column_count": metadata.column_count,
        "file_size_bytes": None,  # filled in at read time from actual parquet size
        "last_scanned": metadata.created_at,
    }
    try:
        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta_summary, f, indent=2)
    except Exception:
        pass  # non-fatal — live computation will still work as fallback


# -----------------------------------------------------------------------------
# Conversion handlers
# -----------------------------------------------------------------------------


def normalize_parquet(source_path: Path, parquet_path: Path) -> None:
    """
    Validate an uploaded Parquet file and copy it into canonical storage.

    PERFORMANCE NOTE
    ----------------
    We intentionally do NOT read the entire file into memory.
    For large Parquet files (100M+ rows) a full pq.read_table() + pq.write_table()
    cycle can take several minutes and use many GB of RAM.

    Instead we:
    1. Read only the schema (file footer only — instant for any file size)
    2. Validate it has columns
    3. Copy the raw bytes with shutil.copy2 (OS-level copy, no decompression)

    This reduces a 5-minute import to a few seconds for large files.
    The file is already valid Parquet — no conversion or normalization needed.
    """

    try:
        # Read only the schema — reads the Parquet file footer, not the data.
        # This is near-instant even for 220M row files.
        schema = pq.read_schema(source_path)
        if len(schema) == 0:
            raise DatasetValidationError("Parquet file contains no columns.")

    except DatasetValidationError:
        raise
    except Exception as exc:
        raise DatasetConversionError(
            f"Failed to read Parquet file schema: {exc}"
        ) from exc

    # Copy raw bytes — no decompression, no row iteration, no memory allocation.
    try:
        shutil.copy2(source_path, parquet_path)
    except Exception as exc:
        raise DatasetConversionError(
            f"Failed to copy Parquet file to storage: {exc}"
        ) from exc


def _strip_trailing_special_chars_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip trailing non-alphanumeric characters from every string column.

    CMS and other government datasets often append footnote markers to values
    (e.g. "Stelara*", "12,345†", "Denosumab**").  These trailing characters
    cause exact-match SQL filters to miss rows silently because the stored
    value does not equal the user's search term.

    Only object-dtype columns are modified.  Numeric columns are unaffected.
    NaN / None values pass through unchanged.
    """
    _trailing_special = re.compile(r"[^A-Za-z0-9]+$")
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda v: _trailing_special.sub("", v) if isinstance(v, str) else v
            )
    return df


def convert_csv_to_parquet(
    source_path: Path,
    parquet_path: Path,
    strip_trailing_special_chars: bool = False,
) -> None:
    """Read CSV data and write it into canonical Parquet storage."""

    try:
        dataframe = pd.read_csv(source_path)
    except Exception as exc:
        raise DatasetValidationError(f"Failed to read CSV file: {exc}") from exc

    if strip_trailing_special_chars:
        dataframe = _strip_trailing_special_chars_from_df(dataframe)

    dataframe_to_parquet(dataframe, parquet_path, source_label="CSV")


def convert_tsv_to_parquet(
    source_path: Path,
    parquet_path: Path,
    strip_trailing_special_chars: bool = False,
) -> None:
    """Read TSV data (tab-separated values) and write into canonical Parquet storage."""

    # on_bad_lines="warn" requires pandas >= 1.3.
    # Fall back to the older error_bad_lines=False for compatibility.
    try:
        dataframe = pd.read_csv(
            source_path,
            sep="\t",
            encoding="utf-8",
            on_bad_lines="warn",
        )
    except TypeError:
        # Older pandas — use legacy parameter names
        try:
            dataframe = pd.read_csv(
                source_path,
                sep="\t",
                encoding="utf-8",
                error_bad_lines=False,
                warn_bad_lines=True,
            )
        except Exception as exc:
            raise DatasetValidationError(f"Failed to read TSV file: {exc}") from exc
    except Exception as exc:
        raise DatasetValidationError(f"Failed to read TSV file: {exc}") from exc

    if strip_trailing_special_chars:
        dataframe = _strip_trailing_special_chars_from_df(dataframe)

    dataframe_to_parquet(dataframe, parquet_path, source_label="TSV")


def convert_xlsx_to_parquet(
    source_path: Path,
    parquet_path: Path,
    strip_trailing_special_chars: bool = False,
) -> None:
    """
    Read the first worksheet from an Excel file and write canonical Parquet.
    """

    try:
        dataframe = pd.read_excel(source_path, sheet_name=0, engine="openpyxl")
    except Exception as exc:
        raise DatasetValidationError(f"Failed to read Excel file: {exc}") from exc

    if strip_trailing_special_chars:
        dataframe = _strip_trailing_special_chars_from_df(dataframe)

    dataframe_to_parquet(dataframe, parquet_path, source_label="Excel")


def dataframe_to_parquet(dataframe: pd.DataFrame, parquet_path: Path, source_label: str) -> None:
    """
    Validate a DataFrame and persist it as Parquet.
    """

    if dataframe is None or dataframe.empty:
        raise DatasetValidationError(f"{source_label} dataset is empty.")

    if len(dataframe.columns) == 0:
        raise DatasetValidationError(f"{source_label} dataset contains no columns.")

    try:
        table = pa.Table.from_pandas(dataframe, preserve_index=False)
        pq.write_table(table, parquet_path)
    except Exception as exc:
        raise DatasetConversionError(f"Failed to convert {source_label} dataset to Parquet: {exc}") from exc


# -----------------------------------------------------------------------------
# Metadata inspection helpers
# -----------------------------------------------------------------------------


def inspect_parquet(parquet_path: Path) -> tuple[int, list[DatasetColumn]]:
    """
    Read row count and schema from the Parquet file footer.

    PERFORMANCE NOTE
    ----------------
    pq.read_metadata() reads only the Parquet file footer — a few KB regardless
    of how many rows the file contains. This is instant for any file size.

    pq.read_table() would load the entire file into memory — avoid for large files.
    """

    try:
        # Read file-level metadata from footer only (no row data loaded)
        meta = pq.read_metadata(parquet_path)
        schema = pq.read_schema(parquet_path)
    except Exception as exc:
        raise DatasetValidationError(
            f"Failed to inspect Parquet file: {exc}"
        ) from exc

    # Row count is stored in the footer per row group — sum them.
    row_count = sum(
        meta.row_group(i).num_rows for i in range(meta.num_row_groups)
    )

    columns = [
        DatasetColumn(name=schema.field(i).name, type=str(schema.field(i).type))
        for i in range(len(schema))
    ]

    return row_count, columns


# -----------------------------------------------------------------------------
# Reference table import
# -----------------------------------------------------------------------------


@dataclass
class ReferenceImportResult:
    """Return value for the lightweight reference table import pipeline."""

    reference_name: str
    reference_dir: str
    parquet_path: str
    columns: list[DatasetColumn]
    row_count: int


def import_reference_table(
    uploaded_file: BinaryIO,
    original_filename: str,
    display_name: str | None = None,
    registered_root: str | Path = "data/references",
    overwrite: bool = True,
) -> ReferenceImportResult:
    """
    Import a small reference/lookup table into Parquet storage.

    This is a lightweight version of import_dataset designed for small CSV/TSV/
    Excel files used as JOIN targets (e.g. IRA exclusion lists, category
    mappings, manufacturer lists).

    Key differences from import_dataset:
    - Always overwrites (one reference table at a time)
    - No profiling, no dataset_context.json, no insights
    - Writes only _meta.json with column names/types and row count
    - No Parquet source support (reference tables are always small files)
    """
    if not original_filename or not original_filename.strip():
        raise DatasetValidationError("Uploaded file must include a filename.")

    original_type = detect_file_type(original_filename)

    resolved_display_name = normalize_display_name(display_name, original_filename)
    registered_name = make_registered_name(resolved_display_name)

    ref_dir = Path(registered_root).resolve() / registered_name

    if ref_dir.exists() and overwrite:
        _rmtree_robust(ref_dir)

    ref_dir.mkdir(parents=True, exist_ok=True)

    source_upload_path = ref_dir / f"upload.{original_type}"
    parquet_path = ref_dir / "source.parquet"

    write_uploaded_file(uploaded_file, source_upload_path)

    if original_type == "parquet":
        normalize_parquet(source_upload_path, parquet_path)
    elif original_type == "csv":
        convert_csv_to_parquet(source_upload_path, parquet_path)
    elif original_type == "tsv":
        convert_tsv_to_parquet(source_upload_path, parquet_path)
    elif original_type == "xlsx":
        convert_xlsx_to_parquet(source_upload_path, parquet_path)
    else:
        raise UnsupportedDatasetTypeError(f"Unsupported file type: {original_type}")

    row_count, columns = inspect_parquet(parquet_path)

    if row_count <= 0:
        raise DatasetValidationError("Reference table is empty.")

    # Write minimal _meta.json — no full metadata.json needed
    meta_path = ref_dir / "_meta.json"
    meta_summary = {
        "reference_name": registered_name,
        "original_filename": original_filename,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": [{"name": c.name, "type": c.type} for c in columns],
        "created_at": utc_now_iso(),
    }
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump(meta_summary, f, indent=2)

    return ReferenceImportResult(
        reference_name=registered_name,
        reference_dir=str(ref_dir),
        parquet_path=str(parquet_path),
        columns=columns,
        row_count=row_count,
    )
