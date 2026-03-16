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
import re
import shutil
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
# Public service entrypoint
# -----------------------------------------------------------------------------


def import_dataset(
    uploaded_file: BinaryIO,
    original_filename: str,
    display_name: str | None = None,
    registered_root: str | Path = "data/datasets",
) -> DatasetImportResult:
    """
    Import a user-uploaded dataset into normalized Parquet storage.
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
    dataset_dir = Path(registered_root) / registered_name

    if dataset_dir.exists():
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
        convert_csv_to_parquet(source_upload_path, parquet_path)
    elif original_type == "tsv":
        # TSV uses the same path as CSV but with tab delimiter
        convert_tsv_to_parquet(source_upload_path, parquet_path)
    elif original_type == "xlsx":
        convert_xlsx_to_parquet(source_upload_path, parquet_path)
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
    """Write dataset metadata in a human-readable JSON format."""

    payload = asdict(metadata)
    payload["columns"] = [asdict(column) for column in metadata.columns]

    with destination.open("w", encoding="utf-8") as output_file:
        json.dump(payload, output_file, indent=2)


# -----------------------------------------------------------------------------
# Conversion handlers
# -----------------------------------------------------------------------------


def normalize_parquet(source_path: Path, parquet_path: Path) -> None:
    """
    Validate an uploaded Parquet file and rewrite it into canonical storage.
    """

    try:
        table = pq.read_table(source_path)
        if table.num_columns == 0:
            raise DatasetValidationError("Parquet file contains no columns.")

        pq.write_table(table, parquet_path)
    except DatasetValidationError:
        raise
    except Exception as exc:
        raise DatasetConversionError(f"Failed to normalize Parquet file: {exc}") from exc


def convert_csv_to_parquet(source_path: Path, parquet_path: Path) -> None:
    """Read CSV data and write it into canonical Parquet storage."""

    try:
        dataframe = pd.read_csv(source_path)
    except Exception as exc:
        raise DatasetValidationError(f"Failed to read CSV file: {exc}") from exc

    dataframe_to_parquet(dataframe, parquet_path, source_label="CSV")


def convert_tsv_to_parquet(source_path: Path, parquet_path: Path) -> None:
    """Read TSV data (tab-separated values) and write into canonical Parquet storage."""

    try:
        dataframe = pd.read_csv(source_path, sep="\t", encoding="utf-8", on_bad_lines="warn")
    except Exception as exc:
        raise DatasetValidationError(f"Failed to read TSV file: {exc}") from exc

    dataframe_to_parquet(dataframe, parquet_path, source_label="TSV")


def convert_xlsx_to_parquet(source_path: Path, parquet_path: Path) -> None:
    """
    Read the first worksheet from an Excel file and write canonical Parquet.
    """

    try:
        dataframe = pd.read_excel(source_path, sheet_name=0, engine="openpyxl")
    except Exception as exc:
        raise DatasetValidationError(f"Failed to read Excel file: {exc}") from exc

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
    Read row count and schema details from the normalized Parquet file.
    """

    try:
        table = pq.read_table(parquet_path)
    except Exception as exc:
        raise DatasetValidationError(f"Failed to inspect normalized Parquet file: {exc}") from exc

    columns = [
        DatasetColumn(name=field.name, type=str(field.type))
        for field in table.schema
    ]

    return table.num_rows, columns
