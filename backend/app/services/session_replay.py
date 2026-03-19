"""
File: services/session_replay.py

Purpose
-------
Replay engine for Analytics Workbench session files.

This module re-executes replayable events (query_run, reference_load,
reference_delete) from a saved session JSON file and compares actual
results against recorded values and optional baselines.

Responsibilities
----------------
- list available session JSON files
- load and parse session JSON
- check schema requirements against live datasets
- replay replayable events via DuckDB
- compare row counts against recorded values and baselines
- annotate session files with baseline expectations
"""

from __future__ import annotations

import io
import json
import logging
import re
import shutil
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ReplayStepResult:
    event_index: int
    event_type: str
    status: str          # "pass", "fail", "skip", "error"
    message: str
    expected_row_count: int | None = None
    actual_row_count: int | None = None
    expected_columns: list[str] | None = None
    actual_columns: list[str] | None = None
    elapsed_seconds: float = 0.0
    error_detail: str | None = None


@dataclass
class ReplayReport:
    session_id: str
    replay_id: str           # new UUID
    replayed_at: str         # ISO
    session_file: str
    schema_check: str        # "pass", "fail", "skipped"
    total_steps: int
    passed: int
    failed: int
    skipped: int
    errors: int
    schema_mismatches: list[dict] | None = None
    steps: list[ReplayStepResult] = field(default_factory=list)
    overall_status: str = "pass"  # "pass" if failed==0 and errors==0


# ---------------------------------------------------------------------------
# Event types that get replayed vs skipped
# ---------------------------------------------------------------------------

_REPLAYABLE_EVENTS = {"query_run", "reference_load", "reference_delete"}

_SKIPPED_EVENTS = {
    "session_start", "session_end", "dataset_import", "dataset_delete",
    "query_save", "export", "passport_export", "result_passport",
    "insights_generated", "suggestions_generated", "ai_sql_generated",
}


# ---------------------------------------------------------------------------
# SQL rewriting (simplified version of main.py's _rewrite_sql_dataset_reference)
# ---------------------------------------------------------------------------

def _sql_escape_path(p: str) -> str:
    """Escape single quotes before embedding a path into SQL."""
    return p.replace("'", "''")


def _rewrite_sql_for_replay(
    sql: str,
    dataset_name: str,
    dataset_parquet_path: str,
    reference_parquet_path: str | None = None,
    reference_name: str | None = None,
) -> str:
    """
    Rewrite FROM dataset / JOIN reference to read_parquet(...) for replay.

    Simplified version of main.py's _rewrite_sql_dataset_reference.
    Does NOT raise on missing dataset match — returns SQL as-is if no
    rewrite target is found (the SQL execution will fail naturally).
    """
    rewritten = sql
    ds_escaped = _sql_escape_path(dataset_parquet_path)
    ds_parquet_sql = f"read_parquet('{ds_escaped}')"

    _SQL_KW = (
        r'on|where|join|left|right|inner|outer|cross|full|natural|'
        r'group|order|limit|having|union|using|select|from|set|into|'
        r'intersect|except|window|qualify|fetch|offset|returning|when'
    )

    # Rewrite dataset references
    identifiers = ["dataset", dataset_name]
    for ident in identifiers:
        if not ident:
            continue
        pattern = re.compile(
            rf'(?i)\b(from|join)\s+(")?{re.escape(ident)}(")?\b'
            rf'(\s+as\s+\w+|\s+(?!{_SQL_KW}\b)\w+)?'
        )

        def _repl(match: re.Match[str], _ident=ident) -> str:
            keyword = match.group(1)
            existing_alias = match.group(4)
            if existing_alias:
                alias = existing_alias.strip().split()[-1]
            else:
                alias = _ident
            return f"{keyword} {ds_parquet_sql} AS {alias}"

        rewritten = pattern.sub(_repl, rewritten)

    # Rewrite reference table references
    if reference_parquet_path:
        ref_escaped = _sql_escape_path(reference_parquet_path)
        ref_parquet_sql = f"read_parquet('{ref_escaped}')"

        ref_identifiers = ["reference"]
        if reference_name and reference_name != "reference":
            ref_identifiers.append(reference_name)

        for ref_ident in ref_identifiers:
            ref_pattern = re.compile(
                rf'(?i)\b(from|join)\s+(")?{re.escape(ref_ident)}(")?\b'
                rf'(\s+as\s+\w+|\s+(?!{_SQL_KW}\b)\w+)?'
            )

            def _ref_repl(m: re.Match[str], _ri=ref_ident) -> str:
                keyword = m.group(1)
                existing_alias = m.group(4)
                if existing_alias:
                    alias = existing_alias.strip().split()[-1]
                else:
                    alias = _ri
                return f"{keyword} {ref_parquet_sql} AS {alias}"

            rewritten = ref_pattern.sub(_ref_repl, rewritten)

    return rewritten


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

class SessionReplayEngine:
    def __init__(
        self,
        datasets_dir: Path,
        references_dir: Path,
        reference_library_dir: Path,
        sessions_dir: Path,
    ):
        self.datasets_dir = Path(datasets_dir).resolve()
        self.references_dir = Path(references_dir).resolve()
        self.reference_library_dir = Path(reference_library_dir).resolve()
        self.sessions_dir = Path(sessions_dir).resolve()

    # ------------------------------------------------------------------
    # list / load
    # ------------------------------------------------------------------

    def list_session_files(self) -> list[dict]:
        """Return metadata about each session JSON file in sessions_dir."""
        if not self.sessions_dir.exists():
            return []
        results = []
        for f in sorted(self.sessions_dir.glob("*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                results.append({
                    "filename": f.name,
                    "session_id": data.get("session_id", ""),
                    "started_at": data.get("started_at", ""),
                    "event_count": len(data.get("events", [])),
                })
            except Exception:
                results.append({
                    "filename": f.name,
                    "session_id": "",
                    "started_at": "",
                    "event_count": 0,
                })
        return results

    def load_session(self, filename: str) -> dict:
        """Load and return session JSON data. Raises FileNotFoundError if missing."""
        filepath = self.sessions_dir / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Session file not found: {filename}")
        return json.loads(filepath.read_text(encoding="utf-8"))

    # ------------------------------------------------------------------
    # Schema check
    # ------------------------------------------------------------------

    def check_schema(self, session_data: dict) -> tuple[str, list[dict]]:
        """
        Compare schema_requirements (if present) against live datasets.

        Returns (status, mismatches) where status is "pass", "fail", or "skipped".
        """
        requirements = session_data.get("schema_requirements")
        if not requirements:
            return "skipped", []

        datasets_req = requirements.get("datasets", {})
        if not datasets_req:
            return "skipped", []

        mismatches: list[dict] = []

        for ds_name, expected_columns in datasets_req.items():
            ds_parquet = self.datasets_dir / ds_name / "source.parquet"
            if not ds_parquet.exists():
                mismatches.append({
                    "dataset": ds_name,
                    "issue": "dataset_not_found",
                    "expected_columns": expected_columns,
                })
                continue

            # Get actual columns via DuckDB DESCRIBE
            try:
                con = duckdb.connect()
                escaped = _sql_escape_path(str(ds_parquet))
                result = con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
                ).fetchall()
                actual_columns = [row[0] for row in result]
                con.close()
            except Exception as exc:
                mismatches.append({
                    "dataset": ds_name,
                    "issue": "describe_error",
                    "error": str(exc),
                })
                continue

            missing = [c for c in expected_columns if c not in actual_columns]
            if missing:
                mismatches.append({
                    "dataset": ds_name,
                    "issue": "missing_columns",
                    "missing": missing,
                    "expected": expected_columns,
                    "actual": actual_columns,
                })

        status = "fail" if mismatches else "pass"
        return status, mismatches

    # ------------------------------------------------------------------
    # Reference helpers
    # ------------------------------------------------------------------

    def _find_active_reference(self) -> tuple[str | None, str | None]:
        """
        Find the currently active reference table.
        Returns (parquet_path, reference_name) or (None, None).
        """
        if not self.references_dir.exists():
            return None, None
        for d in self.references_dir.iterdir():
            if d.is_dir() and (d / "source.parquet").exists():
                return str((d / "source.parquet").resolve()), d.name
        return None, None

    def _load_reference_from_library(self, source_filename: str) -> str | None:
        """
        Load a library CSV as an active reference table.
        Returns the reference name on success, None on failure.
        """
        lib_csv = self.reference_library_dir / source_filename
        if not lib_csv.exists():
            return None

        try:
            from app.services.dataset_import import import_reference_table
        except ImportError:
            from backend.app.services.dataset_import import import_reference_table

        try:
            with open(lib_csv, "rb") as f:
                result = import_reference_table(
                    uploaded_file=f,
                    original_filename=source_filename,
                    registered_root=self.references_dir,
                    overwrite=True,
                )
            return result.reference_name
        except Exception as exc:
            logger.warning("Failed to load library reference %s: %s", source_filename, exc)
            return None

    def _delete_reference(self, reference_name: str) -> bool:
        """Delete a reference table directory. Returns True on success."""
        ref_dir = self.references_dir / reference_name
        if not ref_dir.exists():
            return True  # Already gone
        try:
            shutil.rmtree(ref_dir)
            return True
        except Exception as exc:
            logger.warning("Failed to delete reference %s: %s", reference_name, exc)
            return False

    # ------------------------------------------------------------------
    # Replay
    # ------------------------------------------------------------------

    def replay(self, filename: str, stop_on_failure: bool = False) -> ReplayReport:
        """
        Replay a session file, executing replayable events.

        Returns a ReplayReport with per-step results.
        """
        session_data = self.load_session(filename)
        session_id = session_data.get("session_id", "unknown")
        events = session_data.get("events", [])

        # Check schema
        schema_status, schema_mismatches = self.check_schema(session_data)

        # Build baselines lookup: event_index -> baseline dict
        baselines = {}
        for bl in session_data.get("baselines", []):
            baselines[bl["event_index"]] = bl

        steps: list[ReplayStepResult] = []
        passed = 0
        failed = 0
        skipped = 0
        errors = 0

        for idx, event in enumerate(events):
            evt_type = event.get("event_type", "")

            if evt_type not in _REPLAYABLE_EVENTS:
                if evt_type in _SKIPPED_EVENTS:
                    steps.append(ReplayStepResult(
                        event_index=idx,
                        event_type=evt_type,
                        status="skip",
                        message=f"Skipped non-replayable event: {evt_type}",
                    ))
                    skipped += 1
                continue

            details = event.get("details", {})

            if evt_type == "query_run":
                step = self._replay_query_run(idx, details, baselines.get(idx))
            elif evt_type == "reference_load":
                step = self._replay_reference_load(idx, details)
            elif evt_type == "reference_delete":
                step = self._replay_reference_delete(idx, details)
            else:
                step = ReplayStepResult(
                    event_index=idx,
                    event_type=evt_type,
                    status="skip",
                    message=f"Unknown replayable event: {evt_type}",
                )

            steps.append(step)

            if step.status == "pass":
                passed += 1
            elif step.status == "fail":
                failed += 1
            elif step.status == "error":
                errors += 1
            elif step.status == "skip":
                skipped += 1

            if stop_on_failure and step.status in ("fail", "error"):
                # Skip remaining events
                for remaining_idx in range(idx + 1, len(events)):
                    remaining_type = events[remaining_idx].get("event_type", "")
                    if remaining_type in _REPLAYABLE_EVENTS or remaining_type in _SKIPPED_EVENTS:
                        steps.append(ReplayStepResult(
                            event_index=remaining_idx,
                            event_type=remaining_type,
                            status="skip",
                            message="Skipped due to stop_on_failure",
                        ))
                        skipped += 1
                break

        overall = "pass" if (failed == 0 and errors == 0) else "fail"

        return ReplayReport(
            session_id=session_id,
            replay_id=str(uuid.uuid4()),
            replayed_at=datetime.now(timezone.utc).isoformat(),
            session_file=filename,
            schema_check=schema_status,
            schema_mismatches=schema_mismatches if schema_mismatches else None,
            total_steps=len(steps),
            passed=passed,
            failed=failed,
            skipped=skipped,
            errors=errors,
            steps=steps,
            overall_status=overall,
        )

    def _replay_query_run(
        self,
        idx: int,
        details: dict,
        baseline: dict | None,
    ) -> ReplayStepResult:
        """Execute a query_run event and compare results."""
        dataset_name = details.get("dataset", "")
        sql = details.get("sql", "")
        recorded_row_count = details.get("row_count")

        if not sql:
            return ReplayStepResult(
                event_index=idx,
                event_type="query_run",
                status="error",
                message="No SQL in event details",
            )

        # Resolve dataset parquet path
        ds_parquet = self.datasets_dir / dataset_name / "source.parquet"
        if not ds_parquet.exists():
            return ReplayStepResult(
                event_index=idx,
                event_type="query_run",
                status="error",
                message=f"Dataset '{dataset_name}' not found",
                error_detail=f"Expected: {ds_parquet}",
            )

        # Resolve reference if any
        ref_path, ref_name = self._find_active_reference()

        # Strip trailing semicolons
        clean_sql = sql.rstrip().rstrip(";")

        # Rewrite SQL
        try:
            rewritten = _rewrite_sql_for_replay(
                sql=clean_sql,
                dataset_name=dataset_name,
                dataset_parquet_path=str(ds_parquet.resolve()),
                reference_parquet_path=ref_path,
                reference_name=ref_name,
            )
        except Exception as exc:
            return ReplayStepResult(
                event_index=idx,
                event_type="query_run",
                status="error",
                message=f"SQL rewrite failed: {exc}",
                error_detail=str(exc),
            )

        # Execute
        t0 = time.perf_counter()
        try:
            con = duckdb.connect()
            result = con.execute(rewritten)
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description] if result.description else []
            actual_row_count = len(rows)
            con.close()
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            return ReplayStepResult(
                event_index=idx,
                event_type="query_run",
                status="error",
                message=f"SQL execution failed: {exc}",
                elapsed_seconds=round(elapsed, 4),
                error_detail=str(exc),
            )

        elapsed = time.perf_counter() - t0

        # Determine expected row count: baseline takes priority, then event's recorded value
        if baseline:
            expected_row_count = baseline.get("expected_row_count", recorded_row_count)
            expected_columns = baseline.get("expected_columns")
        else:
            expected_row_count = recorded_row_count
            expected_columns = None

        # Compare
        row_match = (expected_row_count is None) or (actual_row_count == expected_row_count)
        col_match = (expected_columns is None) or (sorted(columns) == sorted(expected_columns))

        if row_match and col_match:
            status = "pass"
            message = f"Query OK: {actual_row_count} rows"
        else:
            status = "fail"
            parts = []
            if not row_match:
                parts.append(
                    f"row_count mismatch: expected {expected_row_count}, got {actual_row_count}"
                )
            if not col_match:
                parts.append(
                    f"column mismatch: expected {expected_columns}, got {columns}"
                )
            message = "; ".join(parts)

        return ReplayStepResult(
            event_index=idx,
            event_type="query_run",
            status=status,
            message=message,
            expected_row_count=expected_row_count,
            actual_row_count=actual_row_count,
            expected_columns=expected_columns,
            actual_columns=columns,
            elapsed_seconds=round(elapsed, 4),
        )

    def _replay_reference_load(self, idx: int, details: dict) -> ReplayStepResult:
        """Replay a reference_load event."""
        ref_name = details.get("reference_name", "")
        source = details.get("source")  # Library CSV filename if from library

        if source:
            # Try loading from library
            loaded_name = self._load_reference_from_library(source)
            if loaded_name:
                return ReplayStepResult(
                    event_index=idx,
                    event_type="reference_load",
                    status="pass",
                    message=f"Loaded reference '{loaded_name}' from library: {source}",
                )
            else:
                # Library CSV not found — check if reference already exists
                ref_pq = self.references_dir / ref_name / "source.parquet"
                if ref_pq.exists():
                    return ReplayStepResult(
                        event_index=idx,
                        event_type="reference_load",
                        status="pass",
                        message=f"Reference '{ref_name}' already exists (library source not found)",
                    )
                return ReplayStepResult(
                    event_index=idx,
                    event_type="reference_load",
                    status="skip",
                    message=f"Library CSV '{source}' not found, reference not available",
                )
        else:
            # Non-library reference — check if it already exists
            ref_pq = self.references_dir / ref_name / "source.parquet"
            if ref_pq.exists():
                return ReplayStepResult(
                    event_index=idx,
                    event_type="reference_load",
                    status="pass",
                    message=f"Reference '{ref_name}' already exists",
                )
            return ReplayStepResult(
                event_index=idx,
                event_type="reference_load",
                status="skip",
                message=f"Reference '{ref_name}' not found, no source to load from",
            )

    def _replay_reference_delete(self, idx: int, details: dict) -> ReplayStepResult:
        """Replay a reference_delete event."""
        ref_name = details.get("reference_name", "")
        success = self._delete_reference(ref_name)
        if success:
            return ReplayStepResult(
                event_index=idx,
                event_type="reference_delete",
                status="pass",
                message=f"Deleted reference '{ref_name}'",
            )
        return ReplayStepResult(
            event_index=idx,
            event_type="reference_delete",
            status="error",
            message=f"Failed to delete reference '{ref_name}'",
        )

    # ------------------------------------------------------------------
    # Annotate baselines
    # ------------------------------------------------------------------

    def annotate_baselines(self, filename: str) -> dict:
        """
        Run the session, record actual row_counts as baselines,
        and write them back to the session JSON file.

        Returns the updated session data.
        """
        session_data = self.load_session(filename)
        events = session_data.get("events", [])

        baselines: list[dict] = []

        for idx, event in enumerate(events):
            evt_type = event.get("event_type", "")

            if evt_type == "reference_load":
                details = event.get("details", {})
                self._replay_reference_load(idx, details)
            elif evt_type == "reference_delete":
                details = event.get("details", {})
                self._replay_reference_delete(idx, details)
            elif evt_type == "query_run":
                details = event.get("details", {})
                step = self._replay_query_run(idx, details, baseline=None)
                if step.status in ("pass", "fail") and step.actual_row_count is not None:
                    bl: dict = {
                        "event_index": idx,
                        "expected_row_count": step.actual_row_count,
                        "tolerance": "exact",
                    }
                    if step.actual_columns:
                        bl["expected_columns"] = step.actual_columns
                    baselines.append(bl)

        session_data["baselines"] = baselines

        # Write back
        filepath = self.sessions_dir / filename
        filepath.write_text(json.dumps(session_data, indent=2), encoding="utf-8")

        return session_data
