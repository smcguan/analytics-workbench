#!/usr/bin/env python
"""
tests/run_all.py - AW Feature + Workflow Test Runner

Runs test_features.py and test_workflows.py with formatted output.

Usage:
    python tests/run_all.py              # run all feature + workflow tests
    python tests/run_all.py --features   # feature tests only
    python tests/run_all.py --workflows  # workflow tests only
    python tests/run_all.py --fast       # skip slow workflow tests

Exit code 0 if all tests pass.
Exit code 1 if any test fails (blocks git push).

Add to pre-push hook (.git/hooks/pre-push):
    python -m pytest tests/ -q --tb=short || exit 1
    python tests/run_all.py || exit 1
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

TESTS_DIR     = Path(__file__).parent
REPORT_DIR    = TESTS_DIR.parent / "reports"
REPORT_XML    = REPORT_DIR / "feature_report.xml"

# Class name -> readable section header
_SECTION_MAP = {
    "TestImportPipeline":          "IMPORT PIPELINE",
    "TestInsights":                "INSIGHTS",
    "TestNaturalLanguageQueries":  "NATURAL LANGUAGE QUERIES",
    "TestSqlExecution":            "SQL EXECUTION",
    "TestReferenceTableJoin":      "REFERENCE TABLE JOIN",
    "TestResultNarrative":         "RESULT NARRATIVE",
    "TestColumnNameInterpreter":   "COLUMN NAME INTERPRETER",
    "TestAnalysisSequence":        "ANALYSIS SEQUENCE",
    "TestSuggestQuestions":        "SUGGEST QUESTIONS",
    "TestSaveAsDataset":           "SAVE AS DATASET",
    "TestSavedQueries":            "SAVED QUERIES",
    "TestSessionLog":              "SESSION LOG",
    "TestExplain":                 "EXPLAIN",
    "TestWorkflowSaveResume":      "WORKFLOW SAVE / RESUME",
    "TestSchemaAndPreview":        "SCHEMA AND PREVIEW",
    "TestPassport":                "EXPORT PASSPORT",
    # Workflow classes
    "TestWorkflowDiscovery":       "WORKFLOW DISCOVERY",
    "TestWorkflowPartD":           "TUTORIAL #1 - PART D IRA EXCLUSION",
    "TestWorkflowPartB":           "TUTORIAL #2 - PART B GLOBE CANDIDATES",
    "TestWorkflowMedicaid":        "TUTORIAL #4 - MEDICAID MULTI-STATE",
    "TestWorkflowRealEstate":      "TUTORIAL #5 - REAL ESTATE",
    "TestWorkflowRetailParameterized": "TUTORIAL #6 - RETAIL PARAMETERIZED",
    "TestWorkflowTaxi":            "TUTORIAL - NYC TAXI",
    "TestWorkflowSaaS":            "TUTORIAL - SAAS ACCOUNTS",
}

# ============================================================
# XML PARSING
# ============================================================

def _readable_test_name(raw_name: str) -> str:
    """Convert test function name to readable label."""
    # Remove 'test_' prefix and replace underscores
    name = raw_name.removeprefix("test_")
    return name.replace("_", " ")


def _get_section(classname: str) -> str:
    """Map fully-qualified classname to section header."""
    short = classname.rsplit(".", 1)[-1] if "." in classname else classname
    return _SECTION_MAP.get(short, short.replace("Test", "").upper().strip())


def _parse_junit_xml(xml_path: Path) -> list[dict]:
    """
    Parse JUnit XML and return list of test result dicts:
      {section, name, status (pass/fail/skip/error), time, message}
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    # JUnit XML can have <testsuite> as root or wrapped in <testsuites>
    if root.tag == "testsuites":
        suites = list(root)
    elif root.tag == "testsuite":
        suites = [root]
    else:
        suites = list(root.iter("testsuite"))

    results = []
    for suite in suites:
        for tc in suite.iter("testcase"):
            classname = tc.attrib.get("classname", "")
            name      = tc.attrib.get("name", "")
            elapsed   = float(tc.attrib.get("time", "0") or "0")

            failure = tc.find("failure")
            error   = tc.find("error")
            skipped = tc.find("skipped")

            if failure is not None:
                status  = "fail"
                message = (failure.attrib.get("message") or failure.text or "")[:200]
            elif error is not None:
                status  = "error"
                message = (error.attrib.get("message") or error.text or "")[:200]
            elif skipped is not None:
                status  = "skip"
                message = skipped.attrib.get("message", "")
            else:
                status  = "pass"
                message = ""

            results.append({
                "section": _get_section(classname),
                "classname": classname,
                "name": name,
                "label": _readable_test_name(name),
                "status": status,
                "time": elapsed,
                "message": message,
            })

    return results


# ============================================================
# FORMATTED OUTPUT
# ============================================================

# ANSI colours (disabled automatically if no TTY)
def _ansi(code: str, text: str) -> str:
    if sys.stdout.isatty():
        return f"\033[{code}m{text}\033[0m"
    return text

GREEN  = lambda t: _ansi("32", t)
RED    = lambda t: _ansi("31", t)
YELLOW = lambda t: _ansi("33", t)
BOLD   = lambda t: _ansi("1",  t)
DIM    = lambda t: _ansi("2",  t)
CYAN   = lambda t: _ansi("36", t)

WIDTH = 63


_SEP = "=" * WIDTH   # ASCII-safe; works on Windows cp1252


def _banner(version: str) -> None:
    today = date.today().isoformat()
    title = f"AW FEATURE TEST SUITE  {version}  {today}"
    print(BOLD(_SEP))
    print(BOLD(title))
    print(BOLD(_SEP))
    print()


def _print_section_header(section: str) -> None:
    print(BOLD(CYAN(section)))


def _status_icon(status: str) -> str:
    return {"pass": GREEN("PASS"), "fail": RED("FAIL"),
            "error": RED("ERR "), "skip": YELLOW("SKIP")}.get(status, "?   ")


def _print_result(r: dict) -> None:
    icon   = _status_icon(r["status"])
    label  = r["label"][:55]
    timing = DIM(f"{r['time']:.2f}s")

    if r["status"] in ("fail", "error") and r["message"]:
        # Truncate and clean up message
        msg = r["message"].strip().replace("\n", " ")[:80]
        print(f"  {icon} {label} {timing}")
        print(f"      {RED(msg)}")
    elif r["status"] == "skip":
        print(f"  {icon} {DIM(label)} {timing}")
    else:
        print(f"  {icon} {label} {timing}")


def _print_summary(results: list[dict], total_time: float) -> None:
    passed  = sum(1 for r in results if r["status"] == "pass")
    failed  = sum(1 for r in results if r["status"] in ("fail", "error"))
    skipped = sum(1 for r in results if r["status"] == "skip")
    total   = len(results)

    print()
    print(BOLD("=" * WIDTH))
    print(BOLD("SUMMARY"))
    print("  " + "-" * (WIDTH - 2))

    feat = [r for r in results if "TestWorkflow" not in r["classname"]]
    wf   = [r for r in results if "TestWorkflow" in r["classname"]]

    feat_pass = sum(1 for r in feat if r["status"] == "pass")
    feat_fail = sum(1 for r in feat if r["status"] in ("fail", "error"))
    wf_pass   = sum(1 for r in wf   if r["status"] == "pass")
    wf_fail   = sum(1 for r in wf   if r["status"] in ("fail", "error"))

    if feat:
        print(f"  Feature tests:   {GREEN(str(feat_pass))} passed"
              + (f", {RED(str(feat_fail))} failed" if feat_fail else ""))
    if wf:
        print(f"  Workflow tests:  {GREEN(str(wf_pass))} passed"
              + (f", {RED(str(wf_fail))} failed" if wf_fail else ""))
    if skipped:
        print(f"  Skipped:         {YELLOW(str(skipped))}")

    print(f"  Total:           {passed}/{total} passed")
    print(f"  Duration:        {total_time:.1f}s")
    print("  " + "-" * (WIDTH - 2))

    if failed == 0:
        print(f"  {GREEN('ALL TESTS PASSED')} [OK]")
    else:
        print(f"  {RED(f'{failed} FAILURE(S) - fix before pushing')}")

    print(BOLD("=" * WIDTH))

    # Detailed failures list
    failures = [r for r in results if r["status"] in ("fail", "error")]
    if failures:
        print()
        print(BOLD(RED("FAILURES (fix before pushing):")))
        for i, r in enumerate(failures, 1):
            section = r["section"]
            label   = r["label"]
            msg     = r["message"].strip().replace("\n", " ")[:100] if r["message"] else ""
            print(f"  {RED(str(i) + '.')} {section} - {label}")
            if msg:
                print(f"     {DIM(msg)}")
        print()


# ============================================================
# VERSION DETECTION
# ============================================================

def _get_version() -> str:
    """Read version from CONTEXT.md (Current version: vX.Y.Z line)."""
    try:
        ctx = Path(__file__).parent.parent / "CONTEXT.md"
        for line in ctx.read_text(encoding="utf-8").splitlines():
            if "**Current version:**" in line:
                parts = line.split("**Current version:**")
                if len(parts) > 1:
                    return parts[1].strip().split()[0]
    except Exception:
        pass
    return "unknown"


# ============================================================
# RUNNER
# ============================================================

def _run_pytest(test_files: list[str], report_xml: Path) -> tuple[int, float]:
    """Run pytest on the given files, capture JUnit XML. Returns (returncode, elapsed)."""
    report_xml.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "pytest",
        *test_files,
        f"--junit-xml={report_xml}",
        "-q",
        "--tb=short",
        "--no-header",
    ]

    t0 = time.perf_counter()
    result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
    elapsed = time.perf_counter() - t0

    return result.returncode, elapsed


def main() -> int:
    parser = argparse.ArgumentParser(description="AW Feature Test Runner")
    parser.add_argument("--features",  action="store_true", help="Run feature tests only")
    parser.add_argument("--workflows", action="store_true", help="Run workflow tests only")
    parser.add_argument("--fast",      action="store_true",
                        help="Skip slow workflow API tests (run discovery only)")
    args = parser.parse_args()

    tests_dir = Path(__file__).parent
    feature_file  = str(tests_dir / "test_features.py")
    workflow_file = str(tests_dir / "test_workflows.py")

    if args.features:
        test_files = [feature_file]
    elif args.workflows:
        test_files = [workflow_file]
    elif args.fast:
        # Only run discovery checks - skip the API-heavy workflow classes
        test_files = [
            f"{workflow_file}::TestWorkflowDiscovery",
            feature_file,
        ]
    else:
        test_files = [feature_file, workflow_file]

    version = _get_version()
    _banner(version)

    print(DIM(f"Running: {', '.join(Path(f).name for f in test_files)}"))
    print()

    returncode, total_time = _run_pytest(test_files, REPORT_XML)

    if not REPORT_XML.exists():
        print(RED("ERROR: pytest did not produce JUnit XML report."))
        print(DIM("  Check that pytest is installed: pip install pytest"))
        return 1

    results = _parse_junit_xml(REPORT_XML)

    if not results:
        print(YELLOW("No test results found in report."))
        return returncode

    # Group by section and print
    seen_sections: set[str] = set()
    current_section = ""

    for r in results:
        if r["section"] != current_section:
            if current_section:
                print()
            current_section = r["section"]
            if current_section not in seen_sections:
                _print_section_header(current_section)
                seen_sections.add(current_section)
        _print_result(r)

    _print_summary(results, total_time)

    return 1 if any(r["status"] in ("fail", "error") for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
