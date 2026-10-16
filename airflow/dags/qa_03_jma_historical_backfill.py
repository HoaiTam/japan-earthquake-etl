"""Prepare and, after ORC-07, verify QA-03 JMA historical backfill evidence."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
from typing import Any, Mapping


class Qa03Error(RuntimeError):
    pass


def _load(path: Path, limit: int = 256 * 1024) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise Qa03Error("QA03_MATRIX_NOT_BOUNDED")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Qa03Error("QA03_MATRIX_INVALID_JSON") from error
    if not isinstance(value, dict):
        raise Qa03Error("QA03_MATRIX_OBJECT_REQUIRED")
    return value


def load_matrix(path: str | Path) -> dict[str, Any]:
    matrix = _load(Path(path))
    if matrix.get("task_id") != "QA-03" or matrix.get("matrix_version") != "qa-03-v1":
        raise Qa03Error("QA03_MATRIX_VERSION_MISMATCH")
    scopes = matrix.get("scope")
    required = {(1997, "jan-sep"), (1997, "oct-dec"), (2000, "full-year"), (2023, "full-year")}
    actual = {(item.get("year"), item.get("segment")) for item in scopes} if isinstance(scopes, list) else set()
    if actual != required:
        raise Qa03Error("QA03_REPRESENTATIVE_SCOPE_REQUIRED")
    months = {month for item in scopes for month in item.get("months", [])}
    if not {"1997-09", "1997-10", "2000-01", "2023-09"} <= months:
        raise Qa03Error("QA03_TARGET_MONTHS_REQUIRED")
    cases = matrix.get("cases")
    if not isinstance(cases, list) or not cases:
        raise Qa03Error("QA03_CASES_REQUIRED")
    seen: set[str] = set()
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get("case_id"), str) or case["case_id"] in seen:
            raise Qa03Error("QA03_CASE_INVALID")
        if not isinstance(case.get("expected"), dict):
            raise Qa03Error("QA03_CASE_INVALID")
        seen.add(case["case_id"])
    return matrix


def prepare_report(matrix_path: str | Path) -> dict[str, Any]:
    matrix = load_matrix(matrix_path)
    cases = [{"case_id": case["case_id"], "status": "PENDING_RUNTIME", "expected": case["expected"]}
             for case in matrix["cases"]]
    return {
        "task_id": "QA-03", "report_version": "qa-03-report-v1", "status": "PREPARED",
        "runtime_complete": False, "synthetic_revision_fixture": True,
        "dependency_gate": matrix["dependency_gate"], "input_pins": matrix["input_pins"],
        "scope": matrix["scope"], "coverage": {"selected_months": sorted({m for item in matrix["scope"] for m in item["months"]}),
                                                    "remaining_period": "not covered by this representative run"},
        "cases": cases,
        "capacity": {"duration_seconds": None, "peak_memory_mb": None, "resource_failures": 0,
                      "download_reused": None, "verified_archive_reused": None},
        "summary": {"scope_count": len(matrix["scope"]), "case_count": len(cases), "pending_runtime": len(cases)},
        "next_gate": "Run closed-month batches through ORC-07 real adapters and attach Bronze-to-Published counts/snapshots.",
    }


def write_preparation(matrix_path: str | Path, json_path: str | Path, markdown_path: str | Path) -> dict[str, Any]:
    report = prepare_report(matrix_path)
    json_target, markdown_target = Path(json_path), Path(markdown_path)
    json_target.parent.mkdir(parents=True, exist_ok=True)
    markdown_target.parent.mkdir(parents=True, exist_ok=True)
    json_target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = ["# QA-03 - JMA historical backfill", "", f"Status: **{report['status']}**", "",
             "Selected months are representative coverage; they do not prove the full historical period.", "",
             "| Year | Segment | Months | Release |", "|---:|---|---|---|"]
    lines.extend(f"| {item['year']} | {item['segment']} | {', '.join(item['months'])} | `{item['release']}` |" for item in report["scope"])
    lines.extend(["", "## Runtime cases", "", "| Case | Status |", "|---|---|"])
    lines.extend(f"| {case['case_id']} | {case['status']} |" for case in report["cases"])
    lines.extend(["", "Capacity fields remain empty until the bounded runtime run.", ""])
    markdown_target.write_text("\n".join(lines), encoding="utf-8")
    return report


def run_live(request_path: str | Path, environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    request = _load(Path(request_path), 64 * 1024)
    if request.get("task_id") != "QA-03" or not request.get("matrix_path"):
        raise Qa03Error("QA03_RUNTIME_REQUEST_INVALID")
    command_text = env.get("QA_03_RUNTIME_COMMAND", "").strip()
    if not command_text:
        raise Qa03Error("QA03_RUNTIME_COMMAND_REQUIRED")
    command = shlex.split(command_text)
    if not command:
        raise Qa03Error("QA03_RUNTIME_COMMAND_REQUIRED")
    timeout = int(env.get("QA_03_RUNTIME_TIMEOUT_SECONDS", "3600"))
    if not 1 <= timeout <= 3600:
        raise Qa03Error("QA03_RUNTIME_TIMEOUT_INVALID")
    completed = subprocess.run(command + [str(Path(request_path))], env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               timeout=timeout, check=False)
    if completed.returncode != 0:
        raise Qa03Error("QA03_RUNTIME_FAILED")
    report = _load(Path(request["report_path"]), 4 * 1024 * 1024)
    if report.get("task_id") != "QA-03" or report.get("runtime_complete") is not True or report.get("status") != "PASS":
        raise Qa03Error("QA03_RUNTIME_REPORT_INCOMPLETE")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix", type=Path)
    parser.add_argument("--json", type=Path)
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--live", type=Path, dest="live_request")
    args = parser.parse_args()
    if args.live_request is not None:
        print(json.dumps(run_live(args.live_request), sort_keys=True))
        return
    if not args.matrix or not args.json or not args.markdown:
        parser.error("--matrix, --json and --markdown are required for preparation")
    print(json.dumps(write_preparation(args.matrix, args.json, args.markdown), sort_keys=True))


if __name__ == "__main__":
    main()
