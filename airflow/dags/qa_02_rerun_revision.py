"""Prepare and, when a real adapter is supplied, verify QA-02 evidence."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
from typing import Any, Mapping


class Qa02Error(RuntimeError):
    pass


def _load(path: Path, limit: int = 256 * 1024) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise Qa02Error("QA02_MATRIX_NOT_BOUNDED")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Qa02Error("QA02_MATRIX_INVALID_JSON") from error
    if not isinstance(value, dict):
        raise Qa02Error("QA02_MATRIX_OBJECT_REQUIRED")
    return value


def load_matrix(path: str | Path) -> dict[str, Any]:
    matrix = _load(Path(path))
    if matrix.get("task_id") != "QA-02" or matrix.get("matrix_version") != "qa-02-v1":
        raise Qa02Error("QA02_MATRIX_VERSION_MISMATCH")
    cases = matrix.get("cases")
    if not isinstance(cases, list) or not cases:
        raise Qa02Error("QA02_CASES_REQUIRED")
    seen: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            raise Qa02Error("QA02_CASE_INVALID")
        case_id, operation_id = case.get("case_id"), case.get("operation_id")
        if (not isinstance(case_id, str) or case_id in seen or not isinstance(operation_id, str)
                or not case.get("category") or not isinstance(case.get("expected"), dict)):
            raise Qa02Error("QA02_CASE_INVALID")
        seen.add(case_id)
    return matrix


def prepare_report(matrix_path: str | Path) -> dict[str, Any]:
    matrix = load_matrix(matrix_path)
    cases = [{"case_id": case["case_id"], "operation_id": case["operation_id"],
              "category": case["category"], "status": "PENDING_RUNTIME",
              "expected": case["expected"]} for case in matrix["cases"]]
    return {
        "task_id": "QA-02", "report_version": "qa-02-report-v1", "status": "PREPARED",
        "runtime_complete": False, "synthetic_fixture": True,
        "dependency_gate": matrix["dependency_gate"], "input_pins": matrix["input_pins"],
        "cases": cases,
        "summary": {"case_count": len(cases), "pending_runtime": len(cases), "passed": 0, "failed": 0},
        "next_gate": "Run through ORC-07 real adapters in a QA namespace, then attach before/after reconciliation.",
    }


def write_preparation(matrix_path: str | Path, json_path: str | Path, markdown_path: str | Path) -> dict[str, Any]:
    report = prepare_report(matrix_path)
    json_target, markdown_target = Path(json_path), Path(markdown_path)
    json_target.parent.mkdir(parents=True, exist_ok=True)
    markdown_target.parent.mkdir(parents=True, exist_ok=True)
    json_target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = ["# QA-02 - Rerun, duplicate and late revision", "", f"Status: **{report['status']}**", "",
             "Runtime acceptance is pending ORC-07 real adapters. Synthetic cases are a test plan, not a pass.", "",
             "| Case | Category | Operation | Status |", "|---|---|---|---|"]
    lines.extend(f"| {case['case_id']} | {case['category']} | `{case['operation_id']}` | {case['status']} |" for case in report["cases"])
    lines.extend(["", "## Reconciliation to record", "", "- logical keys and current/history rows", "- canonical count and source overlap", "- partition contents and exact snapshot IDs", "- KPI before/after and retained old snapshots", ""])
    markdown_target.write_text("\n".join(lines), encoding="utf-8")
    return report


def run_live(request_path: str | Path, environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    request = _load(Path(request_path), 64 * 1024)
    if request.get("task_id") != "QA-02" or not request.get("matrix_path"):
        raise Qa02Error("QA02_RUNTIME_REQUEST_INVALID")
    command_text = env.get("QA_02_RUNTIME_COMMAND", "").strip()
    if not command_text:
        raise Qa02Error("QA02_RUNTIME_COMMAND_REQUIRED")
    command = shlex.split(command_text)
    if not command:
        raise Qa02Error("QA02_RUNTIME_COMMAND_REQUIRED")
    timeout = int(env.get("QA_02_RUNTIME_TIMEOUT_SECONDS", "3600"))
    if not 1 <= timeout <= 3600:
        raise Qa02Error("QA02_RUNTIME_TIMEOUT_INVALID")
    completed = subprocess.run(command + [str(Path(request_path))], env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               timeout=timeout, check=False)
    if completed.returncode != 0:
        raise Qa02Error("QA02_RUNTIME_FAILED")
    report = _load(Path(request["report_path"]), 4 * 1024 * 1024)
    if report.get("task_id") != "QA-02" or report.get("runtime_complete") is not True or report.get("status") != "PASS":
        raise Qa02Error("QA02_RUNTIME_REPORT_INCOMPLETE")
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
