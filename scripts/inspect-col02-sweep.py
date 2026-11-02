#!/usr/bin/env python3
"""COL-02 Headless runner: runs the bounded parameter sweep on development pilot, verifies rerun invariance, and generates sweep report."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "notebooks"))
sys.path.insert(0, str(ROOT / "tests"))

from experiment_harness import (
    load_feature_bundle,
    MODE_DEVELOPMENT,
    canonical,
    sha256_bytes,
)
from parameter_sweep import (
    SWEEP_CONFIG_FILES,
    run_bounded_parameter_sweep,
    verify_sweep_rerun_invariants,
)
from test_experiment_harness import create_pilot_fixture


def main():
    parser = argparse.ArgumentParser(description="COL-02 Parameter Sweep Runner")
    parser.add_argument("--pilot-bundle", type=Path, help="Path to pilot bundle directory containing manifest/features")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "staging/ml-experiments/col02-sweep", help="Directory for sweep outputs")
    parser.add_argument("--report-json", type=Path, help="Path to write col02-sweep-report.json")
    args = parser.parse_args()

    temp_bundle_dir = None
    if args.pilot_bundle and args.pilot_bundle.is_dir():
        bundle_path = args.pilot_bundle.resolve()
        print(f"Loading real pilot bundle from: {bundle_path}")
    else:
        temp_bundle_dir = tempfile.TemporaryDirectory()
        bundle_path = Path(temp_bundle_dir.name)
        create_pilot_fixture(bundle_path)
        print(f"Using self-contained synthetic pilot fixture at: {bundle_path}")

    bundle = load_feature_bundle(bundle_path, mode=MODE_DEVELOPMENT)
    total_cands = bundle["features_table"].num_rows
    total_ms = bundle["mainshocks_table"].num_rows
    print(f"Bundle ID: {bundle['bundle_id']}, Candidates: {total_cands}, Mainshocks: {total_ms}")

    out_dir = args.output_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print("\n--- Running Bounded Parameter Sweep (8 configurations + 1 control) ---")
    sweep = run_bounded_parameter_sweep(
        bundle=bundle,
        project_root=ROOT,
        output_dir=out_dir,
        allow_overwrite=True,
        code_version="col02-sweep-v1",
    )
    matrix = sweep["matrix"]
    print(f"Sweep completed. Configurations evaluated: {len(matrix['configurations'])}")

    print("\n--- Parameter Sweep Results Matrix ---")
    header = f"{'Config Key':<15} | {'Algorithm':<15} | {'Version':<20} | {'Seq Mbrs':<9} | {'Aftershocks':<11} | {'Noise':<6} | {'Status':<10}"
    print(header)
    print("-" * len(header))
    for r in matrix["configurations"]:
        status_str = f"{r['successful_windows']}/{r['windows_processed']} PASS"
        print(f"{r['config_key']:<15} | {r['algorithm_name']:<15} | {r['model_config_version']:<20} | {r['total_sequence_members']:<9} | {r['total_aftershocks']:<11} | {r['total_noise']:<6} | {status_str:<10}")

    print("\n--- Verifying Deterministic Rerun Invariance ---")
    rerun_dir = out_dir / "rerun_check"
    rerun_sweep = run_bounded_parameter_sweep(
        bundle=bundle,
        project_root=ROOT,
        output_dir=rerun_dir,
        config_keys=["DBSCAN_C1"],
        allow_overwrite=True,
    )
    invariants = verify_sweep_rerun_invariants(
        sweep["results"]["DBSCAN_C1"],
        rerun_sweep["results"]["DBSCAN_C1"],
    )
    print(f"Rerun Invariants Check: {invariants['status']} (tol rtol={invariants['rtol']}, atol={invariants['atol']})")

    # Generate formal report
    report_path = args.report_json or (out_dir / "col02-sweep-report.json")
    report = {
        "report_version": "col-02-sweep-report-v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "pilot_id": bundle["bundle_id"],
        "candidate_count": total_cands,
        "mainshock_count": total_ms,
        "sweep_configurations_evaluated": len(matrix["configurations"]),
        "configurations": matrix["configurations"],
        "rerun_verification": invariants,
        "allowed_usage": "DEVELOPMENT_ONLY",
        "research_ready": False,
        "official_exported": False,
        "scientific_approval": False,
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nFormal Sweep Report saved to: {report_path}")

    if temp_bundle_dir:
        temp_bundle_dir.cleanup()


if __name__ == "__main__":
    main()
