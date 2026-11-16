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
    parser.add_argument(
        "--pilot-bundle",
        type=Path,
        help="Path to pilot bundle directory containing manifest/features",
    )
    parser.add_argument(
        "--synthetic-fixture",
        action="store_true",
        default=False,
        help="Explicit flag to allow using a synthetic pilot fixture for dev/testing only",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "staging/ml-experiments/col02-sweep",
        help="Directory for sweep outputs",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        help="Path to write col02-sweep-report.json",
    )
    parser.add_argument(
        "--allow-overwrite",
        action="store_true",
        default=False,
        help="Explicitly permit overwriting an existing sweep output directory",
    )
    args = parser.parse_args()

    temp_bundle_dir = None
    if args.pilot_bundle is not None:
        if args.synthetic_fixture:
            parser.error("Cannot combine --pilot-bundle and --synthetic-fixture.")
        if not args.pilot_bundle.exists():
            print(f"ERROR: Explicit pilot bundle path does not exist: {args.pilot_bundle}", file=sys.stderr)
            sys.exit(2)
        if not args.pilot_bundle.is_dir():
            print(f"ERROR: Explicit pilot bundle path is not a directory: {args.pilot_bundle}", file=sys.stderr)
            sys.exit(2)
        bundle_path = args.pilot_bundle.resolve()
        input_kind = "REAL_PILOT"
        print(f"Loading real pilot bundle from: {bundle_path}")
    elif args.synthetic_fixture:
        temp_bundle_dir = tempfile.TemporaryDirectory()
        bundle_path = Path(temp_bundle_dir.name)
        create_pilot_fixture(bundle_path)
        input_kind = "SYNTHETIC_FIXTURE"
        print(f"Using explicitly requested synthetic pilot fixture at: {bundle_path}")
    else:
        print("ERROR: Missing input dataset. Either --pilot-bundle <dir> or --synthetic-fixture is required.", file=sys.stderr)
        sys.exit(2)

    bundle = load_feature_bundle(bundle_path, mode=MODE_DEVELOPMENT)
    total_cands = bundle["features_table"].num_rows
    total_ms = bundle["mainshocks_table"].num_rows
    print(f"Bundle ID: {bundle['bundle_id']}, Candidates: {total_cands}, Mainshocks: {total_ms}")

    out_dir = args.output_dir.resolve()

    print("\n--- Running Bounded Parameter Sweep (8 configurations + 1 control) ---")
    try:
        sweep = run_bounded_parameter_sweep(
            bundle=bundle,
            project_root=ROOT,
            output_dir=out_dir,
            allow_overwrite=args.allow_overwrite,
            code_version="col02-sweep-v1",
        )
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        if temp_bundle_dir:
            temp_bundle_dir.cleanup()
        sys.exit(1)

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
        allow_overwrite=args.allow_overwrite,
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
        "input_kind": input_kind,
        "is_synthetic_fixture": (input_kind == "SYNTHETIC_FIXTURE"),
        "pilot_id": bundle["bundle_id"],
        "pilot_bundle_path": str(bundle_path),
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
