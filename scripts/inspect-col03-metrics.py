#!/usr/bin/env python3
"""COL-03 Headless runner: extracts verified baseline metrics table, generates spatial/timeline plots, and packages handoff manifest for EXP-04."""
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
from baseline_visualization import (
    extract_baseline_metrics,
    export_metrics_table,
    plot_spatial_and_temporal_views,
    plot_algorithm_comparison_summary,
    create_handoff_manifest,
)
from test_experiment_harness import create_pilot_fixture


def main():
    parser = argparse.ArgumentParser(description="COL-03 Baseline Metrics and Plots Runner")
    parser.add_argument("--pilot-bundle", type=Path, help="Path to pilot bundle directory containing manifest/features")
    parser.add_argument("--results-dir", type=Path, help="Directory containing baseline or sweep results")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "staging/ml-experiments/col03-metrics-plots", help="Directory for output tables and plots")
    args = parser.parse_args()

    temp_bundle_dir = None
    if args.pilot_bundle and args.pilot_bundle.is_dir():
        bundle_path = args.pilot_bundle.resolve()
        print(f"Loading real pilot bundle from: {bundle_path}")
    elif (ROOT / "staging/ml-pilot/pilot-kit/bundle").is_dir():
        bundle_path = (ROOT / "staging/ml-pilot/pilot-kit/bundle").resolve()
        print(f"Loading real pilot bundle from default repo path: {bundle_path}")
    else:
        temp_bundle_dir = tempfile.TemporaryDirectory()
        bundle_path = Path(temp_bundle_dir.name) / "bundle"
        bundle_path.mkdir(parents=True, exist_ok=True)
        create_pilot_fixture(bundle_path)
        print(f"Using self-contained synthetic pilot fixture at: {bundle_path}")

    bundle = load_feature_bundle(bundle_path, mode=MODE_DEVELOPMENT)
    total_cands = bundle["features_table"].num_rows
    total_ms = bundle["mainshocks_table"].num_rows
    print(f"Bundle ID: {bundle['bundle_id']}, Candidates: {total_cands}, Mainshocks: {total_ms}")

    # Resolve results directory: default to COL-02 sweep output or EXP-02 Colab output
    if args.results_dir and args.results_dir.is_dir():
        res_dir = args.results_dir.resolve()
    elif (ROOT / "staging/ml-experiments/col02-sweep").is_dir():
        res_dir = (ROOT / "staging/ml-experiments/col02-sweep").resolve()
    elif (ROOT / "staging/exp02-colab-rosy179/results").is_dir():
        res_dir = (ROOT / "staging/exp02-colab-rosy179/results").resolve()
    else:
        raise ValueError("NO_VALID_RESULTS_DIRECTORY_FOUND: Provide --results-dir pointing to baseline/sweep results")

    print(f"Extracting baseline metrics from: {res_dir}")
    out_dir = args.output_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Extract and export metrics table with source provenance
    print("\n--- 1. Extracting Verified Baseline Metrics Table ---")
    metrics_rows, source_provenance = extract_baseline_metrics(res_dir, bundle, return_provenance=True)
    csv_p, json_p = export_metrics_table(metrics_rows, out_dir)
    print(f"Exported CSV:  {csv_p}")
    print(f"Exported JSON: {json_p}")

    print("\n--- Summary Table (Sample Rows) ---")
    header = f"{'Algorithm':<15} | {'Version':<20} | {'Mainshock ID':<20} | {'Seq':<6} | {'Noise':<6} | {'DBCV':<8} | {'Status':<10}"
    print(header)
    print("-" * len(header))
    for r in metrics_rows:
        dbcv_str = str(r['dbcv_score']) if r['dbcv_score'] is not None else "null"
        print(f"{r['algorithm_name']:<15} | {r['model_config_version']:<20} | {r['mainshock_event_id'][:18]+'..':<20} | {r['sequence_members']:<6} | {r['noise_candidates']:<6} | {dbcv_str:<8} | {r['window_status']:<10}")

    # 2. Generate plots
    print("\n--- 2. Generating Relative Space and Timeline Plots ---")
    exported_plots = []
    # Pick representational subdirectories for spatial/timeline views
    plot_subdirs = [p for p in res_dir.iterdir() if p.is_dir() and (p / "memberships.parquet").is_file()]
    for sdir in plot_subdirs[:4]:  # Plot up to 4 key configurations
        mem_p = sdir / "memberships.parquet"
        algo = sdir.name.upper()
        sp_path, tl_path = plot_spatial_and_temporal_views(bundle, mem_p, algo, out_dir)
        exported_plots.extend([sp_path, tl_path])
        print(f"  Generated {algo} plots: {sp_path.name}, {tl_path.name}")

    summary_chart_p = plot_algorithm_comparison_summary(metrics_rows, out_dir)
    exported_plots.append(summary_chart_p)
    print(f"  Generated Comparison Chart: {summary_chart_p.name}")

    # 3. Create Handoff Manifest for ThanhTris (EXP-04)
    print("\n--- 3. Packaging Handoff Manifest for ThanhTris (EXP-04) ---")
    all_handoff_files = [csv_p, json_p] + exported_plots
    manifest_p = create_handoff_manifest(
        output_dir=out_dir,
        pilot_id=bundle["bundle_id"],
        exported_files=all_handoff_files,
        recipient="ThanhTris",
        source_provenance=source_provenance,
    )
    print(f"Handoff Manifest created: {manifest_p}")

    if temp_bundle_dir:
        temp_bundle_dir.cleanup()

    print("\nCOL-03 Baseline Metrics and Plots generation COMPLETED successfully.")


if __name__ == "__main__":
    main()
