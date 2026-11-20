"""Unit, contract, and negative gate tests for EXP-05 out-of-period extension evaluation.

Uses tiny deterministic synthetic REPRODUCTION and EXTENSION collection fixtures;
never touches real DAT-03 bundles or claims real scientific results.
"""
from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = ROOT / "notebooks"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))

import research_collection as collection  # noqa: E402
import experiment_harness as harness  # noqa: E402
import evaluation  # noqa: E402
import extension_evaluation as ext_eval  # noqa: E402
import tests.test_experiment_harness as test_harness_util  # noqa: E402


def _load_cli_module():
    spec = importlib.util.spec_from_file_location("run_exp05_extension_cli", ROOT / "scripts/run-exp05-extension.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _write_json(path: Path, value: object) -> None:
    path.write_text(harness.canonical(value), encoding="utf-8")


def _reindex_collection(root: Path) -> dict[str, str]:
    files = {
        p.relative_to(root).as_posix(): collection._hash(p)
        for p in root.rglob("*")
        if p.is_file() and p.name not in ("dataset_manifest.json", "checksums.sha256", "export-checksums.sha256")
    }
    (root / "export-checksums.sha256").write_text("".join(f"{v}  {k}\n" for k, v in sorted(files.items())), encoding="utf-8")
    manifest = json.loads((root / "dataset_manifest.json").read_bytes())
    manifest["export_manifest_sha256"] = collection._hash(root / "export-checksums.sha256")
    _write_json(root / "dataset_manifest.json", manifest)
    files.update({n: collection._hash(root / n) for n in ("dataset_manifest.json", "export-checksums.sha256")})
    (root / "checksums.sha256").write_text("".join(f"{v}  {k}\n" for k, v in sorted(files.items())), encoding="utf-8")
    return {
        "manifest_sha256": collection._hash(root / "dataset_manifest.json"),
        "checksums_sha256": collection._hash(root / "checksums.sha256"),
    }


def _build_collection_fixture(
    root: Path,
    pilot_template_root: Path,
    *,
    dataset_split: str,
    dataset_id: str,
    collection_id: str,
    mc_value: float,
    period_start_utc: str,
    period_end_utc_exclusive: str,
    required_month_count: int,
    window_model_version: str = "wm_expanded_v1.0",
    scaling_config_version: str = "scaling-global-research-90k-v1",
    feature_version: str = "relative-4d-v1",
    gold_publication_id: str = "pub_" + "a" * 64,
    gold_snapshot_id: int = 7580315846938041321,
    dense_cluster_points: int = 12,
    base_time: datetime = datetime(2020, 3, 1, tzinfo=timezone.utc),
) -> dict[str, str]:
    root.mkdir(parents=True, exist_ok=True)
    pilot_manifest = json.loads((pilot_template_root / "pilot_manifest.json").read_bytes())
    feat_seed = pq.ParquetFile(pilot_template_root / "features-part-00000.parquet").read().drop(["pilot_id"])
    main_seed = pq.ParquetFile(pilot_template_root / "mainshocks.parquet").read().drop(["pilot_id"])
    own_template = feat_seed.to_pylist()[0]
    post_template = feat_seed.to_pylist()[1]
    main_template = main_seed.to_pylist()[0]

    # Shard 0: dense cluster around mainshock (0,0,0,0) with Omori decay + 4 far background points
    # Shard 1: isolated mainshock at (0,0,0,0) with remote off-center cluster so mainshock classifies as noise (-1)
    shard_specs = [
        ("shard_00000", f"{dataset_split.lower()}_main_0", base_time, True, dense_cluster_points + 4),
        ("shard_00001", f"{dataset_split.lower()}_main_1", base_time + timedelta(days=45), False, 5),
    ]

    total_rows = 0
    shards_plan = []
    for shard_id, mid, m_time, dense_shard, expected_cand_count in shard_specs:
        shard_dir = root / shard_id
        shard_dir.mkdir(parents=True, exist_ok=True)
        rows = []

        own_row = copy.deepcopy(own_template)
        own_row.update(
            schema_version="1.0",
            dataset_id=dataset_id,
            mainshock_event_id=mid,
            candidate_event_id=mid,
            relative_time_role="MAINSHOCK",
            window_model_version=window_model_version,
            scaling_config_version=scaling_config_version,
            feature_version=feature_version,
            mc_value=mc_value,
            is_mainshock=True,
            candidate_time_utc=m_time,
            created_at_utc=m_time,
        )
        rows.append(own_row)

        if dense_shard:
            post_offsets_h = [0.2 * (1.5 ** i) for i in range(dense_cluster_points)]
            for idx, dt_h in enumerate(post_offsets_h):
                r = copy.deepcopy(post_template)
                offset_km = 0.25 + 0.08 * (idx % 4)
                r.update(
                    schema_version="1.0",
                    dataset_id=dataset_id,
                    mainshock_event_id=mid,
                    candidate_event_id=f"{mid}_c{idx:03d}",
                    relative_time_role="POST",
                    window_model_version=window_model_version,
                    scaling_config_version=scaling_config_version,
                    feature_version=feature_version,
                    mc_value=mc_value,
                    candidate_magnitude=3.2 + 0.1 * (idx % 5),
                    delta_time_hours=dt_h,
                    time_scaled=dt_h / 720.0,
                    dx_km=offset_km,
                    dy_km=offset_km * 0.5,
                    dz_km=0.1 * (idx % 3),
                    distance_3d_km=round((offset_km ** 2 + (offset_km * 0.5) ** 2) ** 0.5, 4),
                    x_scaled=offset_km / 50.0,
                    y_scaled=(offset_km * 0.5) / 50.0,
                    z_scaled=(0.1 * (idx % 3)) / 30.0,
                    candidate_time_utc=m_time + timedelta(hours=dt_h),
                    created_at_utc=m_time,
                )
                rows.append(r)

            for bg_idx in range(4):
                r = copy.deepcopy(post_template)
                dist = 180.0 + 35.0 * bg_idx
                dt_h = 300.0 + 90.0 * bg_idx
                r.update(
                    schema_version="1.0",
                    dataset_id=dataset_id,
                    mainshock_event_id=mid,
                    candidate_event_id=f"{mid}_bg{bg_idx:03d}",
                    relative_time_role="POST",
                    window_model_version=window_model_version,
                    scaling_config_version=scaling_config_version,
                    feature_version=feature_version,
                    mc_value=mc_value,
                    candidate_magnitude=2.2,
                    delta_time_hours=dt_h,
                    time_scaled=dt_h / 720.0,
                    dx_km=dist,
                    dy_km=dist,
                    dz_km=50.0 + 15.0 * bg_idx,
                    distance_3d_km=dist,
                    x_scaled=dist / 50.0,
                    y_scaled=dist / 50.0,
                    z_scaled=(50.0 + 15.0 * bg_idx) / 30.0,
                    candidate_time_utc=m_time + timedelta(hours=dt_h),
                    created_at_utc=m_time,
                )
                rows.append(r)
        else:
            # 4 remote tight points far from isolated mainshock -> mainshock stays noise (-1)
            for off_idx in range(4):
                r = copy.deepcopy(post_template)
                dt_h = 200.0 + 0.1 * off_idx
                r.update(
                    schema_version="1.0",
                    dataset_id=dataset_id,
                    mainshock_event_id=mid,
                    candidate_event_id=f"{mid}_remote{off_idx:03d}",
                    relative_time_role="POST",
                    window_model_version=window_model_version,
                    scaling_config_version=scaling_config_version,
                    feature_version=feature_version,
                    mc_value=mc_value,
                    delta_time_hours=dt_h,
                    time_scaled=dt_h / 720.0,
                    dx_km=220.0 + 0.2 * off_idx,
                    dy_km=220.0,
                    dz_km=120.0,
                    distance_3d_km=320.0,
                    x_scaled=4.4 + 0.004 * off_idx,
                    y_scaled=4.4,
                    z_scaled=4.0,
                    candidate_time_utc=m_time + timedelta(hours=dt_h),
                    created_at_utc=m_time,
                )
                rows.append(r)

        cand_count = len(rows)
        assert cand_count == expected_cand_count + 1 if dense_shard else cand_count == 5
        pq.write_table(pa.Table.from_pylist(rows, schema=feat_seed.schema), shard_dir / "features.parquet")

        m_row = copy.deepcopy(main_template)
        m_row.update(
            schema_version="1.0",
            dataset_id=dataset_id,
            mainshock_event_id=mid,
            window_model_version=window_model_version,
            mc_value=mc_value,
            candidate_count=cand_count,
            event_time_utc=m_time,
            created_at_utc=m_time,
        )
        pq.write_table(pa.Table.from_pylist([m_row], schema=main_seed.schema), shard_dir / "mainshocks.parquet")

        total_rows += cand_count
        shards_plan.append({
            "shard_id": shard_id,
            "mainshock_count": 1,
            "feature_row_count": cand_count,
            "mainshocks": [{"mainshock_event_id": mid, "candidate_row_count": cand_count}],
        })

    plan = {
        "dataset_id": dataset_id,
        "collection_id": collection_id,
        "plan_sha256": collection_id.removeprefix("col_"),
        "full_mainshock_assignment_verified": True,
        "split_windows": False,
        "shards": shards_plan,
        "mainshock_count": len(shard_specs),
        "feature_row_count": total_rows,
        "global_mc": {"scope": "FULL_PINNED_SPLIT", "mc_value": mc_value},
        "coverage": {"status": "COMPLETE", "required_month_count": required_month_count},
    }
    _write_json(root / "collection-preparation.json", {"status": "RESEARCH_COLLECTION_PREPARED", "plan": plan})
    manifest = {
        "export_version": collection.VERSION,
        "dataset_status": "EXPORTED",
        "schema_version": "1.0",
        "dataset_id": dataset_id,
        "collection_id": collection_id,
        "collection_plan": plan,
        "mainshock_count": len(shard_specs),
        "feature_row_count": total_rows,
        "dataset_split": dataset_split,
        "mc_value": mc_value,
        "period_start_utc": period_start_utc,
        "period_end_utc_exclusive": period_end_utc_exclusive,
        "gold_publication_id": gold_publication_id,
        "gold_snapshot_id": gold_snapshot_id,
        "feature_version": feature_version,
        "scaling_config_version": scaling_config_version,
        "window_model_version": window_model_version,
        "feature_schema": [e for e in pilot_manifest["feature_schema"] if e["name"] != "pilot_id"],
    }
    _write_json(root / "dataset_manifest.json", manifest)
    return _reindex_collection(root)


class ExtensionEvaluationTest(unittest.TestCase):
    """Comprehensive unit and negative tests for EXP-05 extension runner & freeze gate."""

    @classmethod
    def setUpClass(cls) -> None:
        cls._shared_tmp = Path(tempfile.mkdtemp(prefix="exp05_shared_"))
        cls.proj_root = cls._shared_tmp / "repo"
        shutil.copytree(ROOT / "config", cls.proj_root / "config")
        pilot_tpl = cls._shared_tmp / "pilot_tpl"
        pilot_tpl.mkdir()
        test_harness_util.create_pilot_fixture(pilot_tpl)
        cls.pilot_tpl = pilot_tpl

        cls.repro_root = cls._shared_tmp / "REPRODUCTION"
        cls.repro_pins = _build_collection_fixture(
            cls.repro_root,
            pilot_tpl,
            dataset_split="REPRODUCTION",
            dataset_id="ds_" + "1" * 64,
            collection_id="col_" + "2" * 64,
            mc_value=0.5,
            period_start_utc="2000-01-01T00:00:00Z",
            period_end_utc_exclusive="2018-10-01T00:00:00Z",
            required_month_count=225,
            dense_cluster_points=10,
            base_time=datetime(2012, 1, 1, tzinfo=timezone.utc),
        )

        cls.ext_root = cls._shared_tmp / "EXTENSION"
        cls.ext_pins = _build_collection_fixture(
            cls.ext_root,
            pilot_tpl,
            dataset_split="EXTENSION",
            dataset_id="ds_" + "3" * 64,
            collection_id="col_" + "4" * 64,
            mc_value=0.4,
            period_start_utc="2018-10-01T00:00:00Z",
            period_end_utc_exclusive="2024-01-01T00:00:00Z",
            required_month_count=63,
            dense_cluster_points=14,
            base_time=datetime(2020, 5, 1, tzinfo=timezone.utc),
        )

        configs, pins = evaluation.load_algorithm_configs(project_root=cls.proj_root)
        cls.configs = configs
        cls.config_pins = pins
        repro_eval = evaluation.compare_all_models_on_official(
            cls.repro_root,
            expected_split="REPRODUCTION",
            manifest_sha256=cls.repro_pins["manifest_sha256"],
            checksums_sha256=cls.repro_pins["checksums_sha256"],
            model_configs=configs,
            config_pins=pins,
            repeats=5,
            sigma=0.01,
            seed=42,
            project_root=cls.proj_root,
        )
        repro_details = repro_eval["algorithm_details"]
        cls.base_exp04_report = {
            "report_version": "exp04-official-reproduction-v2",
            "task_id": "EXP-04",
            "status": "PASSED",
            "evaluation_scope": "OFFICIAL_RESEARCH",
            "scientific_approval": False,
            "frozen_rule_published": True,
            "dataset": repro_eval["dataset_info"],
            "algorithm_config_pins": pins,
            "evaluation_parameters": {
                "stability_repeats": 5,
                "perturbation_sigma": 0.01,
                "random_seed": 42,
                "min_omori_aftershocks": 5,
            },
            "exp02_cross_verification": {
                "report_sha256": "a" * 64,
                "report_sha256_verified": True,
                "upstream_status": "PASSED",
                "upstream_evaluation_scope": "OFFICIAL_RESEARCH",
                "upstream_scientific_approval": False,
                "dataset_and_pins_verified": True,
                "config_pins_verified": True,
                "physical_bundles_verified": True,
                "physical_shards_verified": True,
                "primary_membership_parity_verified": True,
                "all_algos_exact_match": True,
                "all_checks_passed": True,
                "algorithms": {
                    algo: {
                        "run_id": f"exp_run_{algo.lower()}_fixture",
                        "matches": True,
                        "exp02_aftershocks": repro_details[algo]["total_aftershocks"],
                        "exp04_aftershocks": repro_details[algo]["total_aftershocks"],
                        "primary_membership_sha256": repro_details[algo]["primary_membership_sha256"],
                        "primary_window_summary_sha256": repro_details[algo]["primary_window_summary_sha256"],
                        "physical_bundle_verified": True,
                        "physical_shards_verified": True,
                        "primary_membership_parity_verified": True,
                    }
                    for algo in ("WINDOW", "DBSCAN", "HDBSCAN_GLOBAL")
                },
            },
            "exp03_cross_verification": {
                "run_id": "exp_run_adaptive_fixture",
                "report_sha256": "b" * 64,
                "report_sha256_verified": True,
                "upstream_status": "PASSED",
                "upstream_evaluation_scope": "OFFICIAL_RESEARCH",
                "upstream_scientific_approval": False,
                "dataset_and_pins_verified": True,
                "config_pins_verified": True,
                "physical_bundles_verified": True,
                "physical_shards_verified": True,
                "primary_membership_parity_verified": True,
                "primary_membership_sha256": repro_details["HDBSCAN_ADAPTIVE"]["primary_membership_sha256"],
                "primary_window_summary_sha256": repro_details["HDBSCAN_ADAPTIVE"]["primary_window_summary_sha256"],
                "exact_match": True,
                "all_checks_passed": True,
                "multi_sequence_reconciliation": {
                    "raw_aftershocks": repro_details["HDBSCAN_ADAPTIVE"]["total_aftershocks"],
                    "resolved_unique_aftershocks": repro_details["HDBSCAN_ADAPTIVE"]["total_aftershocks"],
                    "aftershocks_deduplicated_count": 0,
                    "ambiguous_candidates_count": 0,
                },
            },
            "frozen_extension_rule": {
                "rule_version": ext_eval.FROZEN_EXTENSION_RULE_VERSION,
                "frozen_from_split": "REPRODUCTION",
                "frozen_from_dataset_id": repro_eval["dataset_info"]["dataset_id"],
                "frozen_from_collection_id": repro_eval["dataset_info"]["collection_id"],
                "gold_publication_id": repro_eval["dataset_info"].get("gold_publication_id"),
                "gold_snapshot_id": repro_eval["dataset_info"].get("gold_snapshot_id"),
                "locked_feature_contract": {
                    "window_model_version": "wm_expanded_v1.0",
                    "scaling_config_version": "scaling-global-research-90k-v1",
                    "feature_version": "relative-4d-v1",
                    "mc_value": 0.5,
                },
                "locked_algorithm_configs": {
                    algo: {
                        "config_path": p["config_path"],
                        "model_config_version": p["model_config_version"],
                        "algorithm_version": p["algorithm_version"],
                        "file_bytes_sha256": p["file_bytes_sha256"],
                        "canonical_config_sha256": p["canonical_config_sha256"],
                    }
                    for algo, p in pins.items()
                },
                "extension_guardrails": {
                    "no_parameter_override_on_extension": True,
                    "no_accuracy_or_f1_metric": True,
                    "preserve_failed_windows_in_denominator": True,
                    "preserve_honest_nulls_with_reason_codes": True,
                    "retrospective_candidate_only": True,
                    "scientific_approval": False,
                },
            },
            "evaluation_summary": repro_eval,
        }

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls._shared_tmp, ignore_errors=True)

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="exp05_case_"))
        self.proj = self.tmp / "repo"
        shutil.copytree(self.proj_root / "config", self.proj / "config")
        self.exp04_path = self.tmp / "exp04_report.json"
        self.exp04_sha = self._write_exp04(self.base_exp04_report)
        self.ext_bundle = harness.load_feature_bundle(
            self.ext_root,
            mode=harness.MODE_OFFICIAL,
            manifest_sha256=self.ext_pins["manifest_sha256"],
            checksums_sha256=self.ext_pins["checksums_sha256"],
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_exp04(self, payload: dict) -> str:
        raw = json.dumps(payload, indent=2).encode("utf-8")
        self.exp04_path.write_bytes(raw)
        return harness.sha256_bytes(raw)

    def test_rejects_missing_untrusted_or_tampered_exp04_report_sha256(self) -> None:
        for bad_sha in (None, "", "not-a-sha", "0" * 64):
            with self.assertRaisesRegex(ValueError, "EXP05_EXP04_"):
                ext_eval.verify_reproduction_freeze_gate(
                    self.exp04_path,
                    expected_exp04_report_sha256=bad_sha,
                    extension_bundle=self.ext_bundle,
                    project_root=self.proj,
                )

        tampered = copy.deepcopy(self.base_exp04_report)
        tampered["status"] = "FAILED"
        self.exp04_path.write_text(json.dumps(tampered, indent=2), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP05_EXP04_REPORT_SHA_MISMATCH"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=self.exp04_sha,
                extension_bundle=self.ext_bundle,
                project_root=self.proj,
            )

    def test_rejects_failed_or_approved_or_wrong_rule_exp04_report(self) -> None:
        bad_cases = [
            ({"status": "FAILED"}, "EXP05_EXP04_GATE_FAILED"),
            ({"scientific_approval": True}, "EXP05_EXP04_GATE_FAILED"),
            ({"frozen_extension_rule": {**self.base_exp04_report["frozen_extension_rule"], "rule_version": "exp04-frozen-reproduction-rule-v1"}}, "EXP05_FROZEN_RULE_VERSION_MISMATCH"),
            ({"frozen_extension_rule": {**self.base_exp04_report["frozen_extension_rule"], "frozen_from_split": "PILOT"}}, "EXP05_FROZEN_RULE_INVALID_SPLIT"),
        ]
        for patch, err_regex in bad_cases:
            bad_report = copy.deepcopy(self.base_exp04_report)
            bad_report.update(patch)
            sha = self._write_exp04(bad_report)
            with self.assertRaisesRegex(ValueError, err_regex):
                ext_eval.verify_reproduction_freeze_gate(
                    self.exp04_path,
                    expected_exp04_report_sha256=sha,
                    extension_bundle=self.ext_bundle,
                    project_root=self.proj,
                )

    def test_rejects_custom_configs_and_on_disk_config_tampering(self) -> None:
        with self.assertRaisesRegex(ValueError, "EXP05_CONFIG_OVERRIDE_FORBIDDEN"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=self.exp04_sha,
                extension_bundle=self.ext_bundle,
                project_root=self.proj,
                custom_configs=copy.deepcopy(self.configs),
            )

        dbscan_path = self.proj / "config/ml/models/dbscan_global_dev_v1.json"
        dbscan_cfg = json.loads(dbscan_path.read_text(encoding="utf-8"))
        dbscan_cfg["eps"] = 0.75
        dbscan_path.write_text(json.dumps(dbscan_cfg, indent=2), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP05_CONFIG_FREEZE_MISMATCH"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=self.exp04_sha,
                extension_bundle=self.ext_bundle,
                project_root=self.proj,
            )

    def test_rejects_evaluation_parameter_overrides(self) -> None:
        for kwargs in (
            {"repeats": 3},
            {"sigma": 0.05},
            {"seed": 99},
            {"min_omori_aftershocks": 10},
        ):
            with self.assertRaisesRegex(ValueError, "EXP05_EVAL_PARAM_OVERRIDE_FORBIDDEN"):
                ext_eval.verify_reproduction_freeze_gate(
                    self.exp04_path,
                    expected_exp04_report_sha256=self.exp04_sha,
                    extension_bundle=self.ext_bundle,
                    project_root=self.proj,
                    **kwargs,
                )

    def test_rejects_unsupported_min_omori_aftershocks_even_when_matching_frozen_report(self) -> None:
        bad_report = copy.deepcopy(self.base_exp04_report)
        bad_report["evaluation_parameters"]["min_omori_aftershocks"] = 99
        sha = self._write_exp04(bad_report)
        with self.assertRaisesRegex(ValueError, "EXP05_UNSUPPORTED_MIN_OMORI_AFTERSHOCKS"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=sha,
                extension_bundle=self.ext_bundle,
                project_root=self.proj,
                min_omori_aftershocks=99,
            )

        cli = _load_cli_module()
        with self.assertRaisesRegex(ValueError, "EXP05_UNSUPPORTED_MIN_OMORI_AFTERSHOCKS"):
            cli.execute_official_extension(
                bundle_dir=self.ext_root,
                exp04_report_path=self.exp04_path,
                expected_exp04_report_sha256=sha,
                output_dir=self.tmp / "blocked_omori99",
                output_json=self.tmp / "blocked_omori99.json",
                output_md=self.tmp / "blocked_omori99.md",
                handoff_path=None,
                expected_handoff_sha256=None,
                manifest_sha256=self.ext_pins["manifest_sha256"],
                checksums_sha256=self.ext_pins["checksums_sha256"],
                min_omori_aftershocks=99,
                project_root=self.proj,
            )

    def test_rejects_missing_or_failed_exp02_and_exp03_cross_verifications_even_with_valid_sha(self) -> None:
        # Missing exp02_cross_verification
        no_exp02 = copy.deepcopy(self.base_exp04_report)
        no_exp02.pop("exp02_cross_verification")
        sha = self._write_exp04(no_exp02)
        with self.assertRaisesRegex(ValueError, "EXP05_EXP02_CROSS_VERIFY_REQUIRED"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=sha,
                extension_bundle=self.ext_bundle,
                project_root=self.proj,
            )

        # Missing exp03_cross_verification
        no_exp03 = copy.deepcopy(self.base_exp04_report)
        no_exp03.pop("exp03_cross_verification")
        sha = self._write_exp04(no_exp03)
        with self.assertRaisesRegex(ValueError, "EXP05_EXP03_CROSS_VERIFY_REQUIRED"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=sha,
                extension_bundle=self.ext_bundle,
                project_root=self.proj,
            )

        # Disabled / mismatched exp02 flags even with valid SHA
        for mutator in (
            lambda r: r["exp02_cross_verification"].update({"all_checks_passed": False}),
            lambda r: r["exp02_cross_verification"].update({"physical_shards_verified": False}),
            lambda r: r["exp02_cross_verification"].update({"upstream_status": "FAILED"}),
            lambda r: r["exp02_cross_verification"]["algorithms"]["DBSCAN"].update({"matches": False}),
            lambda r: r["exp02_cross_verification"]["algorithms"]["WINDOW"].update({"primary_membership_sha256": "0" * 64}),
        ):
            bad = copy.deepcopy(self.base_exp04_report)
            mutator(bad)
            sha = self._write_exp04(bad)
            with self.assertRaisesRegex(ValueError, "EXP05_EXP02_CROSS_VERIFY_FAILED"):
                ext_eval.verify_reproduction_freeze_gate(
                    self.exp04_path,
                    expected_exp04_report_sha256=sha,
                    extension_bundle=self.ext_bundle,
                    project_root=self.proj,
                )

        # Disabled / mismatched exp03 flags even with valid SHA
        for mutator in (
            lambda r: r["exp03_cross_verification"].update({"all_checks_passed": False}),
            lambda r: r["exp03_cross_verification"].update({"exact_match": False}),
            lambda r: r["exp03_cross_verification"].update({"primary_membership_parity_verified": False}),
            lambda r: r["exp03_cross_verification"].update({"primary_membership_sha256": "0" * 64}),
            lambda r: r["exp03_cross_verification"].pop("multi_sequence_reconciliation"),
        ):
            bad = copy.deepcopy(self.base_exp04_report)
            mutator(bad)
            sha = self._write_exp04(bad)
            with self.assertRaisesRegex(ValueError, "EXP05_EXP03_CROSS_VERIFY_FAILED"):
                ext_eval.verify_reproduction_freeze_gate(
                    self.exp04_path,
                    expected_exp04_report_sha256=sha,
                    extension_bundle=self.ext_bundle,
                    project_root=self.proj,
                )

        # Real committed EXP-04_OFFICIAL_REPRODUCTION.json passes strict upstream cross-check validation
        real_exp04 = ROOT / "docs/evidence/EXP-04_OFFICIAL_REPRODUCTION.json"
        if real_exp04.is_file():
            ext_eval._verify_exp04_upstream_cross_checks(json.loads(real_exp04.read_bytes()))

    def test_rejects_unpublished_rule_and_missing_cross_check_digests(self) -> None:
        for field in ("frozen_rule_published", "EXP-02 summary SHA", "EXP-03 membership SHA"):
            with self.subTest(field=field):
                report = copy.deepcopy(self.base_exp04_report)
                if field == "frozen_rule_published":
                    report[field] = False
                    reason = "EXP05_FROZEN_RULE_NOT_PUBLISHED"
                elif field.startswith("EXP-02"):
                    report["exp02_cross_verification"]["algorithms"]["DBSCAN"].pop("primary_window_summary_sha256")
                    report["evaluation_summary"]["algorithm_details"]["DBSCAN"].pop("primary_window_summary_sha256")
                    reason = "EXP05_EXP02_CROSS_VERIFY_FAILED"
                else:
                    report["exp03_cross_verification"].pop("primary_membership_sha256")
                    report["evaluation_summary"]["algorithm_details"]["HDBSCAN_ADAPTIVE"].pop("primary_membership_sha256")
                    reason = "EXP05_EXP03_CROSS_VERIFY_FAILED"
                sha = self._write_exp04(report)
                with self.assertRaisesRegex(ValueError, reason):
                    ext_eval.verify_reproduction_freeze_gate(
                        self.exp04_path,
                        expected_exp04_report_sha256=sha,
                        extension_bundle=self.ext_bundle,
                        project_root=self.proj,
                    )

    def test_rejects_non_extension_split_same_dataset_and_contract_mismatch(self) -> None:
        repro_bundle = harness.load_feature_bundle(
            self.repro_root,
            mode=harness.MODE_OFFICIAL,
            manifest_sha256=self.repro_pins["manifest_sha256"],
            checksums_sha256=self.repro_pins["checksums_sha256"],
        )
        with self.assertRaisesRegex(ValueError, "EXP05_REQUIRES_EXTENSION_SPLIT"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=self.exp04_sha,
                extension_bundle=repro_bundle,
                project_root=self.proj,
            )

        dup_bundle = copy.deepcopy(self.ext_bundle)
        dup_bundle["manifest"] = dict(self.ext_bundle["manifest"], dataset_id=self.base_exp04_report["dataset"]["dataset_id"])
        with self.assertRaisesRegex(ValueError, "EXP05_REQUIRES_DISTINCT_DATASET_ID"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=self.exp04_sha,
                extension_bundle=dup_bundle,
                project_root=self.proj,
            )

        bad_feat = copy.deepcopy(self.ext_bundle)
        bad_feat["manifest"] = dict(self.ext_bundle["manifest"], feature_version="relative-4d-v99")
        with self.assertRaisesRegex(ValueError, "EXP05_FEATURE_CONTRACT_MISMATCH"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=self.exp04_sha,
                extension_bundle=bad_feat,
                project_root=self.proj,
            )

        bad_gold = copy.deepcopy(self.ext_bundle)
        bad_gold["manifest"] = dict(self.ext_bundle["manifest"], gold_publication_id="pub_" + "9" * 64)
        with self.assertRaisesRegex(ValueError, "EXP05_GOLD_PUBLICATION_MISMATCH"):
            ext_eval.verify_reproduction_freeze_gate(
                self.exp04_path,
                expected_exp04_report_sha256=self.exp04_sha,
                extension_bundle=bad_gold,
                project_root=self.proj,
            )

    def test_end_to_end_extension_run_verifies_bundles_shards_drift_and_parity(self) -> None:
        out_dir = self.tmp / "exp05_bundles"
        res = ext_eval.run_extension_bundles_and_evaluate(
            bundle_dir=self.ext_root,
            exp04_report_path=self.exp04_path,
            expected_exp04_report_sha256=self.exp04_sha,
            output_dir=out_dir,
            manifest_sha256=self.ext_pins["manifest_sha256"],
            checksums_sha256=self.ext_pins["checksums_sha256"],
            code_version="f" * 40,
            allow_overwrite=False,
            is_retry=False,
            project_root=self.proj,
        )

        comp = res["comparison"]
        fg = comp["freeze_gate"]
        self.assertEqual(fg["gate_status"], "PASSED")
        self.assertEqual(fg["mc_drift"], -0.1)
        self.assertTrue(fg["gold_pin_verified"])

        # Data drift reflects fixture deltas cleanly
        drift = comp["data_drift"]
        self.assertEqual(drift["reproduction"]["mc_value"], 0.5)
        self.assertEqual(drift["extension"]["mc_value"], 0.4)
        self.assertGreater(drift["shifts"]["feature_row_count_delta"], 0)
        self.assertEqual(drift["extension"]["mainshock_count"], 2)

        # Sibling MLI-03 bundles exist, lack checksums.sha256, and preserve failed windows in denominator
        for algo, subdir in ext_eval.ALGO_OUTPUT_SUBDIRS.items():
            full_bundle_dir = out_dir / subdir
            shards_dir = out_dir / f"{subdir}_shards"
            self.assertTrue((full_bundle_dir / "_SUCCESS.json").is_file())
            self.assertFalse((full_bundle_dir / "checksums.sha256").exists())
            self.assertTrue((shards_dir / "shard_00000" / "_SUCCESS.json").is_file())
            self.assertTrue((shards_dir / "shard_00001" / "_SUCCESS.json").is_file())

            ac = comp["algorithm_comparisons"][algo]
            self.assertEqual(ac["extension"]["total_windows"], 2)
            self.assertEqual(
                ac["extension"]["success_windows"] + ac["extension"]["failure_windows"],
                2,
            )
            self.assertTrue(ac["bundle_verification"]["physical_bundle_verified"])
            self.assertTrue(ac["bundle_verification"]["physical_shards_verified"])
            self.assertTrue(ac["bundle_verification"]["primary_membership_parity_verified"])

        # WINDOW has 0% failure and honest NULL DBCV (never 0.0)
        win_ext = comp["algorithm_comparisons"]["WINDOW"]["extension"]
        self.assertEqual(win_ext["failure_windows"], 0)
        self.assertIsNone(win_ext["mean_dbcv_score"])
        self.assertEqual(win_ext["dbcv_reason_counts"], {ext_eval.METRIC_NOT_APPLICABLE: 2})

        # Isolated shard_00001 mainshock preserves failed window in denominator on density baselines
        dbscan_ext = comp["algorithm_comparisons"]["DBSCAN"]["extension"]
        self.assertEqual(dbscan_ext["failure_windows"], 1)
        self.assertEqual(dbscan_ext["total_windows"], 2)
        self.assertAlmostEqual(dbscan_ext["failure_rate"], 0.5, places=4)

        # Guardrails & zero forbidden metrics
        g = comp["guardrails"]
        self.assertTrue(g["is_temporal_generalization_check"])
        self.assertFalse(g["is_supervised_accuracy"])
        self.assertFalse(g["parameter_tuning_applied_on_extension"])
        self.assertFalse(g["earthquake_prediction_claimed"])
        self.assertFalse(g["scientific_approval"])
        ext_eval.assert_no_forbidden_metrics(comp)

        with self.assertRaisesRegex(ValueError, "EXP05_FORBIDDEN_SUPERVISED_METRIC"):
            ext_eval.assert_no_forbidden_metrics({"nested": {"f1_score": 0.9}})

    def test_cli_execute_official_extension_and_real_exp04_sha_pin_resolution(self) -> None:
        cli = _load_cli_module()
        real_exp04 = ROOT / "docs/evidence/EXP-04_OFFICIAL_REPRODUCTION.json"
        if real_exp04.is_file():
            lf_bytes = real_exp04.read_bytes().replace(b"\r\n", b"\n")
            for newline in (b"\n", b"\r\n"):
                with self.subTest(newline=newline):
                    report_bytes = lf_bytes.replace(b"\n", newline)
                    path = self.tmp / "official-exp04.json"
                    path.write_bytes(report_bytes)
                    self.assertEqual(harness.sha256_bytes(report_bytes), cli.resolve_default_exp04_report_sha256(path))

        out_dir = self.tmp / "cli_bundles"
        out_json = self.tmp / "EXP-05_TEST.json"
        out_md = self.tmp / "EXP-05_TEST.md"
        report = cli.execute_official_extension(
            bundle_dir=self.ext_root,
            exp04_report_path=self.exp04_path,
            expected_exp04_report_sha256=self.exp04_sha,
            output_dir=out_dir,
            output_json=out_json,
            output_md=out_md,
            handoff_path=None,
            expected_handoff_sha256=None,
            manifest_sha256=self.ext_pins["manifest_sha256"],
            checksums_sha256=self.ext_pins["checksums_sha256"],
            repeats=5,
            sigma=0.01,
            seed=42,
            min_omori_aftershocks=5,
            allow_overwrite=False,
            is_retry=False,
            project_root=self.proj,
        )
        self.assertEqual(report["task_id"], "EXP-05")
        self.assertEqual(report["status"], "PASSED")
        self.assertFalse(report["scientific_approval"])
        self.assertTrue(out_json.is_file())
        self.assertTrue(out_md.is_file())
        md_text = out_md.read_text(encoding="utf-8")
        self.assertIn("Out-of-Period", md_text)
        self.assertIn("scientific_approval=false", md_text)
        self.assertIn("accuracy", md_text.lower())  # mentions ban on accuracy/F1 in prose guardrail


if __name__ == "__main__":
    unittest.main()
