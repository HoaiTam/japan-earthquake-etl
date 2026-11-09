"""Unit and integration tests for MLI-04: Trino Views and Static Experiment Report.

Verifies:
1. SQL View Syntax & Real Schema Conformity:
   - View definitions match exact physical schema produced by SparkMlIcebergWriter and DatasetManifestWriter.
   - Direct execution of SQL view queries from files against real-schema fixture tables.
   - Strict gate filter: only CANDIDATE and APPROVED runs are visible.
   - Zero credential or forbidden path leaks.
2. Grain & Metric Integrity:
   - v_algorithm_comparison preserves experiment_run_id grain, preventing aggregation across independent runs.
   - Denominator integrity: failure windows are retained in window counts and failure rate.
   - Honest NULL policy: DBCV is NULL for WINDOW, not coerced to 0.0.
   - Numeric zero integrity: legitimate 0.0 metrics (noise, dbcv, stability) are preserved, not converted to N/A.
3. Multi-Sequence Resolution:
   - Strictly requires resolution_rank = 1 for resolved unique aftershocks.
   - Rows with resolution_rank IS NULL (including AMBIGUOUS) and resolution_rank > 1 are NEVER considered winners.
4. Static Report Generator & Publication Gate Fail-Closed:
   - Input {} or empty run list with --mode official raises ValueError (fail-closed).
   - Auto mode rejects unapproved runs (e.g. TRAINING_EXTERNAL) from producing official reports.
   - Development mode produces DEVELOPMENT_ONLY badge and explicitly marks SQL parity as CHƯA NGHIỆM THU.
5. Data Dictionary Completeness:
   - Documents all views, updated schemas, and scientific reason codes.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import unittest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

_spec = importlib.util.spec_from_file_location(
    "generate_experiment_report", str(SCRIPTS_DIR / "generate-experiment-report.py")
)
report_gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report_gen)


def adapt_trino_view_sql_for_sqlite(sql_content: str, view_name: str) -> str:
    """Extract view SELECT query from Trino DDL and adapt namespace for SQLite execution."""
    match = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+[a-zA-Z0-9_\.]+\s+AS\s+(.*)", sql_content, re.DOTALL | re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find CREATE VIEW statement in SQL for {view_name}")
    query = match.group(1).strip()
    if query.endswith(";"):
        query = query[:-1].strip()
    # Replace iceberg.ml. with empty prefix so it references local SQLite tables
    query = query.replace("iceberg.ml.", "")
    # In SQLite, NULLIF and CAST ... AS double are compatible
    return f"CREATE VIEW {view_name} AS {query};"


class TestMli04ViewsAndReport(unittest.TestCase):
    """Test suite for MLI-04 deliverables and review fixes."""

    @classmethod
    def setUpClass(cls):
        cls.sql_dir = PROJECT_ROOT / "sql" / "ml"
        cls.report_script = PROJECT_ROOT / "scripts" / "generate-experiment-report.py"
        cls.data_dict = PROJECT_ROOT / "docs" / "specs" / "ML_REPORT_DATA_DICTIONARY.md"
        cls.pilot_eval_json = PROJECT_ROOT / "docs" / "evidence" / "EXP-04_PILOT_EVALUATION.json"

    def test_sql_view_files_exist(self):
        """Verify that all required Trino view SQL files and verification scripts exist."""
        required_files = [
            "v_experiment_runs.sql",
            "v_algorithm_comparison.sql",
            "v_sequence_memberships_resolved.sql",
            "v_failure_windows.sql",
            "ml_verification_queries.sql",
        ]
        for fname in required_files:
            file_path = self.sql_dir / fname
            self.assertTrue(file_path.exists(), f"Missing required SQL file: {fname}")
            content = file_path.read_text(encoding="utf-8")
            self.assertGreater(len(content.strip()), 50, f"File {fname} is suspiciously short")

    def test_sql_publication_gate_in_views(self):
        """Verify that all serving views strictly enforce publication gate (CANDIDATE/APPROVED)."""
        view_files = [
            "v_experiment_runs.sql",
            "v_algorithm_comparison.sql",
            "v_sequence_memberships_resolved.sql",
            "v_failure_windows.sql",
        ]
        gate_pattern = re.compile(r"experiment_status\s+IN\s*\(\s*'CANDIDATE'\s*,\s*'APPROVED'\s*\)", re.IGNORECASE)
        for fname in view_files:
            content = (self.sql_dir / fname).read_text(encoding="utf-8")
            self.assertTrue(
                gate_pattern.search(content),
                f"Serving view {fname} must contain gate filter: experiment_status IN ('CANDIDATE', 'APPROVED')",
            )

    def test_sql_no_credentials_or_forbidden_paths(self):
        """Verify that SQL files contain no credentials, private tokens, or user home paths."""
        forbidden_regex = re.compile(
            r"(?i)(?:password|access_token|secret_key|private_key|gh[pousr]_[A-Za-z0-9]{36,}|/Users/|/home/|[A-Za-z]:\\Users\\)"
        )
        for sql_file in self.sql_dir.glob("*.sql"):
            content = sql_file.read_text(encoding="utf-8")
            match = forbidden_regex.search(content)
            self.assertIsNone(match, f"Found forbidden secret or path in {sql_file.name}: {match.group(0) if match else ''}")

    def test_v_experiment_runs_uses_real_schema_columns(self):
        """Verify that v_experiment_runs.sql references valid schema columns and not obsolete names."""
        content = (self.sql_dir / "v_experiment_runs.sql").read_text(encoding="utf-8")
        self.assertIn("e.result_artifact_uri", content, "Must use result_artifact_uri")
        self.assertNotIn("e.artifact_uri", content, "artifact_uri does not exist in experiment_run schema")
        self.assertIn("e.result_ready_at_utc", content, "Must use result_ready_at_utc")
        self.assertNotIn("e.completed_at_utc", content, "completed_at_utc does not exist in experiment_run schema")
        self.assertNotIn("e.package_version", content, "package_version does not exist in experiment_run schema")

    def test_direct_sql_execution_on_real_schema(self):
        """Directly execute SQL view DDL from files against SQLite tables matching real schema."""
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()

        # Real schema matching SparkMlIcebergWriter and DatasetManifestWriter
        cursor.executescript("""
        CREATE TABLE dataset_manifest (
            schema_version TEXT,
            dataset_id TEXT PRIMARY KEY,
            dataset_split TEXT,
            gold_table_name TEXT,
            physical_gold_table_name TEXT,
            gold_publication_id TEXT,
            publication_bundle_sha256 TEXT,
            identity_payload_sha256 TEXT,
            filter_config_json TEXT,
            mc_method TEXT,
            mc_region_version TEXT,
            mc_config_json TEXT,
            mc_config_sha256 TEXT,
            exclusion_counts_json TEXT,
            audit_report_uri TEXT,
            feature_schema_sha256 TEXT,
            export_artifact_uri TEXT,
            export_manifest_sha256 TEXT,
            dataset_status TEXT,
            status_reason_code TEXT,
            build_run_id TEXT,
            dataset_config_version TEXT,
            audit_rule_version TEXT,
            mc_method_version TEXT,
            window_model_version TEXT,
            feature_version TEXT,
            scaling_config_version TEXT,
            code_version TEXT,
            period_start_utc TEXT,
            period_end_utc_exclusive TEXT,
            observation_cutoff_utc TEXT,
            created_at_utc TEXT,
            updated_at_utc TEXT,
            gold_snapshot_id INTEGER,
            input_event_count INTEGER,
            eligible_event_count INTEGER,
            excluded_event_count INTEGER,
            mainshock_count INTEGER,
            candidate_row_count INTEGER,
            feature_row_count INTEGER,
            mc_value REAL
        );

        CREATE TABLE experiment_run (
            schema_version TEXT,
            experiment_run_id TEXT PRIMARY KEY,
            dataset_id TEXT,
            algorithm_name TEXT,
            algorithm_version TEXT,
            model_config_version TEXT,
            experiment_config_json TEXT,
            experiment_config_sha256 TEXT,
            code_version TEXT,
            notebook_version TEXT,
            requirements_lock_sha256 TEXT,
            runtime_environment_json TEXT,
            random_seed INTEGER,
            result_artifact_uri TEXT,
            result_bundle_sha256 TEXT,
            membership_row_count INTEGER,
            summary_row_count INTEGER,
            import_run_id TEXT,
            membership_snapshot_id INTEGER,
            summary_snapshot_id INTEGER,
            experiment_status TEXT,
            status_reason_code TEXT,
            created_at_utc TEXT,
            result_ready_at_utc TEXT,
            imported_at_utc TEXT,
            approved_at_utc TEXT,
            approved_by TEXT,
            dataset_registry_snapshot_id TEXT,
            candidate_source_snapshot_id TEXT,
            mainshock_source_snapshot_id TEXT,
            dataset_manifest_sha256 TEXT,
            export_manifest_sha256 TEXT
        );

        CREATE TABLE sequence_summary (
            schema_version TEXT,
            experiment_run_id TEXT,
            dataset_id TEXT,
            mainshock_event_id TEXT,
            algorithm_name TEXT,
            model_config_version TEXT,
            window_result_status TEXT,
            productive_sequence_detected INTEGER,
            productive_definition_version TEXT,
            candidate_count INTEGER,
            cluster_count INTEGER,
            noise_count INTEGER,
            noise_ratio REAL,
            sequence_member_count INTEGER,
            aftershock_count INTEGER,
            foreshock_candidate_count INTEGER,
            sequence_duration_hours REAL,
            largest_aftershock_magnitude REAL,
            magnitude_difference REAL,
            spatial_extent_km REAL,
            mean_membership_probability REAL,
            dbcv_score REAL,
            omori_k REAL,
            omori_c REAL,
            omori_p REAL,
            omori_fit_metric REAL,
            stability_components_json TEXT,
            runtime_seconds REAL,
            peak_memory_mb REAL,
            is_nested_mainshock_candidate INTEGER,
            parent_mainshock_event_id TEXT,
            nested_membership_probability REAL,
            metric_reason_codes TEXT,
            failure_reason_code TEXT,
            created_at_utc TEXT
        );

        CREATE TABLE sequence_membership (
            schema_version TEXT,
            experiment_run_id TEXT,
            dataset_id TEXT,
            mainshock_event_id TEXT,
            candidate_event_id TEXT,
            algorithm_name TEXT,
            model_config_version TEXT,
            cluster_id INTEGER,
            is_noise INTEGER,
            is_mainshock_cluster_member INTEGER,
            is_sequence_member INTEGER,
            membership_probability REAL,
            event_role_candidate TEXT,
            normalized_distance REAL,
            multi_sequence_status TEXT,
            resolution_rank INTEGER,
            created_at_utc TEXT
        );
        """)

        # Adapt and execute view DDL directly from the SQL files
        for vname in ["v_experiment_runs", "v_algorithm_comparison", "v_sequence_memberships_resolved", "v_failure_windows"]:
            sql_file = self.sql_dir / f"{vname}.sql"
            sqlite_ddl = adapt_trino_view_sql_for_sqlite(sql_file.read_text(encoding="utf-8"), vname)
            cursor.execute(sqlite_ddl)

        # Insert dataset
        cursor.execute("""
        INSERT INTO dataset_manifest (dataset_id, dataset_split, gold_table_name, gold_snapshot_id, period_start_utc, period_end_utc_exclusive, feature_version, scaling_config_version)
        VALUES ('ds_real_01', 'REPRODUCTION', 'iceberg.gold.earthquake_event_current', 10001, '2018-01-01', '2018-10-01', 'f1', 's1');
        """)

        # Insert two DBSCAN runs with same algorithm & config but different run_id and seeds
        # Run 1: exp_dbs_seed42 (CANDIDATE, seed 42)
        # Run 2: exp_dbs_seed43 (CANDIDATE, seed 43)
        # Run 3: exp_unapproved (TRAINING_EXTERNAL)
        cursor.executescript("""
        INSERT INTO experiment_run (experiment_run_id, dataset_id, algorithm_name, model_config_version, code_version, random_seed, experiment_status, result_artifact_uri, result_ready_at_utc, created_at_utc)
        VALUES
        ('exp_dbs_seed42', 'ds_real_01', 'DBSCAN', 'dbs-v1', 'commit_42', 42, 'CANDIDATE', 's3://art/dbs42', '2026-11-01', '2026-11-01'),
        ('exp_dbs_seed43', 'ds_real_01', 'DBSCAN', 'dbs-v1', 'commit_43', 43, 'CANDIDATE', 's3://art/dbs43', '2026-11-01', '2026-11-01'),
        ('exp_unapproved', 'ds_real_01', 'WINDOW', 'win-v1', 'commit_un', 0, 'TRAINING_EXTERNAL', 's3://art/un', '2026-11-01', '2026-11-01');

        INSERT INTO sequence_summary (experiment_run_id, dataset_id, mainshock_event_id, algorithm_name, model_config_version, window_result_status, candidate_count, sequence_member_count, aftershock_count, noise_ratio, dbcv_score, omori_p, failure_reason_code, runtime_seconds)
        VALUES
        ('exp_dbs_seed42', 'ds_real_01', 'main_1', 'DBSCAN', 'dbs-v1', 'SUCCESS', 10, 8, 8, 0.05, 0.35, 0.95, NULL, 1.2),
        ('exp_dbs_seed42', 'ds_real_01', 'main_2', 'DBSCAN', 'dbs-v1', 'FAILED', 5, 0, 0, NULL, NULL, NULL, 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 0.4),
        ('exp_dbs_seed43', 'ds_real_01', 'main_1', 'DBSCAN', 'dbs-v1', 'SUCCESS', 10, 7, 7, 0.08, 0.31, 0.92, NULL, 1.1),
        ('exp_dbs_seed43', 'ds_real_01', 'main_2', 'DBSCAN', 'dbs-v1', 'SUCCESS', 5, 4, 4, 0.10, 0.28, 0.88, NULL, 0.9);
        """)

        # 1. Test v_experiment_runs excludes unapproved
        cursor.execute("SELECT experiment_run_id, result_artifact_uri FROM v_experiment_runs")
        runs = cursor.fetchall()
        run_ids = [r[0] for r in runs]
        self.assertIn("exp_dbs_seed42", run_ids)
        self.assertIn("exp_dbs_seed43", run_ids)
        self.assertNotIn("exp_unapproved", run_ids)
        self.assertEqual(runs[0][1], "s3://art/dbs42")

        # 2. Test v_algorithm_comparison preserves grain by experiment_run_id (Review Point 6)
        cursor.execute("SELECT experiment_run_id, total_windows, success_windows, failure_windows, raw_aftershock_memberships FROM v_algorithm_comparison ORDER BY experiment_run_id")
        comp_rows = cursor.fetchall()
        self.assertEqual(len(comp_rows), 2, "Must produce 2 distinct rows for 2 distinct runs, NOT collapsed into 1 row!")

        # Row 1: seed 42 (1 success, 1 failure, total 2, aftershocks 8)
        self.assertEqual(comp_rows[0][0], "exp_dbs_seed42")
        self.assertEqual(comp_rows[0][1], 2)
        self.assertEqual(comp_rows[0][2], 1)
        self.assertEqual(comp_rows[0][3], 1)
        self.assertEqual(comp_rows[0][4], 8)

        # Row 2: seed 43 (2 successes, 0 failures, total 2, aftershocks 11)
        self.assertEqual(comp_rows[1][0], "exp_dbs_seed43")
        self.assertEqual(comp_rows[1][1], 2)
        self.assertEqual(comp_rows[1][2], 2)
        self.assertEqual(comp_rows[1][3], 0)
        self.assertEqual(comp_rows[1][4], 11)

        # 3. Test v_failure_windows captures failed window
        cursor.execute("SELECT experiment_run_id, mainshock_event_id, failure_reason_code FROM v_failure_windows")
        fail_rows = cursor.fetchall()
        self.assertEqual(len(fail_rows), 1)
        self.assertEqual(fail_rows[0][0], "exp_dbs_seed42")
        self.assertEqual(fail_rows[0][1], "main_2")
        self.assertEqual(fail_rows[0][2], "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")

        conn.close()

    def test_multi_sequence_resolution_ranks_and_ambiguous(self):
        """Test v_sequence_memberships_resolved resolution logic across rank 1, rank > 1, and AMBIGUOUS."""
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.executescript("""
        CREATE TABLE experiment_run (
            experiment_run_id TEXT PRIMARY KEY,
            experiment_status TEXT
        );

        CREATE TABLE sequence_membership (
            experiment_run_id TEXT,
            dataset_id TEXT,
            mainshock_event_id TEXT,
            candidate_event_id TEXT,
            algorithm_name TEXT,
            model_config_version TEXT,
            cluster_id INTEGER,
            is_noise INTEGER,
            is_mainshock_cluster_member INTEGER,
            is_sequence_member INTEGER,
            membership_probability REAL,
            event_role_candidate TEXT,
            normalized_distance REAL,
            multi_sequence_status TEXT,
            resolution_rank INTEGER,
            created_at_utc TEXT
        );
        """)

        sql_file = self.sql_dir / "v_sequence_memberships_resolved.sql"
        sqlite_ddl = adapt_trino_view_sql_for_sqlite(sql_file.read_text(encoding="utf-8"), "v_sequence_memberships_resolved")
        cursor.execute(sqlite_ddl)

        cursor.executescript("""
        INSERT INTO experiment_run VALUES ('exp_res', 'CANDIDATE');

        INSERT INTO sequence_membership VALUES
        -- Case 1: Winner rank 1 POST -> is_resolved_unique_aftershock = true (1)
        ('exp_res', 'ds1', 'm1', 'cand_winner', 'DBSCAN', 'v1', 0, 0, 1, 1, 0.9, 'POST', 0.1, 'MULTIPLE', 1, '2026-11-01'),

        -- Case 2: Tied loser rank 2 POST -> is_resolved_unique_aftershock = false (0)
        ('exp_res', 'ds1', 'm2', 'cand_winner', 'DBSCAN', 'v1', 0, 0, 1, 1, 0.9, 'POST', 0.3, 'MULTIPLE', 2, '2026-11-01'),

        -- Case 3: AMBIGUOUS candidate with resolution_rank = NULL -> is_resolved_unique_aftershock = false (0)
        ('exp_res', 'ds1', 'm1', 'cand_ambig', 'DBSCAN', 'v1', 0, 0, 1, 1, 0.5, 'POST', 0.5, 'AMBIGUOUS', NULL, '2026-11-01'),

        -- Case 4: NOT_MEMBER candidate with resolution_rank = NULL -> is_resolved_unique_aftershock = false (0)
        ('exp_res', 'ds1', 'm1', 'cand_noise', 'DBSCAN', 'v1', -1, 1, 0, 0, 0.0, 'POST', 0.9, 'NOT_MEMBER', NULL, '2026-11-01'),

        -- Case 5: MAINSHOCK role with resolution_rank = 1 -> is_resolved_unique_aftershock = false (0) (not POST!)
        ('exp_res', 'ds1', 'm1', 'm1', 'DBSCAN', 'v1', 0, 0, 1, 1, 1.0, 'MAINSHOCK', 0.0, 'UNIQUE', 1, '2026-11-01');
        """)

        cursor.execute("""
        SELECT candidate_event_id, resolution_rank, multi_sequence_status, is_resolved_unique_aftershock
        FROM v_sequence_memberships_resolved
        WHERE mainshock_event_id = 'm1'
        ORDER BY candidate_event_id;
        """)
        results = {r[0]: (r[1], r[2], bool(r[3])) for r in cursor.fetchall()}

        # cand_winner must be resolved as true (rank 1)
        self.assertTrue(results["cand_winner"][2], "Rank 1 winner must be resolved as unique aftershock")

        # cand_ambig must be false (rank NULL)
        self.assertFalse(results["cand_ambig"][2], "AMBIGUOUS candidate with NULL rank must NEVER be marked as resolved unique aftershock")

        # cand_noise must be false
        self.assertFalse(results["cand_noise"][2], "NOT_MEMBER must be false")

        # m1 (MAINSHOCK) must be false
        self.assertFalse(results["m1"][2], "MAINSHOCK must not be an aftershock")

        # Check m2 record for cand_winner (rank 2)
        cursor.execute("SELECT is_resolved_unique_aftershock FROM v_sequence_memberships_resolved WHERE mainshock_event_id = 'm2'")
        self.assertFalse(bool(cursor.fetchone()[0]), "Rank 2 duplicate must NOT be resolved as unique aftershock")

        conn.close()

    def test_metric_zero_integrity_not_coerced_to_na(self):
        """Verify that legitimate 0.0 numeric values are strictly preserved and not changed to N/A."""
        # 0.0 noise ratio should format as 0.0%
        self.assertEqual(report_gen.format_metric(0.0, fmt=".1f", suffix="%"), "0.0%")
        # 0.0 DBCV should format as 0.0000
        self.assertEqual(report_gen.format_metric(0.0, fmt=".4f"), "0.0000")
        # 0.0 stability should format as 0.0000
        self.assertEqual(report_gen.format_metric(0.0, fmt=".4f"), "0.0000")
        # None should format as N/A
        self.assertEqual(report_gen.format_metric(None), "N/A")

        # In normalized payload extraction, test that 0.0 values are preserved
        mock_raw = {
            "evaluation_summary": {
                "pilot_id": "pilot_zero_test",
                "comparison_table": [{
                    "Algorithm": "TEST_ALGO",
                    "Total Windows": 1,
                    "Success Windows": 1,
                    "Failure Windows": 0,
                    "Failure Rate": 0.0,
                    "Total Aftershocks": 10,
                    "Mean Noise Ratio": 0.0,
                    "Mean DBCV": 0.0,
                    "Mean Omori p": 1.0,
                    "Omori Fitted Windows": "1/1",
                    "Mean Jaccard Stability": 0.0,
                    "Mean ARI Stability": 0.0,
                    "Runtime (s)": 0.5,
                }],
                "algorithm_details": {
                    "TEST_ALGO": {
                        "total_windows": 1,
                        "success_windows": 1,
                        "failure_windows": 0,
                        "failure_rate": 0.0,
                        "mean_noise_ratio": 0.0,
                        "mean_dbcv_score": 0.0,
                        "mean_omori_p": 1.0,
                        "mean_omori_r2": 0.99,
                        "omori_fitted_windows": 1,
                        "mean_jaccard_stability": 0.0,
                        "mean_ari_stability": 0.0,
                        "runtime_seconds": 0.5,
                    }
                }
            }
        }
        normalized = report_gen.extract_normalized_payload(mock_raw)
        row = normalized["comparison_table"][0]
        self.assertEqual(row["mean_noise_ratio"], 0.0)
        self.assertEqual(row["mean_dbcv"], 0.0)
        self.assertEqual(row["mean_jaccard_stability"], 0.0)
        self.assertEqual(row["mean_ari_stability"], 0.0)

        # In details
        detail = normalized["model_analyses"]["TEST_ALGO"]
        self.assertEqual(detail["mean_noise_ratio"], "0.0%")
        self.assertEqual(detail["mean_dbcv"], "0.0000")
        self.assertEqual(detail["mean_jaccard_stability"], "0.0000")
        self.assertEqual(detail["mean_ari_stability"], "0.0000")

    def test_publication_gate_fail_closed_official_mode(self):
        """Verify that official mode rejects empty payload, empty runs, or unapproved states."""
        # 1. Empty payload {} in official mode
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report({}, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))

        # 2. Empty runs list in official mode
        empty_runs_payload = {
            "metadata": {"dataset_id": "ds_official", "git_commit": "c1", "research_ready": True},
            "runs": [],
            "comparison_table": [],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(empty_runs_payload, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))

        # 3. Payload with TRAINING_EXTERNAL run in official mode
        unapproved_payload = {
            "metadata": {"dataset_id": "ds_official", "git_commit": "c1", "research_ready": True},
            "runs": [
                {"experiment_run_id": "exp_ok", "status": "APPROVED", "dataset_id": "ds_official"},
                {"experiment_run_id": "exp_bad", "status": "TRAINING_EXTERNAL", "dataset_id": "ds_official"},
            ],
            "comparison_table": [{
                "algorithm": "WINDOW",
                "experiment_run_id": "exp_ok",
                "dataset_id": "ds_official",
                "total_aftershocks": 10,
                "runtime_seconds": 1.0,
                "model_config_version": "v1",
            }],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(unapproved_payload, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))

        # 4. Auto mode with unapproved runs attempting official publishing
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(unapproved_payload, mode="auto")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))

    def test_publication_gate_rejects_missing_metadata(self):
        """Verify that official mode strictly rejects missing or UNKNOWN_DATASET metadata."""
        # Missing dataset_id
        payload_no_ds = {
            "metadata": {"git_commit": "commit_123", "research_ready": True},
            "runs": [{"experiment_run_id": "exp_ok", "status": "APPROVED"}],
            "comparison_table": [{
                "algorithm": "WINDOW",
                "experiment_run_id": "exp_ok",
                "total_aftershocks": 5,
                "runtime_seconds": 0.5,
                "model_config_version": "v1",
            }],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(payload_no_ds, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))
        self.assertIn("dataset_id", str(ctx.exception))

        # UNKNOWN_DATASET
        payload_unk_ds = {
            "metadata": {"dataset_id": "UNKNOWN_DATASET", "git_commit": "commit_123", "research_ready": True},
            "runs": [{"experiment_run_id": "exp_ok", "status": "APPROVED"}],
            "comparison_table": [{
                "algorithm": "WINDOW",
                "experiment_run_id": "exp_ok",
                "dataset_id": "UNKNOWN_DATASET",
                "total_aftershocks": 5,
                "runtime_seconds": 0.5,
                "model_config_version": "v1",
            }],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(payload_unk_ds, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))

    def test_publication_gate_rejects_missing_status_without_registry(self):
        """Verify that official mode rejects rows missing experiment_status when no registry is provided."""
        payload = {
            "metadata": {"dataset_id": "ds_A", "git_commit": "c1", "research_ready": True},
            "comparison_table": [{
                "algorithm": "WINDOW",
                "experiment_run_id": "exp_no_status",
                "dataset_id": "ds_A",
                "model_config_version": "v1",
                "total_aftershocks": 10,
                "runtime_seconds": 1.0,
            }],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))
        self.assertIn("experiment_status", str(ctx.exception))

    def test_publication_gate_rejects_conflicting_status_with_registry(self):
        """Verify that official mode rejects row status conflicting with registry run status."""
        payload = {
            "metadata": {"dataset_id": "ds_A", "git_commit": "c1", "research_ready": True},
            "runs": [{"experiment_run_id": "exp_1", "status": "APPROVED", "dataset_id": "ds_A", "model_config_version": "v1"}],
            "comparison_table": [{
                "algorithm": "WINDOW",
                "experiment_run_id": "exp_1",
                "dataset_id": "ds_A",
                "experiment_status": "REJECTED",
                "model_config_version": "v1",
                "total_aftershocks": 10,
                "runtime_seconds": 1.0,
            }],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))
        self.assertIn("conflicting status", str(ctx.exception))

    def test_publication_gate_rejects_conflicting_config_with_registry(self):
        """Verify that official mode rejects row config conflicting with registry run config."""
        payload = {
            "metadata": {"dataset_id": "ds_A", "git_commit": "c1", "research_ready": True},
            "runs": [{"experiment_run_id": "exp_1", "status": "APPROVED", "dataset_id": "ds_A", "model_config_version": "v1"}],
            "comparison_table": [{
                "algorithm": "WINDOW",
                "experiment_run_id": "exp_1",
                "dataset_id": "ds_A",
                "experiment_status": "APPROVED",
                "model_config_version": "v2_conflicting",
                "total_aftershocks": 10,
                "runtime_seconds": 1.0,
            }],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))
        self.assertIn("conflicting config version", str(ctx.exception))

    def test_publication_gate_rejects_foreign_or_cross_dataset_rows(self):
        """Verify that official mode rejects rows from foreign runs, cross-datasets, or unapproved runs."""
        # Case 1: Row has experiment_run_id from another run not declared in approved runs
        foreign_row_payload = {
            "metadata": {"dataset_id": "ds_A", "git_commit": "commit_abc", "research_ready": True},
            "runs": [{"experiment_run_id": "exp_approved_1", "status": "APPROVED", "dataset_id": "ds_A"}],
            "comparison_table": [
                {
                    "algorithm": "DBSCAN",
                    "experiment_run_id": "exp_foreign_unapproved",
                    "dataset_id": "ds_A",
                    "total_aftershocks": 20,
                    "runtime_seconds": 1.2,
                    "model_config_version": "v1",
                }
            ],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(foreign_row_payload, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))
        self.assertIn("exp_foreign_unapproved", str(ctx.exception))

        # Case 2: Row belongs to dataset_B while report metadata is dataset_A (cross-dataset)
        cross_ds_row_payload = {
            "metadata": {"dataset_id": "ds_A", "git_commit": "commit_abc", "research_ready": True},
            "runs": [{"experiment_run_id": "exp_1", "status": "APPROVED", "dataset_id": "ds_A"}],
            "comparison_table": [
                {
                    "algorithm": "DBSCAN",
                    "experiment_run_id": "exp_1",
                    "dataset_id": "ds_B",
                    "total_aftershocks": 20,
                    "runtime_seconds": 1.2,
                    "model_config_version": "v1",
                }
            ],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(cross_ds_row_payload, mode="official")
        self.assertIn("CROSS_DATASET_VIOLATION", str(ctx.exception))

        # Case 3: Row has unapproved status directly (when runs list is omitted)
        unapproved_row_payload = {
            "metadata": {"dataset_id": "ds_A", "git_commit": "commit_abc", "research_ready": True},
            "comparison_table": [
                {
                    "algorithm": "DBSCAN",
                    "experiment_run_id": "exp_rej",
                    "dataset_id": "ds_A",
                    "experiment_status": "REJECTED",
                    "total_aftershocks": 20,
                    "runtime_seconds": 1.2,
                    "model_config_version": "v1",
                }
            ],
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.build_markdown_report(unapproved_row_payload, mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))
        self.assertIn("REJECTED", str(ctx.exception))

    def test_normalize_row_missing_required_fields_fails_closed(self):
        """Verify that normalize_comparison_row strictly fails-closed on missing required fields."""
        # Missing algorithm
        with self.assertRaises(ValueError) as ctx:
            report_gen.normalize_comparison_row({"total_aftershocks": 10, "runtime_seconds": 1.0})
        self.assertIn("algorithm", str(ctx.exception).lower())

        # Missing aftershocks
        with self.assertRaises(ValueError) as ctx:
            report_gen.normalize_comparison_row({"algorithm_name": "DBSCAN", "avg_runtime_seconds": 1.0})
        self.assertIn("aftershocks", str(ctx.exception).lower())

        # Missing runtime
        with self.assertRaises(ValueError) as ctx:
            report_gen.normalize_comparison_row({"algorithm_name": "DBSCAN", "raw_aftershock_memberships": 10})
        self.assertIn("runtime", str(ctx.exception).lower())

    def test_adapter_preserves_legitimate_null_when_all_windows_fail(self):
        """Verify that adapter accepts legitimate NULL aftershocks and preserves failure counts when all windows fail."""
        all_failed_row = {
            "algorithm_name": "WINDOW",
            "experiment_run_id": "run_all_failed",
            "dataset_id": "ds_fail_test",
            "model_config_version": "win-v1",
            "code_version": "git_sha",
            "random_seed": 42,
            "experiment_status": "APPROVED",
            "total_windows": 2,
            "success_windows": 0,
            "failure_windows": 2,
            "skipped_windows": 0,
            "failure_rate": 1.0,
            "raw_aftershock_memberships": None,  # Legitimate NULL from SQL view when all windows fail
            "avg_noise_ratio": None,
            "avg_dbcv_score": None,
            "avg_omori_p": None,
            "omori_fitted_windows": 0,
            "avg_runtime_seconds": 0.5,
        }
        # Normalization must succeed and preserve NULL
        norm = report_gen.normalize_comparison_row(all_failed_row)
        self.assertEqual(norm["algorithm"], "WINDOW")
        self.assertIsNone(norm["total_aftershocks"])
        self.assertEqual(norm["failure_rate"], 1.0)
        self.assertEqual(norm["failure_windows"], 2)

        # Markdown report rendering must succeed and show N/A for aftershocks while preserving counts
        payload = {
            "metadata": {
                "dataset_id": "ds_fail_test",
                "git_commit": "git_sha",
                "research_ready": True,
            },
            "comparison_table": [norm],
        }
        md = report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("| **WINDOW** | `run_all_failed` | `win-v1` | `42` | 2 | 0 | 2 | 100.0% | N/A |", md)

    def test_sql_view_to_report_integration(self):
        """Integration test: query v_algorithm_comparison directly from SQLite fixture and pipe into report generator."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Create schema matching SparkMlIcebergWriter
        cursor.executescript("""
        CREATE TABLE dataset_manifest (
            dataset_id TEXT PRIMARY KEY,
            dataset_split TEXT,
            gold_table_name TEXT,
            gold_snapshot_id INTEGER,
            period_start_utc TEXT,
            period_end_utc_exclusive TEXT,
            code_version TEXT
        );

        CREATE TABLE experiment_run (
            experiment_run_id TEXT PRIMARY KEY,
            dataset_id TEXT,
            algorithm_name TEXT,
            model_config_version TEXT,
            code_version TEXT,
            random_seed INTEGER,
            experiment_status TEXT,
            result_artifact_uri TEXT,
            result_ready_at_utc TEXT,
            created_at_utc TEXT
        );

        CREATE TABLE sequence_summary (
            experiment_run_id TEXT,
            dataset_id TEXT,
            mainshock_event_id TEXT,
            algorithm_name TEXT,
            model_config_version TEXT,
            window_result_status TEXT,
            candidate_count INTEGER,
            sequence_member_count INTEGER,
            aftershock_count INTEGER,
            foreshock_candidate_count INTEGER,
            noise_ratio REAL,
            dbcv_score REAL,
            omori_p REAL,
            failure_reason_code TEXT,
            runtime_seconds REAL,
            peak_memory_mb REAL
        );
        """)

        # Adapt and execute view DDL
        sql_file = self.sql_dir / "v_algorithm_comparison.sql"
        sqlite_ddl = adapt_trino_view_sql_for_sqlite(sql_file.read_text(encoding="utf-8"), "v_algorithm_comparison")
        cursor.execute(sqlite_ddl)

        cursor.executescript("""
        INSERT INTO dataset_manifest (dataset_id, dataset_split, gold_table_name, gold_snapshot_id, period_start_utc, period_end_utc_exclusive, code_version)
        VALUES ('ds_official_prod', 'REPRODUCTION', 'iceberg.gold.earthquake_event_current', 99999, '2018-01-01', '2018-10-01', 'git_sha_prod');

        INSERT INTO experiment_run (experiment_run_id, dataset_id, algorithm_name, model_config_version, code_version, random_seed, experiment_status, result_artifact_uri, result_ready_at_utc, created_at_utc)
        VALUES
        ('run_dbscan_01', 'ds_official_prod', 'DBSCAN', 'dbscan-v1', 'git_sha_prod', 42, 'APPROVED', 's3://art/dbs', '2026-11-01', '2026-11-01'),
        ('run_hdbscan_01', 'ds_official_prod', 'HDBSCAN_GLOBAL', 'hdbscan-v1', 'git_sha_prod', 42, 'APPROVED', 's3://art/hdb', '2026-11-01', '2026-11-01'),
        ('run_window_01', 'ds_official_prod', 'WINDOW', 'win-v1', 'git_sha_prod', 42, 'APPROVED', 's3://art/win', '2026-11-01', '2026-11-01');

        INSERT INTO sequence_summary (experiment_run_id, dataset_id, mainshock_event_id, algorithm_name, model_config_version, window_result_status, candidate_count, sequence_member_count, aftershock_count, foreshock_candidate_count, noise_ratio, dbcv_score, omori_p, failure_reason_code, runtime_seconds, peak_memory_mb)
        VALUES
        ('run_dbscan_01', 'ds_official_prod', 'm1', 'DBSCAN', 'dbscan-v1', 'SUCCESS', 15, 12, 12, 0, 0.05, 0.42, 0.95, NULL, 1.5, 120.0),
        ('run_dbscan_01', 'ds_official_prod', 'm2', 'DBSCAN', 'dbscan-v1', 'FAILED', 10, 0, 0, 0, NULL, NULL, NULL, 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 0.5, 100.0),
        ('run_hdbscan_01', 'ds_official_prod', 'm1', 'HDBSCAN_GLOBAL', 'hdbscan-v1', 'SUCCESS', 15, 10, 10, 0, 0.08, 0.38, 0.91, NULL, 2.0, 150.0),
        ('run_hdbscan_01', 'ds_official_prod', 'm2', 'HDBSCAN_GLOBAL', 'hdbscan-v1', 'SUCCESS', 10, 8, 8, 0, 0.10, 0.35, 0.88, NULL, 1.8, 140.0),
        ('run_window_01', 'ds_official_prod', 'm1', 'WINDOW', 'win-v1', 'FAILED', 10, 0, NULL, 0, NULL, NULL, NULL, 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 0.4, 80.0),
        ('run_window_01', 'ds_official_prod', 'm2', 'WINDOW', 'win-v1', 'FAILED', 5, 0, NULL, 0, NULL, NULL, NULL, 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 0.3, 75.0);
        """)

        # Execute query against real view definition
        cursor.execute("SELECT * FROM v_algorithm_comparison ORDER BY algorithm_name")
        view_rows = [dict(r) for r in cursor.fetchall()]
        conn.close()

        self.assertEqual(len(view_rows), 3)
        # Directly pass view output rows into report generator
        payload = {
            "metadata": {
                "dataset_id": "ds_official_prod",
                "git_commit": "git_sha_prod",
                "research_ready": True,
                "data_scope": "OFFICIAL",
            },
            "comparison_table": view_rows,
        }

        # Render report in official mode
        md = report_gen.build_markdown_report(payload, mode="official")

        # 1. Verify algorithm names are not UNKNOWN
        self.assertNotIn("**UNKNOWN**", md)
        self.assertIn("**DBSCAN**", md)
        self.assertIn("**HDBSCAN_GLOBAL**", md)
        self.assertIn("**WINDOW**", md)

        # 2. Verify Run IDs, configs, seeds are clearly visible
        self.assertIn("`run_dbscan_01`", md)
        self.assertIn("`run_hdbscan_01`", md)
        self.assertIn("`run_window_01`", md)
        self.assertIn("`dbscan-v1`", md)
        self.assertIn("`hdbscan-v1`", md)
        self.assertIn("`win-v1`", md)
        self.assertIn("`42`", md)

        # 3. Verify exact aftershock counts from raw_aftershock_memberships (mixed and all-failed)
        # DBSCAN: 12 memberships (mixed: 1 success, 1 fail)
        # HDBSCAN_GLOBAL: 10 + 8 = 18 memberships (all success)
        # WINDOW: N/A memberships (all failed: 0 success, 2 fail)
        self.assertIn("| **DBSCAN** | `run_dbscan_01` | `dbscan-v1` | `42` | 2 | 1 | 1 | 50.0% | 12 | 5.0% | 0.4200 | 0.95 | 1/2 |", md)
        self.assertIn("| **HDBSCAN_GLOBAL** | `run_hdbscan_01` | `hdbscan-v1` | `42` | 2 | 2 | 0 | 0.0% | 18 | 9.0% | 0.3650 | 0.90 | 2/2 |", md)
        self.assertIn("| **WINDOW** | `run_window_01` | `win-v1` | `42` | 2 | 0 | 2 | 100.0% | N/A |", md)

        # 4. Verify truthful parity: without explicit verification_evidence, must be marked PENDING
        self.assertIn("CHƯA ĐỐI SOÁT (PENDING READBACK / NOT VERIFIED)", md)
        self.assertNotIn("ĐÃ ĐỐI SOÁT (VERIFIED)", md)

    def test_official_report_marks_parity_verified_with_evidence(self):
        """Verify that official report outputs VERIFIED only when valid verification evidence is supplied."""
        payload = {
            "metadata": {
                "dataset_id": "ds_official_prod",
                "git_commit": "git_sha_prod",
                "research_ready": True,
            },
            "runs": [{
                "experiment_run_id": "run_1",
                "dataset_id": "ds_official_prod",
                "status": "APPROVED",
                "membership_snapshot_id": 123456789,
                "summary_snapshot_id": 987654321,
            }],
            "comparison_table": [{
                "algorithm": "DBSCAN",
                "experiment_run_id": "run_1",
                "dataset_id": "ds_official_prod",
                "model_config_version": "v1",
                "random_seed": 42,
                "status": "APPROVED",
                "total_windows": 1,
                "success_windows": 1,
                "failure_windows": 0,
                "raw_aftershock_memberships": 10,
                "avg_runtime_seconds": 1.0,
            }],
            "verification_evidence": {
                "status": "PASSED",
                "dataset_id": "ds_official_prod",
                "membership_snapshot_id": 123456789,
                "summary_snapshot_id": 987654321,
                "verified_runs": ["run_1"],
            }
        }
        md = report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("ĐÃ ĐỐI SOÁT (VERIFIED)", md)
        self.assertIn("123456789", md)
        self.assertIn("987654321", md)
        self.assertNotIn("CHƯA ĐỐI SOÁT", md)

    def test_sql_parity_negative_wrong_snapshots(self):
        """Negative test: verification evidence with mismatching snapshot IDs keeps parity PENDING."""
        payload = {
            "metadata": {
                "dataset_id": "ds_official_prod",
                "git_commit": "git_sha_prod",
                "research_ready": True,
            },
            "runs": [{
                "experiment_run_id": "run_1",
                "dataset_id": "ds_official_prod",
                "status": "APPROVED",
                "membership_snapshot_id": 111111,
                "summary_snapshot_id": 222222,
            }],
            "comparison_table": [{
                "algorithm": "DBSCAN",
                "experiment_run_id": "run_1",
                "dataset_id": "ds_official_prod",
                "model_config_version": "v1",
                "status": "APPROVED",
                "raw_aftershock_memberships": 10,
                "avg_runtime_seconds": 1.0,
            }],
            "verification_evidence": {
                "status": "PASSED",
                "dataset_id": "ds_official_prod",
                "membership_snapshot_id": 999999,  # Mismatched snapshot
                "summary_snapshot_id": 888888,    # Mismatched snapshot
            }
        }
        md = report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("CHƯA ĐỐI SOÁT (PENDING READBACK / NOT VERIFIED)", md)
        self.assertNotIn("ĐÃ ĐỐI SOÁT (VERIFIED)", md)

    def test_sql_parity_negative_missing_dataset_binding(self):
        """Negative test: verification evidence missing dataset_id or with mismatched dataset keeps parity PENDING."""
        payload = {
            "metadata": {
                "dataset_id": "ds_official_prod",
                "git_commit": "git_sha_prod",
                "research_ready": True,
            },
            "comparison_table": [{
                "algorithm": "DBSCAN",
                "experiment_run_id": "run_1",
                "dataset_id": "ds_official_prod",
                "model_config_version": "v1",
                "status": "APPROVED",
                "raw_aftershock_memberships": 10,
                "avg_runtime_seconds": 1.0,
            }],
            "verification_evidence": {
                "status": "PASSED",
                "dataset_id": "ds_different",  # Mismatched dataset
                "membership_snapshot_id": 123456789,
                "summary_snapshot_id": 987654321,
            }
        }
        md = report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("CHƯA ĐỐI SOÁT (PENDING READBACK / NOT VERIFIED)", md)
        self.assertNotIn("ĐÃ ĐỐI SOÁT (VERIFIED)", md)

    def test_sql_parity_negative_partial_evidence(self):
        """Negative test: partial verification evidence not covering all rendered runs keeps parity PENDING."""
        payload = {
            "metadata": {
                "dataset_id": "ds_official_prod",
                "git_commit": "git_sha_prod",
                "research_ready": True,
            },
            "comparison_table": [
                {
                    "algorithm": "DBSCAN",
                    "experiment_run_id": "run_1",
                    "dataset_id": "ds_official_prod",
                    "model_config_version": "v1",
                    "status": "APPROVED",
                    "raw_aftershock_memberships": 10,
                    "avg_runtime_seconds": 1.0,
                },
                {
                    "algorithm": "HDBSCAN_GLOBAL",
                    "experiment_run_id": "run_2",
                    "dataset_id": "ds_official_prod",
                    "model_config_version": "v1",
                    "status": "APPROVED",
                    "raw_aftershock_memberships": 15,
                    "avg_runtime_seconds": 1.2,
                },
            ],
            "verification_evidence": {
                "status": "PASSED",
                "dataset_id": "ds_official_prod",
                "membership_snapshot_id": 123456789,
                "summary_snapshot_id": 987654321,
                "verified_runs": ["run_1"],  # Missing run_2!
            }
        }
        md = report_gen.build_markdown_report(payload, mode="official")
        self.assertIn("CHƯA ĐỐI SOÁT (PENDING READBACK / NOT VERIFIED)", md)
        self.assertNotIn("ĐÃ ĐỐI SOÁT (VERIFIED)", md)

    def test_development_mode_truthful_parity_statement(self):
        """Verify that development pilot report explicitly marks SQL parity as CHƯA NGHIỆM THU."""
        with open(self.pilot_eval_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        md = report_gen.build_markdown_report(data, mode="development")
        self.assertIn("DEVELOPMENT_ONLY (research_ready=false)", md)
        self.assertIn("CHƯA NGHIỆM THU (PENDING OFFICIAL IMPORT)", md)
        self.assertIn("Bảo toàn mẫu số (Denominator Integrity)", md)

    def test_data_dictionary_completeness(self):
        """Verify that ML_REPORT_DATA_DICTIONARY.md documents all views and reason codes."""
        self.assertTrue(self.data_dict.exists(), "Data dictionary file must exist")
        content = self.data_dict.read_text(encoding="utf-8")

        required_views = [
            "iceberg.ml.v_experiment_runs",
            "iceberg.ml.v_algorithm_comparison",
            "iceberg.ml.v_sequence_memberships_resolved",
            "iceberg.ml.v_failure_windows",
        ]
        for v in required_views:
            self.assertIn(v, content, f"Data dictionary must document {v}")

        required_codes = [
            "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE",
            "METRIC_NOT_APPLICABLE",
            "METRIC_INSUFFICIENT_MEMBERS",
            "METRIC_FIT_FAILED",
        ]
        for c in required_codes:
            self.assertIn(c, content, f"Data dictionary must document reason code {c}")


if __name__ == "__main__":
    unittest.main()
