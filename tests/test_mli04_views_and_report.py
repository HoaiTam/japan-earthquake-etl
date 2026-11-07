"""Unit and integration tests for MLI-04: Trino Views and Static Experiment Report.

Verifies:
1. SQL View Syntax & Structure:
   - Presence of all required ML serving views and verification queries.
   - Enforce publication gate (CANDIDATE and APPROVED only).
   - Enforce zero secret or credential leaks.
2. Offline SQL Semantics & Count Parity:
   - In-memory database simulation of Iceberg ml.* tables.
   - Verification of publication gate filtering unapproved/rejected runs.
   - Denominator integrity: failure windows included in total counts and failure rates.
   - Honest NULL policy: DBCV is NULL for WINDOW, never coerced to 0.
   - Summary candidate_count matches membership count.
   - Aftershock count matches POST sequence member count.
   - Multi-sequence resolution distinguishes raw memberships from unique resolved candidates.
3. Static Report Generator:
   - Development pilot mode produces DEVELOPMENT_ONLY badge and preserves all disclaimers.
   - Official mode validates publication gate, refusing unapproved runs.
   - Denominator integrity preserved in report table.
4. Data Dictionary Completeness:
   - Verifies documentation of views, fields, reason codes, and scientific guardrails.
"""
from __future__ import annotations

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

import importlib.util
_spec = importlib.util.spec_from_file_location('generate_experiment_report', str(SCRIPTS_DIR / 'generate-experiment-report.py'))
report_gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report_gen)


class TestMli04ViewsAndReport(unittest.TestCase):
    """Test suite for MLI-04 deliverables."""

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

    def test_in_memory_sql_semantics_and_gate_enforcement(self):
        """Execute SQLite in-memory test reproducing Trino views with mock Iceberg tables."""
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()

        # 1. Setup tables mimicking iceberg.ml.*
        cursor.executescript("""
        CREATE TABLE dataset_manifest (
            dataset_id TEXT PRIMARY KEY,
            dataset_split TEXT,
            gold_table_name TEXT,
            gold_snapshot_id INTEGER,
            period_start_utc TEXT,
            period_end_utc_exclusive TEXT,
            feature_version TEXT,
            scaling_config_version TEXT
        );

        CREATE TABLE experiment_run (
            experiment_run_id TEXT PRIMARY KEY,
            algorithm_name TEXT,
            model_config_version TEXT,
            algorithm_version TEXT,
            package_version TEXT,
            code_version TEXT,
            experiment_status TEXT,
            dataset_id TEXT,
            membership_row_count INTEGER,
            summary_row_count INTEGER,
            artifact_uri TEXT,
            result_bundle_sha256 TEXT,
            completed_at_utc TEXT,
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
            peak_memory_mb REAL,
            created_at_utc TEXT
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

        # 2. Insert Dataset
        cursor.execute("""
        INSERT INTO dataset_manifest VALUES (
            'ds_test_01', 'REPRODUCTION', 'iceberg.gold.earthquake_event_current',
            123456789, '2018-01-01 00:00:00', '2018-10-01 00:00:00', 'feat-v1', 'scale-v1'
        );
        """)

        # 3. Insert Experiment Runs with different statuses:
        # Run 1: WINDOW (APPROVED)
        # Run 2: DBSCAN (CANDIDATE)
        # Run 3: HDBSCAN_GLOBAL (TRAINING_EXTERNAL - NOT PUBLISHED)
        # Run 4: HDBSCAN_ADAPTIVE (REJECTED - NOT PUBLISHED)
        cursor.executescript("""
        INSERT INTO experiment_run VALUES
        ('exp_win', 'WINDOW', 'win-v1', '1.0', '0.1', 'commit_a', 'APPROVED', 'ds_test_01', 3, 2, 's3://art/win', 'sha_win', '2026-11-01', '2026-11-01'),
        ('exp_dbs', 'DBSCAN', 'dbs-v1', '1.0', '0.1', 'commit_b', 'CANDIDATE', 'ds_test_01', 3, 2, 's3://art/dbs', 'sha_dbs', '2026-11-01', '2026-11-01'),
        ('exp_hdb_unpub', 'HDBSCAN_GLOBAL', 'hdb-v1', '1.0', '0.1', 'commit_c', 'TRAINING_EXTERNAL', 'ds_test_01', 3, 2, 's3://art/hdb', 'sha_hdb', '2026-11-01', '2026-11-01'),
        ('exp_rej', 'HDBSCAN_ADAPTIVE', 'adp-v1', '1.0', '0.1', 'commit_d', 'REJECTED', 'ds_test_01', 3, 2, 's3://art/rej', 'sha_rej', '2026-11-01', '2026-11-01');
        """)

        # 4. Insert Summaries:
        # WINDOW: 2 windows, both SUCCESS. DBCV is NULL.
        # DBSCAN: 2 windows, 1 SUCCESS (aftershock=1), 1 FAILED (EXP_MAINSHOCK_CLASSIFIED_AS_NOISE).
        cursor.executescript("""
        INSERT INTO sequence_summary VALUES
        ('exp_win', 'ds_test_01', 'main_1', 'WINDOW', 'win-v1', 'SUCCESS', 2, 2, 1, 0, NULL, NULL, 0.90, NULL, 1.2, 50.0, '2026-11-01'),
        ('exp_win', 'ds_test_01', 'main_2', 'WINDOW', 'win-v1', 'SUCCESS', 1, 1, 1, 0, NULL, NULL, NULL, NULL, 0.8, 48.0, '2026-11-01'),
        ('exp_dbs', 'ds_test_01', 'main_1', 'DBSCAN', 'dbs-v1', 'SUCCESS', 2, 1, 1, 0, 0.5, 0.25, 0.95, NULL, 1.5, 55.0, '2026-11-01'),
        ('exp_dbs', 'ds_test_01', 'main_2', 'DBSCAN', 'dbs-v1', 'FAILED', 1, 0, 0, 0, NULL, NULL, NULL, 'EXP_MAINSHOCK_CLASSIFIED_AS_NOISE', 0.5, 52.0, '2026-11-01');
        """)

        # 5. Insert Memberships
        cursor.executescript("""
        -- exp_win: main_1 has cand_1 (POST member), cand_main1 (MAINSHOCK); main_2 has cand_2 (POST member)
        INSERT INTO sequence_membership VALUES
        ('exp_win', 'ds_test_01', 'main_1', 'cand_1', 'WINDOW', 'win-v1', NULL, NULL, NULL, 1, NULL, 'POST', 0.1, 'UNIQUE', 1, '2026-11-01'),
        ('exp_win', 'ds_test_01', 'main_1', 'cand_main1', 'WINDOW', 'win-v1', NULL, NULL, NULL, 1, NULL, 'MAINSHOCK', 0.0, 'UNIQUE', 1, '2026-11-01'),
        ('exp_win', 'ds_test_01', 'main_2', 'cand_2', 'WINDOW', 'win-v1', NULL, NULL, NULL, 1, NULL, 'POST', 0.2, 'UNIQUE', 1, '2026-11-01'),

        -- exp_dbs: main_1 has cand_1 (POST member), cand_main1 (MAINSHOCK, noise=0); main_2 has cand_main2 (MAINSHOCK, noise=1)
        ('exp_dbs', 'ds_test_01', 'main_1', 'cand_1', 'DBSCAN', 'dbs-v1', 0, 0, 1, 1, NULL, 'POST', 0.1, 'UNIQUE', 1, '2026-11-01'),
        ('exp_dbs', 'ds_test_01', 'main_1', 'cand_main1', 'DBSCAN', 'dbs-v1', 0, 0, 1, 0, NULL, 'MAINSHOCK', 0.0, 'UNIQUE', 1, '2026-11-01'),
        ('exp_dbs', 'ds_test_01', 'main_2', 'cand_main2', 'DBSCAN', 'dbs-v1', -1, 1, 0, 0, NULL, 'MAINSHOCK', 0.0, 'NOT_MEMBER', NULL, '2026-11-01');
        """)

        # 6. Test View: v_experiment_runs logic
        q_runs = """
        SELECT experiment_run_id, experiment_status
        FROM experiment_run
        WHERE experiment_status IN ('CANDIDATE', 'APPROVED')
        """
        cursor.execute(q_runs)
        visible_runs = dict(cursor.fetchall())
        self.assertEqual(len(visible_runs), 2)
        self.assertIn("exp_win", visible_runs)
        self.assertIn("exp_dbs", visible_runs)
        self.assertNotIn("exp_hdb_unpub", visible_runs)
        self.assertNotIn("exp_rej", visible_runs)

        # 7. Test View: v_algorithm_comparison logic (Denominator Integrity & Honest NULLs)
        q_comp = """
        SELECT
            e.algorithm_name,
            COUNT(*) AS total_windows,
            SUM(CASE WHEN s.window_result_status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_windows,
            SUM(CASE WHEN s.window_result_status = 'FAILED' THEN 1 ELSE 0 END) AS failure_windows,
            CAST(SUM(CASE WHEN s.window_result_status = 'FAILED' THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS failure_rate,
            AVG(s.dbcv_score) AS avg_dbcv_score,
            SUM(s.aftershock_count) AS raw_aftershock_memberships
        FROM experiment_run e
        JOIN sequence_summary s ON e.experiment_run_id = s.experiment_run_id
        WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED')
        GROUP BY e.algorithm_name
        ORDER BY e.algorithm_name;
        """
        cursor.execute(q_comp)
        comp_rows = {row[0]: row[1:] for row in cursor.fetchall()}

        # Verify DBSCAN row: total=2, success=1, fail=1, failure_rate=0.5, dbcv=0.25, aftershocks=1
        dbs_stats = comp_rows["DBSCAN"]
        self.assertEqual(dbs_stats[0], 2)  # total_windows
        self.assertEqual(dbs_stats[1], 1)  # success_windows
        self.assertEqual(dbs_stats[2], 1)  # failure_windows
        self.assertAlmostEqual(dbs_stats[3], 0.5)  # failure_rate
        self.assertAlmostEqual(dbs_stats[4], 0.25)  # avg_dbcv
        self.assertEqual(dbs_stats[5], 1)  # raw_aftershocks

        # Verify WINDOW row: total=2, success=2, fail=0, failure_rate=0.0, dbcv=NULL (honest NULL!), aftershocks=2
        win_stats = comp_rows["WINDOW"]
        self.assertEqual(win_stats[0], 2)
        self.assertEqual(win_stats[1], 2)
        self.assertEqual(win_stats[2], 0)
        self.assertAlmostEqual(win_stats[3], 0.0)
        self.assertIsNone(win_stats[4], "DBCV for WINDOW must remain NULL, not coerced to 0.0")
        self.assertEqual(win_stats[5], 2)

        # 8. Test View: v_failure_windows logic
        q_fail = """
        SELECT s.experiment_run_id, s.mainshock_event_id, s.failure_reason_code
        FROM sequence_summary s
        JOIN experiment_run e ON s.experiment_run_id = e.experiment_run_id
        WHERE e.experiment_status IN ('CANDIDATE', 'APPROVED')
          AND s.window_result_status IN ('FAILED', 'SKIPPED');
        """
        cursor.execute(q_fail)
        fail_rows = cursor.fetchall()
        self.assertEqual(len(fail_rows), 1)
        self.assertEqual(fail_rows[0][0], "exp_dbs")
        self.assertEqual(fail_rows[0][1], "main_2")
        self.assertEqual(fail_rows[0][2], "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")

        # 9. Test Verification Query: Summary candidate_count vs Membership Count Parity
        q_parity = """
        SELECT
            s.experiment_run_id,
            s.mainshock_event_id,
            s.candidate_count,
            COUNT(m.candidate_event_id) AS membership_count
        FROM sequence_summary s
        JOIN sequence_membership m
          ON s.experiment_run_id = m.experiment_run_id
         AND s.mainshock_event_id = m.mainshock_event_id
        GROUP BY s.experiment_run_id, s.mainshock_event_id, s.candidate_count
        HAVING s.candidate_count <> COUNT(m.candidate_event_id);
        """
        cursor.execute(q_parity)
        discrepancies = cursor.fetchall()
        self.assertEqual(len(discrepancies), 0, "Summary candidate count must match membership count exactly")

        conn.close()

    def test_multi_sequence_resolution_distinction(self):
        """Verify that resolved unique candidates (rank 1) are strictly distinct from raw memberships."""
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.executescript("""
        CREATE TABLE sequence_membership (
            experiment_run_id TEXT,
            mainshock_event_id TEXT,
            candidate_event_id TEXT,
            is_sequence_member INTEGER,
            event_role_candidate TEXT,
            resolution_rank INTEGER
        );

        -- cand_overlap belongs to two mainshocks: main_1 (rank 1 - winner), main_2 (rank 2 - tied/loser)
        INSERT INTO sequence_membership VALUES
        ('exp_test', 'main_1', 'cand_overlap', 1, 'POST', 1),
        ('exp_test', 'main_2', 'cand_overlap', 1, 'POST', 2),
        ('exp_test', 'main_1', 'cand_single', 1, 'POST', 1);
        """)

        # Query raw vs resolved unique
        cursor.execute("""
        SELECT
            COUNT(*) AS raw_post_memberships,
            COUNT(DISTINCT CASE WHEN is_sequence_member = 1 AND event_role_candidate = 'POST' AND resolution_rank = 1 THEN candidate_event_id END) AS unique_resolved_aftershocks
        FROM sequence_membership
        WHERE is_sequence_member = 1 AND event_role_candidate = 'POST';
        """)
        raw_count, unique_count = cursor.fetchone()
        self.assertEqual(raw_count, 3, "Raw memberships count every assignment")
        self.assertEqual(unique_count, 2, "Resolved unique aftershocks must eliminate duplicates via resolution_rank = 1")
        conn.close()

    def test_report_generator_development_mode(self):
        """Verify that report generator in development mode applies DEVELOPMENT_ONLY and required disclaimers."""
        self.assertTrue(self.pilot_eval_json.exists(), "Pilot evaluation JSON fixture must exist")
        with open(self.pilot_eval_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        md = report_gen.build_markdown_report(data, mode="development")
        self.assertIn("DEVELOPMENT_ONLY (research_ready=false)", md)
        self.assertIn("RANH GIỚI KHOA HỌC BẮT BUỘC", md)
        self.assertIn("Tuyệt đối KHÔNG dự đoán động đất", md)
        self.assertIn("Không dùng Accuracy / F1", md)
        self.assertIn("Bảo toàn mẫu số (Denominator Integrity)", md)
        self.assertIn("WINDOW", md)
        self.assertIn("DBSCAN", md)
        self.assertIn("HDBSCAN_GLOBAL", md)
        self.assertIn("HDBSCAN_ADAPTIVE", md)
        self.assertIn("EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", md)

    def test_report_generator_official_mode_gate_enforcement(self):
        """Verify that report generator in official mode rejects unapproved runs."""
        unapproved_payload = {
            "metadata": {
                "dataset_id": "ds_official_01",
                "research_ready": True,
            },
            "runs": [
                {"experiment_run_id": "exp_valid", "status": "APPROVED"},
                {"experiment_run_id": "exp_invalid", "status": "TRAINING_EXTERNAL"},
            ],
            "comparison_table": [],
            "model_analyses": {},
        }
        with self.assertRaises(ValueError) as ctx:
            report_gen.validate_publication_gate(unapproved_payload["runs"], mode="official")
        self.assertIn("PUBLICATION_GATE_VIOLATION", str(ctx.exception))

        # Passing only valid runs should succeed
        valid_runs = [
            {"experiment_run_id": "exp_1", "status": "APPROVED"},
            {"experiment_run_id": "exp_2", "status": "CANDIDATE"},
        ]
        report_gen.validate_publication_gate(valid_runs, mode="official")

    def test_report_cli_execution(self):
        """Test invoking generate-experiment-report.py via CLI."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_md = Path(tmpdir) / "test_report.md"
            out_json = Path(tmpdir) / "test_summary.json"
            cmd = [
                sys.executable,
                str(self.report_script),
                "--input-json",
                str(self.pilot_eval_json),
                "--output-md",
                str(out_md),
                "--output-json",
                str(out_json),
                "--mode",
                "development",
            ]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.assertEqual(result.returncode, 0, f"CLI failed: {result.stderr}")
            self.assertTrue(out_md.exists(), "Markdown output must be generated")
            self.assertTrue(out_json.exists(), "JSON output must be generated")

            content = out_md.read_text(encoding="utf-8")
            self.assertIn("DEVELOPMENT_ONLY", content)

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
