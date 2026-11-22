"""Offline checks for the MLQ-01 reconciliation SQL over a single synthetic window."""
import re
import sqlite3
import unittest
from pathlib import Path


QUERY = Path(__file__).resolve().parents[1] / "sql/ml/mlq01_reconciliation.sql"
FIXTURE = """
CREATE TABLE gold_source (canonical_event_id TEXT);
INSERT INTO gold_source VALUES ('main'), ('target'), ('extra');
CREATE TABLE dataset_source (dataset_id TEXT, dataset_status TEXT, gold_snapshot_id INT, candidate_snapshot_id INT);
INSERT INTO dataset_source VALUES ('dataset', 'EXPORTED', 11, 13);
CREATE TABLE run_source (
    dataset_id TEXT, experiment_run_id TEXT, experiment_status TEXT, result_bundle_sha256 TEXT,
    dataset_registry_snapshot_id TEXT, candidate_source_snapshot_id TEXT,
    membership_snapshot_id INT, summary_snapshot_id INT
);
INSERT INTO run_source VALUES ('dataset', 'run', 'CANDIDATE', 'fixture-sha', '12', '13', 15, 16);
CREATE TABLE candidate_source (dataset_id TEXT, mainshock_event_id TEXT, candidate_event_id TEXT, relative_time_role TEXT);
INSERT INTO candidate_source VALUES ('dataset', 'main', 'main', 'MAINSHOCK'), ('dataset', 'main', 'target', 'POST');
CREATE TABLE membership_source (
    dataset_id TEXT, experiment_run_id TEXT, mainshock_event_id TEXT, candidate_event_id TEXT,
    event_role_candidate TEXT, is_sequence_member BOOLEAN, resolution_rank INT
);
INSERT INTO membership_source VALUES
    ('dataset', 'run', 'main', 'main', 'MAINSHOCK', TRUE, 1),
    ('dataset', 'run', 'main', 'target', 'POST', TRUE, 1),
    ('dataset', 'another-run', 'main', 'target', 'POST', TRUE, 1);
CREATE TABLE summary_source (
    dataset_id TEXT, experiment_run_id TEXT, mainshock_event_id TEXT,
    window_result_status TEXT, candidate_count INT, aftershock_count INT
);
INSERT INTO summary_source VALUES ('dataset', 'run', 'main', 'SUCCESS', 2, 1);
"""


def fixture_query():
    fields = {
        "DATASET_ID": "dataset", "RUN_ID": "run", "MAINSHOCK_ID": "main",
        "EVENT_ID": "target", "BUNDLE_SHA256": "fixture-sha",
        "GOLD_SNAPSHOT": "11", "DATASET_SNAPSHOT": "12", "CANDIDATE_SNAPSHOT": "13",
        "RUN_SNAPSHOT": "14", "MEMBERSHIP_SNAPSHOT": "15", "SUMMARY_SNAPSHOT": "16",
    }
    for name in ("gold", "dataset", "run", "candidate", "membership", "summary"):
        fields[name.upper() + "_TABLE"] = name + "_source"
    query = QUERY.read_text(encoding="utf-8")
    query = re.sub(r" FOR VERSION AS OF __[A-Z]+_SNAPSHOT__", "", query)
    return re.sub(r"__([A-Z_0-9]+)__", lambda match: fields[match[1]], query)


class Mlq01ReconciliationTest(unittest.TestCase):
    def reconcile(self, change=""):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.row_factory = sqlite3.Row
        connection.executescript(FIXTURE)
        if change:
            connection.execute(change)
        return dict(connection.execute(fixture_query()).fetchone())

    def test_one_event_traces_through_all_boundaries(self):
        row = self.reconcile()
        self.assertEqual("PASS", row["lineage_check"])
        self.assertEqual((1, 2, 2, 1), tuple(row[k] for k in
                         ("gold_rows", "candidate_rows", "membership_rows", "target_resolved")))

    def test_rejects_bad_identity_or_unpublished_result(self):
        for change in (
            "DELETE FROM gold_source WHERE canonical_event_id = 'target'",
            "UPDATE dataset_source SET dataset_status = 'BUILDING'",
            "UPDATE dataset_source SET gold_snapshot_id = 99",
            "UPDATE run_source SET experiment_status = 'REJECTED'",
            "UPDATE run_source SET result_bundle_sha256 = 'other'",
            "UPDATE run_source SET membership_snapshot_id = 99",
            "UPDATE run_source SET candidate_source_snapshot_id = '99'",
        ):
            with self.subTest(change=change):
                self.assertEqual("FAIL", self.reconcile(change)["lineage_check"])

    def test_rejects_an_equal_count_with_wrong_candidate_or_result(self):
        for change in (
            "UPDATE membership_source SET candidate_event_id = 'extra' WHERE experiment_run_id = 'run' AND candidate_event_id = 'main'",
            "UPDATE membership_source SET candidate_event_id = 'target' WHERE experiment_run_id = 'run' AND candidate_event_id = 'main'",
            "UPDATE candidate_source SET relative_time_role = 'PRE' WHERE candidate_event_id = 'main'",
            "UPDATE membership_source SET resolution_rank = 2 WHERE candidate_event_id = 'target'",
            "UPDATE summary_source SET aftershock_count = 0",
        ):
            with self.subTest(change=change):
                self.assertEqual("FAIL", self.reconcile(change)["lineage_check"])
