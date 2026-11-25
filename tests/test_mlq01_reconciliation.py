"""Offline reconciliation SQL fixtures and checked-in evidence pin checks."""
import hashlib
import json
import re
import sqlite3
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
QUERY = ROOT / "sql/ml/mlq01_reconciliation.sql"
FIXTURE = """
CREATE TABLE gold_source (canonical_event_id TEXT);
INSERT INTO gold_source VALUES ('main'), ('target'), ('extra');
CREATE TABLE dataset_source (
    dataset_id TEXT, dataset_status TEXT, gold_snapshot_id BIGINT, candidate_snapshot_id BIGINT,
    gold_publication_id TEXT, physical_gold_table_name TEXT, mainshock_snapshot_id BIGINT
);
INSERT INTO dataset_source VALUES ('dataset', 'EXPORTED', 11, 13, 'publication', 'gold_source', 17);
CREATE TABLE run_source (
    dataset_id TEXT, experiment_run_id TEXT, experiment_status TEXT, result_bundle_sha256 TEXT,
    dataset_registry_snapshot_id TEXT, candidate_source_snapshot_id TEXT, mainshock_source_snapshot_id TEXT,
    dataset_manifest_sha256 TEXT, membership_snapshot_id BIGINT, summary_snapshot_id BIGINT
);
INSERT INTO run_source VALUES ('dataset', 'run', 'CANDIDATE', 'fixture-sha', '12', '13', '17', 'manifest-sha', 15, 16);
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
        "MAINSHOCK_SNAPSHOT": "17", "GOLD_PUBLICATION_ID": "publication",
        "DATASET_MANIFEST_SHA256": "manifest-sha",
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
            connection.executescript(change)
        return dict(connection.execute(fixture_query()).fetchone())

    def test_query_has_no_semicolon_for_trino_http_api(self):
        self.assertNotIn(";", QUERY.read_text(encoding="utf-8"))
        self.assertNotIn(";", fixture_query())

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

    def test_rejects_each_wrong_publication_and_lineage_pin(self):
        for change in (
            "UPDATE dataset_source SET gold_publication_id = 'other'",
            "UPDATE dataset_source SET physical_gold_table_name = 'other'",
            "UPDATE dataset_source SET mainshock_snapshot_id = 99",
            "UPDATE run_source SET mainshock_source_snapshot_id = '99'",
            "UPDATE run_source SET dataset_manifest_sha256 = 'other'",
        ):
            with self.subTest(change=change):
                row = self.reconcile(change)
                self.assertEqual((1, 1, 1, 2, 2), tuple(row[k] for k in (
                    "gold_rows", "dataset_rows", "registry_rows", "candidate_rows", "membership_rows")))
                self.assertEqual("FAIL", row["lineage_check"])

    def test_rejects_matching_wrong_mainshock_roles(self):
        row = self.reconcile(
            "UPDATE candidate_source SET relative_time_role = 'PRE' WHERE candidate_event_id = 'main';"
            "UPDATE membership_source SET event_role_candidate = 'PRE' WHERE candidate_event_id = 'main';"
        )
        self.assertEqual(2, row["role_matched_rows"])
        self.assertEqual((0, 0, 1), tuple(row[k] for k in (
            "candidate_mainshock_rows", "membership_mainshock_rows", "target_resolved")))
        self.assertEqual("FAIL", row["lineage_check"])

    def test_rejects_mainshock_not_in_its_own_sequence(self):
        row = self.reconcile("UPDATE membership_source SET is_sequence_member = FALSE WHERE candidate_event_id = 'main'")
        self.assertEqual((1, 0), (row["candidate_mainshock_rows"], row["membership_mainshock_rows"]))
        self.assertEqual("FAIL", row["lineage_check"])

    def test_rejects_extra_mainshock_row_in_window(self):
        row = self.reconcile(
            "INSERT INTO candidate_source VALUES ('dataset', 'main', 'extra', 'MAINSHOCK');"
            "INSERT INTO membership_source VALUES ('dataset', 'run', 'main', 'extra', 'MAINSHOCK', TRUE, 1);"
            "UPDATE summary_source SET candidate_count = 3;"
        )
        self.assertEqual(3, row["role_matched_rows"])
        self.assertEqual((1, 2, 1, 2), tuple(row[k] for k in (
            "candidate_mainshock_rows", "candidate_total_mainshock_rows",
            "membership_mainshock_rows", "membership_total_mainshock_rows")))
        self.assertEqual("FAIL", row["lineage_check"])

        for extra_membership in (
            "INSERT INTO membership_source VALUES ('dataset', 'run', 'main', 'extra', 'MAINSHOCK', FALSE, 2);",
            "INSERT INTO membership_source VALUES ('dataset', 'run', 'main', 'extra', 'PRE', TRUE, 2);",
        ):
            with self.subTest(extra_membership=extra_membership):
                bad = self.reconcile(
                    "INSERT INTO candidate_source VALUES ('dataset', 'main', 'extra', 'MAINSHOCK');"
                    + extra_membership
                    + "UPDATE summary_source SET candidate_count = 3;"
                )
                self.assertEqual("FAIL", bad["lineage_check"])

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

    def test_reconcile_row_supplies_trace_receipt_evidence_fields(self):
        row = self.reconcile()
        trace = {
            "query_id": "20261124_120000_00001_v3i3r",
            "dataset_id": row["dataset_id"],
            "experiment_run_id": row["run_id"],
            "gold_snapshot_id": "11",
            "experiment_snapshot_id": 14,
            "membership_snapshot_id": 15,
            "summary_snapshot_id": 16,
            "mainshock_event_id": row["mainshock_id"],
            "candidate_event_id": row["event_id"],
            "lineage_check": row["lineage_check"],
        }
        self.assertEqual("PASS", trace["lineage_check"])
        self.assertNotEqual(trace["mainshock_event_id"], trace["candidate_event_id"])
        self.assertEqual((1, 1, 1, 1), (
            row["gold_rows"], row["exported_rows"], row["serving_rows"], row["target_resolved"]
        ))

    def test_official_receipt_keeps_evidence_pins_and_trace_coverage(self):
        def lf_sha(path):
            return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()

        base = ROOT / "docs/evidence"
        e2e = json.loads((base / "MLQ-01_E2E_20261125.json").read_bytes())
        extension = e2e["extension"]
        marker = ROOT / extension["success_marker_path"]
        self.assertEqual("PASSED", e2e["status"])
        self.assertIs(e2e["scientific_approval"], False)
        self.assertEqual(extension["success_marker_sha256"], hashlib.sha256(marker.read_bytes()).hexdigest())
        self.assertEqual(e2e["mli04_sha256_lf"], lf_sha(base / "MLI-04_OFFICIAL_READBACK_20261124.json"))
        self.assertEqual(extension["exp05_sha256_lf"], lf_sha(base / "EXP-05_EXTENSION_EVALUATION.json"))
        for pin in (extension["import_receipt"], e2e["rerun_verification"]):
            self.assertEqual(pin["sha256_lf"], lf_sha(ROOT / pin["path"]))
        self.assertEqual({r["algorithm_name"] for r in e2e["runs"].values()},
                         {"WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"})
        traces = [r["trace"] for r in e2e["runs"].values()] + [extension["trace"]]
        self.assertEqual(5, len({t["query_id"] for t in traces}))
        self.assertTrue(all(t["lineage_check"] == "PASS" for t in traces))
