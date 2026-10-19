"""Synthetic QA-01 fixtures are never deployment acceptance evidence."""
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import qa_01_daily_e2e as qa
import real_etl_runtime as runtime


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class DailyE2eTest(unittest.TestCase):
    def setUp(self):
        self.run = {"dag_id": qa.DAG, "run_id": "scheduled__2023-09-30T00:15:00+00:00", "run_type": "scheduled",
                    "state": "success", "data_interval_start": "2023-09-30T00:15:00+00:00",
                    "data_interval_end": "2023-10-01T00:15:00+00:00", "tasks": {t: "success" for t in qa.TASKS}}
        owner = {"run_id": self.run["run_id"], "dag_id": qa.DAG}
        window = {"window_start_utc": "2023-09-28T00:00:00Z", "window_end_utc": "2023-10-01T00:00:00Z"}
        usgs = {**owner, "manifest_uri": "s3://bucket/bronze/usgs.json", "sha256": "a" * 64, "manifest_sha256": "b" * 64}
        jma = {**owner, "manifest_uri": "s3://bucket/bronze/jma.json", "sha256": "c" * 64, "manifest_sha256": "d" * 64,
               "year": 2023, "segment": "full-year", "catalog_release": "release1"}
        self.source = {"run_id": owner["run_id"], "status": "SourcesReady", "verified": True, "published": False,
                       "data_interval_end": self.run["data_interval_end"], "usgs_context": {**owner, **window},
                       "usgs": usgs, "jma": {"archives": [jma]}}
        pins = qa.validate_sources(self.run, self.source)
        op = runtime.operation(qa.DAG, owner["run_id"])
        request = {"resolver_version": "orc-06-v1", "run_id": op, "dag_id": qa.DAG, "target_months": ["2023-09"],
                   "namespace": "iceberg.gold_qa_test", "config_version": "test", "code_version": "test",
                   "baseline_snapshots": {t: 0 for t in runtime.TABLES}, "baseline": None, "bronze_inputs": pins,
                   "processed_at_utc": "2026-10-15T00:00:00Z", "source_cutoff_utc": "2026-10-15T00:00:00Z"}
        self.context = {**owner, **window, "contract_version": runtime.VERSION, "mode": "real", "trigger": "daily",
                        "operation_id": op, "source_summary_sha256": runtime.digest(self.source), "resolver_request": request}
        self.context["context_sha256"] = runtime.digest(self.context)
        self.counts = {"balanced": True, "selected_input_records": 13, "input_unique_revisions": 12,
                       "repeated_input_observation_ids": 1, "output_revisions": 11, "revisions_outside_affected_months": 1,
                       "current": 10, "history": 1, "canonical": 8, "memberships": 10, "links": 2,
                       "sources": [{"source_system": "USGS", "full_bronze_records": 5, "selected_records": 5,
                                    "outside_temporal_selection": 0, "parser_rejects": 0, "unique_revisions_added": 4, "repeated_observation_ids": 1},
                                   {"source_system": "JMA_BULLETIN", "full_bronze_records": 10, "selected_records": 8,
                                    "outside_temporal_selection": 2, "parser_rejects": 0, "unique_revisions_added": 8, "repeated_observation_ids": 0}]}
        self.snapshots = {t: i + 1 for i, t in enumerate(sorted(runtime.TABLES))}
        self.gold = {"gold_run_id": op, "namespace": request["namespace"], "status": "COMMITTED", "published": False,
                     "identity_sha256": "e" * 64, "tables": {t: {"snapshot_id": self.snapshots[t], "table": request["namespace"] + "." + t,
                      "count": 8 if t == "event_current" else 10 if t == "event_source_bridge" else 1} for t in runtime.TABLES}}
        self.commit_raw = encoded(self.gold); bundle_sha = hashlib.sha256(self.commit_raw).hexdigest()
        checks = {p + t: {"sql": "SELECT 0", "expected": 1 if p == "identity:" else 0,
                         "actual": 1 if p == "identity:" else 0} for p in ("identity:", "unique:") for t in runtime.TABLES}
        checks.update({"count:" + t: {"sql": "SELECT count(*)", "expected": self.gold["tables"][t]["count"],
                                    "actual": self.gold["tables"][t]["count"]} for t in runtime.TABLES})
        for name in ("required_fields", "bridge_lineage", "source_counts", "primary_provenance", "utc_jst", "dimensions",
                     "aggregate_count", "changed_run_scope", "outside:event_current", "outside:event_source_bridge"):
            checks[name] = {"sql": "SELECT 0", "expected": 0, "actual": 0}
        verification = {"gold_run_id": op, "namespace": request["namespace"], "identity_sha256": "e" * 64,
                        "bundle_sha256": bundle_sha, "snapshot_bundle": self.gold["tables"], "engine": "TRINO",
                        "verify_status": "PASSED", "published": False, "checks": checks}
        self.artifacts = {"readiness": {"status": "SourcesReady", "bronze_inputs": [{"pin": p, "status": "BronzeReady", "count": 5} for p in pins]},
                          "silver": {"run_id": op, "status": "FullMonthReady", "scope_completeness": "COMPLETE_MONTHS",
                                     "request_sha256": runtime.digest(request), "reconciliation": self.counts, "affected_months": ["2023-09"]},
                          "gold": self.gold, "verify": verification,
                          "publish": {**deepcopy(verification), "published": True, "publication_id": "pub_" + bundle_sha, "event_count": 7}}
        self.artifacts["bronze"] = {**deepcopy(self.artifacts["readiness"]), "status": "BronzeVerified"}
        self.base = "s3://bucket/silver/real-etl/" + op + "/"
        self.build_chain()

    def build_chain(self):
        self.objects = {self.base + "context.json": encoded(self.context),
                        "s3://bucket/silver/gold-commits/" + self.context["operation_id"] + "/commit.json": self.commit_raw}
        receipts = []; upstream = None
        for phase in runtime.PHASES:
            receipt = {k: self.context[k] for k in ("contract_version", "mode", "operation_id", "context_sha256")}
            receipt.update(phase=phase, status="ok", upstream_sha256=runtime.digest(upstream) if upstream else None,
                           artifact_uri=self.base + phase + ".json", artifact_sha256=runtime.digest(self.artifacts[phase]))
            if phase == "silver": receipt["counts"] = self.counts
            if runtime.PHASES.index(phase) >= 3: receipt["snapshots"] = self.snapshots
            if phase == "publish": receipt.update(published=True, publication_id=self.artifacts[phase]["publication_id"], publication_status="Published")
            self.objects[self.base + phase + ".json"] = encoded(self.artifacts[phase])
            self.objects[self.base + phase + "-receipt.json"] = encoded(receipt)
            receipts.append(receipt); upstream = receipt
        self.summary = {"operation_id": self.context["operation_id"], "context_sha256": self.context["context_sha256"],
                        "status": "Published", "published": True, "receipts": receipts, "snapshots": self.snapshots,
                        "publication_id": self.artifacts["publish"]["publication_id"]}
        self.objects["s3://bucket/silver/gold-verification/" + self.context["operation_id"] + "/publication.json"] = encoded(self.artifacts["publish"])
        expected = []
        for t in sorted(runtime.TABLES): expected += [self.gold["tables"][t]["count"], 1]
        self.scalar = Mock(side_effect=expected + [8, 10, 7, 7, 1])

    def verify(self):
        return qa.verify_chain(self.run, self.source, self.context, self.summary, self.objects.__getitem__, self.scalar, "bucket", "silver")

    def test_same_run_chain_and_independent_pinned_kpis(self):
        report = self.verify()
        self.assertEqual("PASSED", report["status"])
        self.assertTrue(report["scheduled_run"])
        self.assertEqual(17, len(report["sql_readback"]))
        sql = "\n".join(c.args[0] for c in self.scalar.call_args_list)
        self.assertIn("FOR VERSION AS OF", sql)
        self.assertNotIn("stable_serving_count", sql)
        self.assertEqual(7, report["natural_roi_event_count"])
        self.assertFalse(report["official_ml_dataset_exported"])

    def test_manual_dag_test_and_unsuccessful_tasks_not_acceptance(self):
        for key, value in (("run_type", "manual"), ("dag_id", "orc_07_real_etl"), ("state", "failed")):
            with self.subTest(key=key):
                run = deepcopy(self.run); run[key] = value
                with self.assertRaises(RuntimeError): qa.validate_run(run)
        self.run["tasks"]["real_etl"] = "skipped"
        with self.assertRaises(RuntimeError): self.verify()
        self.scalar.assert_not_called()

    def test_source_owner_interval_and_pins_cannot_be_stitched(self):
        for mutate in (lambda: self.source.update(run_id="other"),
                       lambda: self.source["usgs_context"].update(window_end_utc="2023-09-30T00:00:00Z"),
                       lambda: self.source["jma"]["archives"][0].update(sha256="f" * 64)):
            old = deepcopy(self.source); mutate()
            with self.assertRaises(RuntimeError): self.verify()
            self.source = old
        self.scalar.assert_not_called()

    def test_checksum_and_cross_run_artifacts_fail_before_sql(self):
        self.objects[self.base + "bronze.json"] = encoded({"status": "changed"})
        with self.assertRaisesRegex(RuntimeError, "ARTIFACT_SHA_CHANGED"): self.verify()
        self.build_chain()
        self.summary["receipts"][0]["artifact_uri"] = self.base.replace("real-etl/", "other/") + "readiness.json"
        with self.assertRaisesRegex(RuntimeError, "CROSS_RUN_ARTIFACT"): self.verify()
        self.scalar.assert_not_called()

    def test_true_balanced_flag_cannot_hide_negative_bool_or_inconsistent_counts(self):
        for name, value in (("current", True), ("history", -1), ("selected_input_records", 99)):
            bad = deepcopy(self.counts); bad[name] = value
            with self.subTest(name=name), self.assertRaises(RuntimeError): qa.reconcile(bad)

    def test_missing_full_month_or_sql_blocker_stops_before_kpi_readback(self):
        self.artifacts["silver"]["scope_completeness"] = "PARTIAL"
        self.build_chain()
        with self.assertRaisesRegex(RuntimeError, "FULL_MONTH_PROOF"): self.verify()
        self.artifacts["silver"]["scope_completeness"] = "COMPLETE_MONTHS"
        self.artifacts["verify"]["checks"]["required_fields"]["actual"] = 1
        self.build_chain()
        with self.assertRaisesRegex(RuntimeError, "TRINO_BLOCKER_FAILED"): self.verify()
        self.scalar.assert_not_called()

    def test_snapshot_or_publication_substitution_fails(self):
        self.summary["snapshots"] = {t: 99 for t in runtime.TABLES}
        with self.assertRaisesRegex(RuntimeError, "SNAPSHOTS_CHANGED"): self.verify()
        self.build_chain()
        self.artifacts["publish"]["publication_id"] = "pub_" + "0" * 64
        self.build_chain()
        with self.assertRaisesRegex(RuntimeError, "PUBLICATION_ID_CHANGED"): self.verify()

    def test_kpi_drift_rejected_even_when_publication_report_passed(self):
        self.scalar = Mock(return_value=999)
        with self.assertRaisesRegex(RuntimeError, "KPI_SQL_MISMATCH"): self.verify()

    def test_parser_rejects_and_per_source_imbalance_block(self):
        for name, value in (("parser_rejects", 1), ("outside_temporal_selection", 42)):
            bad = deepcopy(self.counts); bad["sources"][0][name] = value
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, "SOURCE_COUNT_MISMATCH"): qa.reconcile(bad)

    def test_forged_nonzero_blocker_expectation_is_not_allowed(self):
        self.artifacts["verify"]["checks"]["required_fields"].update(expected=1, actual=1)
        self.build_chain()
        with self.assertRaisesRegex(RuntimeError, "TRINO_EXPECTATION_CHANGED"): self.verify()
        self.scalar.assert_not_called()

    def test_actual_legacy_and_disabled_daily_runs_remain_not_ready(self):
        fake = {"airflow.models.dagrun": SimpleNamespace(DagRun=object()),
                "airflow.models.taskinstance": SimpleNamespace(TaskInstance=object())}
        for legacy in (True, False):
            with self.subTest(legacy=legacy), tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as folder:
                root = Path(folder); slug = "daily-" + hashlib.sha256(self.run["run_id"].encode()).hexdigest()[:32]
                (root / slug).mkdir(); (root / slug / "run_summary.json").write_bytes(encoded(self.source))
                row = SimpleNamespace(**{k: self.run[k] for k in ("run_id", "run_type", "state")},
                                      data_interval_start=qa.utc(self.run["data_interval_start"]), data_interval_end=qa.utc(self.run["data_interval_end"]))
                tasks = self.run["tasks"] if not legacy else {t: s for t, s in self.run["tasks"].items() if t != "real_etl"}
                session = MagicMock()
                dag_query = Mock(); dag_query.filter_by.return_value.one_or_none.return_value = row
                task_query = Mock(); task_query.filter_by.return_value.all.return_value = [SimpleNamespace(task_id=t, state=s) for t, s in tasks.items()]
                session.query.side_effect = [dag_query, task_query]
                manager = MagicMock(); manager.__enter__.return_value = session
                fake["airflow.utils.session"] = SimpleNamespace(create_session=lambda: manager)
                with patch.dict(sys.modules, fake):
                    report = qa.collect(self.run["run_id"], {"SOURCE_READINESS_ROOT": str(root), "ORC07_STAGING_ROOT": str(root / "etl")})
                self.assertEqual("NOT_READY", report["status"])
                self.assertFalse(report["ml_upstream_eligible"])
                self.assertEqual(["real_etl"] if legacy else [], report["missing_tasks"])

    def test_trino_readback_is_bounded_and_rejects_cross_origin_next_uri(self):
        environment = {"TRINO_HOST": "trino", "TRINO_INTERNAL_PORT": "8080"}
        opener = Mock(); opener.open.return_value = io.BytesIO(encoded({"data": [[7]]}))
        with patch.object(qa, "build_opener", return_value=opener):
            self.assertEqual(7, qa.trino_scalar("SELECT 7", environment))
        for result, reason in (({"nextUri": "http://evil.example/v1/statement/a"}, "NEXT_URI_REJECTED"),
                               ({"error": {"message": "private credential"}}, "QUERY_FAILED"),
                               ({"data": [[True]]}, "SCALAR_REQUIRED")):
            opener.open.return_value = io.BytesIO(encoded(result))
            with self.subTest(reason=reason), patch.object(qa, "build_opener", return_value=opener):
                with self.assertRaisesRegex(RuntimeError, reason) as error: qa.trino_scalar("SELECT 7", environment)
                self.assertNotIn("credential", str(error.exception))
        self.assertIsNone(qa.NoRedirect().redirect_request(None, None, None, None, None, None))

    def test_duplicate_keys_nonfinite_and_local_symlink_rejected(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'[]'):
            with self.subTest(raw=raw), self.assertRaises(Exception): qa.load_bytes(raw)
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as folder:
            root = Path(folder); (root / "file.json").write_text('{}'); (root / "alias.json").symlink_to(root / "file.json")
            with self.assertRaisesRegex(RuntimeError, "LOCAL_EVIDENCE_UNAVAILABLE"): qa.local_json(root, "alias.json")


if __name__ == "__main__": unittest.main()
