"""ORC-07 adapter boundaries; synthetic receipts are not real publication evidence."""
from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import real_etl_runtime as runtime
from source_run_guard import lease, SourceRunBusy


class RealEtlRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.env = {"ORC07_STAGING_ROOT": self.tmp.name + "/etl", "SOURCE_GUARD_ROOT": self.tmp.name + "/guard"}
        self.request = {"resolver_version": "orc-06-v1", "run_id": "template", "dag_id": "template", "target_months": ["2023-09"], "bronze_inputs": [],
                        "namespace": "iceberg.gold_qa_test", "config_version": "test", "code_version": "test", "baseline": None,
                        "baseline_snapshots": {name: 0 for name in runtime.TABLES}, "processed_at_utc": "2026-10-15T00:00:00Z",
                        "source_cutoff_utc": "2026-10-15T00:00:00Z"}
        path = runtime.root(self.env) / "requests/scope.json"; path.parent.mkdir(parents=True)
        path.write_text(json.dumps(self.request)); self.sha = hashlib.sha256(path.read_bytes()).hexdigest()
        self.context = runtime.resolve("orc_07_real_etl", "manual__run:1", "scope", self.sha,
                                       "2023-09-01T00:00:00Z", "2023-10-01T00:00:00Z", self.env)
        self.owner = {"dag_id": self.context["dag_id"], "run_id": self.context["run_id"]}
        lease("acquire", self.owner, self.env)

    def result(self, phase, context, upstream, env):
        receipt = {"contract_version": runtime.VERSION, "mode": "real", "operation_id": context["operation_id"],
                   "context_sha256": context["context_sha256"], "phase": phase, "status": "ok",
                   "upstream_sha256": runtime.digest(upstream) if upstream else None,
                   "artifact_uri": "s3://bucket/silver/" + phase + ".json", "artifact_sha256": "a" * 64}
        if phase == "silver": receipt["counts"] = {"balanced": True}
        if runtime.PHASES.index(phase) >= 3: receipt["snapshots"] = {name: i + 1 for i, name in enumerate(sorted(runtime.TABLES))}
        if phase == "publish": receipt.update(published=True, publication_id="pub_" + "b" * 64, publication_status="Published")
        return receipt

    def test_complete_chain_persists_every_phase_then_releases_only_exact_owner(self):
        with patch.object(runtime, "invoke", side_effect=self.result) as child:
            summary = runtime.execute(self.context, self.env)
        self.assertEqual(list(runtime.PHASES), [call.args[0] for call in child.call_args_list])
        self.assertEqual("HELD", lease("assert", self.owner, self.env)["status"])
        self.assertEqual("RELEASED", runtime.release(self.context, summary, self.env)["status"])
        self.assertEqual(6, len(json.loads((runtime.root(self.env) / self.context["operation_id"] / "summary.json").read_text())["receipts"]))

    def test_each_failure_stops_downstream_and_keeps_lease(self):
        for failed in runtime.PHASES:
            calls = []
            def child(phase, *args):
                calls.append(phase)
                if phase == failed: raise RuntimeError("private token=do-not-echo")
                return self.result(phase, *args)
            with self.subTest(failed=failed), patch.object(runtime, "invoke", side_effect=child):
                with self.assertRaisesRegex(RuntimeError, "REAL_ETL_INCOMPLETE_LEASE_RETAINED") as error:
                    runtime.execute(self.context, self.env)
                self.assertNotIn("private", str(error.exception))
                self.assertEqual(list(runtime.PHASES[:runtime.PHASES.index(failed)+1]), calls)
                lease("assert", self.owner, self.env)

    def test_timeout_and_interruption_keep_owner(self):
        import subprocess
        for error in (subprocess.TimeoutExpired("fixed", 1), KeyboardInterrupt()):
            with patch.object(runtime, "invoke", side_effect=error):
                with self.assertRaisesRegex(RuntimeError, "LEASE_RETAINED"): runtime.execute(self.context, self.env)
            lease("assert", self.owner, self.env)

    def test_previous_success_cannot_release_a_new_failed_attempt(self):
        with patch.object(runtime, "invoke", side_effect=self.result):
            summary = runtime.execute(self.context, self.env)
        with patch.object(runtime, "invoke", side_effect=TimeoutError()):
            with self.assertRaises(RuntimeError): runtime.execute(self.context, self.env)
        with self.assertRaisesRegex(RuntimeError, "OUTCOME_UNCONFIRMED"):
            runtime.release(self.context, summary, self.env)
        lease("assert", self.owner, self.env)

    def test_snapshot_drift_and_fake_publication_are_rejected(self):
        gold = self.result("gold", self.context, {}, self.env)
        verify = self.result("verify", self.context, gold, self.env)
        for field, value in (("snapshots", {name: 9 for name in runtime.TABLES}), ("mode", "mock"),
                             ("artifact_uri", "file:///tmp/mock"), ("upstream_sha256", "0" * 64)):
            bad = deepcopy(verify); bad[field] = value
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                runtime.validate_receipt("verify", self.context, gold, bad)
        publish = self.result("publish", self.context, verify, self.env); publish["published"] = False
        with self.assertRaises(RuntimeError): runtime.validate_receipt("publish", self.context, verify, publish)

    def test_identity_and_request_pin_cannot_change_on_retry(self):
        again = runtime.resolve("orc_07_real_etl", "manual__run:1", "scope", self.sha,
                                "2023-09-01T00:00:00Z", "2023-10-01T00:00:00Z", self.env)
        self.assertEqual(self.context, again)
        with self.assertRaisesRegex(RuntimeError, "REAL_CONTEXT_CHANGED"):
            runtime.resolve("orc_07_real_etl", "manual__run:1", "scope", self.sha,
                            "2023-08-01T00:00:00Z", "2023-10-01T00:00:00Z", self.env)
        self.assertNotEqual(runtime.operation("a", "b"), runtime.operation("a", "b/"))
        bad = deepcopy(self.context); bad["resolver_request"]["target_months"] = ["2023-10"]
        with self.assertRaisesRegex(RuntimeError, "REAL_CONTEXT_CHANGED"): runtime.execute(bad, self.env)

    def test_scope_files_reject_traversal_checksum_duplicate_fields_and_symlink(self):
        for key, checksum in (("../scope", self.sha), ("scope", "0" * 64)):
            with self.assertRaises(RuntimeError): runtime.load_request(key, checksum, self.env)
        file = runtime.root(self.env) / "requests/scope.json"
        file.write_text('{"run_id":1,"run_id":2}')
        checksum = hashlib.sha256(file.read_bytes()).hexdigest()
        with self.assertRaises(RuntimeError): runtime.load_request("scope", checksum, self.env)
        linked = file.with_name("linked.json"); linked.symlink_to(file)
        with self.assertRaisesRegex(RuntimeError, "PINNED_REQUEST_UNAVAILABLE"):
            runtime.load_request("linked", checksum, self.env)

    def test_daily_delta_preserves_full_month_pins_and_retry_cutoff(self):
        full = {"source_system": "USGS", "manifest_uri": "s3://bucket/bronze/full.json",
                "manifest_sha256": "a" * 64, "sha256": "b" * 64,
                "window_start_utc": "2023-08-31T23:59:44Z", "window_end_utc": "2023-10-01T00:00:16Z"}
        request = deepcopy(self.request); request["bronze_inputs"] = [full]
        file = runtime.root(self.env) / "requests/daily.json"; file.write_text(json.dumps(request))
        checksum = hashlib.sha256(file.read_bytes()).hexdigest()
        start, end = "2023-09-28T00:00:00Z", "2023-10-01T00:00:00Z"
        summary = {"status": "SourcesReady", "verified": True, "run_id": "daily__1",
                   "usgs_context": {"window_start_utc": start, "window_end_utc": end},
                   "usgs": {"manifest_uri": "s3://bucket/bronze/delta.json", "manifest_sha256": "c" * 64, "sha256": "d" * 64},
                   "jma": {"archives": [{"manifest_uri": "s3://bucket/bronze/jma.json", "manifest_sha256": "e" * 64,
                           "sha256": "f" * 64, "year": 2023, "segment": "annual", "catalog_release": "v1"}]}}
        def resolve(value):
            return runtime.resolve("orc_02_daily_sources", "daily__1", "daily", checksum, start, end, self.env, value)
        first = resolve(summary)
        pins = first["resolver_request"]["bronze_inputs"]
        self.assertEqual(3, len(pins)); self.assertIn(full, pins)
        self.assertEqual("daily", first["trigger"])
        self.assertEqual(first, resolve(summary))  # No moving cutoff on task retry.
        changed = deepcopy(summary); changed["usgs"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "REAL_CONTEXT_CHANGED"): resolve(changed)

    def test_disabled_daily_never_spawns_and_missing_enabled_configuration_fails(self):
        with patch.object(runtime, "execute") as execute:
            self.assertEqual({"status": "SourcesReady", "published": False}, runtime.daily({}, {}, self.env))
            execute.assert_not_called()
        with self.assertRaises(RuntimeError): runtime.enabled({"ORC07_DAILY_MODE": "mock"})
        plan = {"run_id": "x", "usgs": {"window_start_utc": "2023-09-28T00:00:00Z", "window_end_utc": "2023-10-01T00:00:00Z"}}
        with self.assertRaises(RuntimeError): runtime.daily(plan, {}, {**self.env, "ORC07_DAILY_MODE": "real"})

    def test_foreign_run_cannot_execute_or_release(self):
        lease("release", self.owner, self.env); lease("acquire", {"dag_id": "other", "run_id": "other"}, self.env)
        with patch.object(runtime, "invoke") as child:
            with self.assertRaises(SourceRunBusy): runtime.execute(self.context, self.env)
            child.assert_not_called()

    def test_manual_dag_has_no_schedule_and_strict_leaf(self):
        from test_etl_dag_contract import OfflineSdk
        from types import ModuleType
        sdk = OfflineSdk(); airflow_sdk = ModuleType("airflow.sdk")
        airflow_sdk.dag, airflow_sdk.task = sdk.dag, sdk.task
        airflow_sdk.get_current_context = lambda: self.fail("body executed at import")
        pendulum = ModuleType("pendulum"); pendulum.datetime = lambda *a, **k: None
        path = Path(__file__).parents[1] / "dags/orc_07_real_etl.py"
        spec = importlib.util.spec_from_file_location("orc07_dag", path)
        with patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.sdk": airflow_sdk, "pendulum": pendulum}):
            spec.loader.exec_module(importlib.util.module_from_spec(spec))
        self.assertIsNone(sdk.options["schedule"])
        self.assertEqual({"completion_gate"}, set(sdk.tasks)-{parent for parent, child in sdk.edges})
        self.assertTrue(all(task["options"]["trigger_rule"] == "all_success" for task in sdk.tasks.values()))


if __name__ == "__main__": unittest.main()
