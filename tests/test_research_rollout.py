"""Offline planning/capacity guard tests, never Docker or source network."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import ExitStack
import subprocess
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("research_rollout", ROOT / "scripts/research-rollout.py")
r = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(r)


class ResearchRolloutTest(unittest.TestCase):
    def report(self):
        prior = {"tables": {"event_current": {"snapshot_id": 9007199254740993, "count": 105282}}}
        latest = {"scope": {"baseline_snapshots": {"event_current": 9007199254740993}, "affected_months": ["2023-12"]},
                  "tables": {"event_current": {"snapshot_id": 9007199254740997}}}
        return {"namespace": "iceberg.dat03_e2e_" + "a" * 32,
                "cases": [{"month": month, "first": {"commit": latest if month == "2023-12" else prior}}
                          for month in ("2000-01", "2011-04", "2016-04", "2023-12")]}

    def test_both_splits_are_exact_disjoint_and_cover_288_months(self):
        a = r.months("2000-01", "2018-10"); b = r.months("2018-10", "2024-01")
        self.assertEqual((225, 63, 288), (len(a), len(b), len(set(a + b))))
        self.assertFalse(set(a) & set(b)); self.assertEqual("2018-09", a[-1]); self.assertEqual("2018-10", b[0])

    def test_jst_boundary_archive_is_needed_every_december(self):
        for year in range(2000, 2024):
            self.assertEqual([year, year + 1], r.dependencies(f"{year}-12"))
            self.assertEqual([year], r.dependencies(f"{year}-01"))
        self.assertEqual([2023, 2024], r.dependencies("2023-12"))
        for month in ("1999-12", "2024-01", "2000-13", "2000-1", "2000-01;DROP"):
            with self.assertRaises(ValueError): r.dependencies(month)

    def test_invalid_or_empty_range_cannot_be_a_complete_plan(self):
        for start, end in (("2000-01", "2000-01"), ("2024-01", "2000-01"), ("2000-13", "2001-01")):
            with self.assertRaises(ValueError): r.months(start, end)

    def test_capacity_sql_is_readonly_full_row_exact_symmetric_difference(self):
        query, rows = r.capacity_sql(self.report(), 32)
        self.assertEqual(3369024, rows)
        self.assertTrue(query.startswith("SELECT count(*) AS changed_rows"))
        self.assertEqual(2, query.count(" EXCEPT "))
        self.assertEqual(4, query.count("SELECT e.*, n.copy_id"))
        self.assertIn("9007199254740993", query); self.assertIn("9007199254740997", query)
        self.assertNotIn("approx", query.lower()); self.assertNotIn("INSERT", query)

    def test_capacity_copy_limit_is_typed_and_cannot_expand_unbounded(self):
        for value in (True, False, 0, 65, "32", 32.0):
            with self.assertRaises(ValueError): r.capacity_sql(self.report(), value)

    def test_capacity_pins_cannot_be_float_boolean_or_negative(self):
        for value in (9007199254740992.0, True, 0, -1, 9223372036854775808):
            report = self.report(); report["cases"][3]["first"]["commit"]["tables"]["event_current"]["snapshot_id"] = value
            with self.assertRaises(ValueError): r.capacity_sql(report, 32)

    def test_foreign_namespace_changed_baseline_or_month_cannot_be_queried(self):
        for field in ("namespace", "baseline", "affected"):
            report = self.report()
            if field == "namespace": report["namespace"] = "iceberg.gold"
            elif field == "baseline": report["cases"][3]["first"]["commit"]["scope"]["baseline_snapshots"]["event_current"] = 12
            else: report["cases"][3]["first"]["commit"]["scope"]["affected_months"] = ["2023-11"]
            with self.assertRaises(ValueError): r.capacity_sql(report, 32)

    def test_probe_requires_exact_integral_zero_not_merely_exit_zero(self):
        self.assertEqual("STRESS_PROBE_PASSED", r.classify_probe(0, '{"changed_rows":0}', ""))
        for value in (True, "0", 0.0, 1, None):
            self.assertEqual("READBACK_INVALID", r.classify_probe(0, json.dumps({"changed_rows": value}), ""))
        self.assertEqual("READBACK_INVALID", r.classify_probe(0, "not-json", ""))
        for value in ('["changed_rows"]', 'null', '1'):
            self.assertEqual("READBACK_INVALID", r.classify_probe(0, value, ""))

    def test_failure_classification_redacts_server_payload(self):
        self.assertEqual("BLOCKED_MEMORY", r.classify_probe(1, "", "Query exceeded per-node memory limit secret-token"))
        self.assertEqual("BLOCKED_TIME", r.classify_probe(1, "", "Query exceeded time limit"))
        self.assertEqual("FAILED_REVIEW_PRIVATE_LOG", r.classify_probe(1, "", "private server details"))

    def test_symlink_metadata_and_overwrite_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp).resolve(); file = folder / "data.json"; r.save(file, {"one": 1})
            with self.assertRaises(FileExistsError): r.save(file, {"one": 2})
            (folder / "link.json").symlink_to(file)
            with self.assertRaisesRegex(ValueError, "SYMLINK"): r.load(folder / "link.json")

    def test_idle_check_never_acquires_or_releases_lease(self):
        result = type("Result", (), {"returncode": 0, "stdout": "ROLLOUT_IDLE\n"})()
        with patch.object(r.subprocess, "run", return_value=result) as call:
            r.idle(); args = call.call_args.args[0]
            self.assertNotIn("lease(\"acquire", args[-1]); self.assertNotIn("lease(\"release", args[-1])
            self.assertIn("default_transaction_read_only=on", args[-1]); self.assertNotIn("from airflow", args[-1])
            self.assertIn("statement_timeout=10000", args[-1])
        with patch.object(r.subprocess, "run", return_value=type("Result", (), {"returncode": 1, "stdout": ""})()):
            with self.assertRaisesRegex(ValueError, "PREFLIGHT_FAILED"): r.idle()

    def test_trino_query_idle_check_is_bounded_readonly_and_never_cancels(self):
        result = subprocess.CompletedProcess([],0,"ROLLOUT_TRINO_IDLE\n","")
        with patch.object(r.subprocess,"run",return_value=result) as call:
            r.trino_queries_idle();script=call.call_args.args[0][-1]
            self.assertIn("/v1/query",script);self.assertIn("FINISHED",script);self.assertIn("FAILED",script)
            self.assertNotIn("DELETE",script);self.assertNotIn("POST",script)
            self.assertEqual(25,call.call_args.kwargs["timeout"])
        with patch.object(r.subprocess,"run",return_value=subprocess.CompletedProcess([],1,"","private server error")):
            with self.assertRaisesRegex(ValueError,"QUERY_NOT_IDLE"):r.trino_queries_idle()

    def mock_plan(self, mutate=None):
        evidence = json.loads((ROOT / "docs/evidence/DAT-03_RESEARCH_ARCHIVE_PROFILES.json").read_text())
        rows = r.profile.inventory(); captures = {}
        for entry in evidence["archives"]:
            captures[entry["year"]] = dict(entry, sha256=entry["archive_sha256"], bytes=1, inventory_metadata_drift=False)
        if mutate: mutate(evidence, rows, captures)
        def read(path, limit=None):
            path = Path(path)
            if path.name == "DAT-03_RESEARCH_ARCHIVE_PROFILES.json": return evidence
            year = int(path.parent.name[1:])
            return captures[year] if path.name == "capture.json" else evidence["archives"][year-2000]["profile"]
        def sha(path):
            return evidence["archives"][int(Path(path).parent.name[1:])-2000]["profile_sha256"] if Path(path).name.startswith("profile-") else "a" * 64
        with ExitStack() as stack:
            stack.enter_context(patch.object(r, "load", side_effect=read))
            stack.enter_context(patch.object(r.profile, "inventory", return_value=rows))
            stack.enter_context(patch.object(r.profile, "verified_capture", side_effect=lambda folder, row: captures[int(row["year"])]))
            stack.enter_context(patch.object(r.profile, "sha", side_effect=sha))
            return r.plan(ROOT / "staging/mock-plan-only")

    def test_plan_checks_25_profiles_and_seed_boundary_without_certifying_coverage(self):
        plan = self.mock_plan()
        self.assertEqual(288, len(plan["batches"])); self.assertEqual(25, len(plan["sources"]))
        self.assertEqual(3127011, plan["splits"]["REPRODUCTION"]["profile_accepted_rows"])
        self.assertEqual(1361003, plan["splits"]["EXTENSION"]["profile_accepted_rows"])
        december = next(b for b in plan["batches"] if b["month"] == "2022-12")
        self.assertEqual(["2023-01"], december["usgs_capture_months"])
        self.assertEqual(r.months("2023-01", "2024-01"), plan["required_usgs_months"])
        for key in ("official_exported", "research_ready", "coverage_verified", "full_capacity_accepted"):
            self.assertIs(False, plan[key])

    def test_plan_rejects_missing_boundary_inventory_or_source_provenance_drift(self):
        changes = [lambda e, rows, c: rows.pop(), lambda e, rows, c: c[2024].update(inventory_metadata_drift=True),
                   lambda e, rows, c: c[2000].update(sha256="a" * 64),
                   lambda e, rows, c: c[2000].update(retrieved_at_utc="2999-01-01T00:00:00Z")]
        for change in changes:
            with self.assertRaises(ValueError): self.mock_plan(change)

    def test_plan_rejects_missing_month_or_nontyped_negative_counts(self):
        with self.assertRaisesRegex(ValueError, "MONTH_MISSING"):
            self.mock_plan(lambda e, rows, c: e["archives"][0]["profile"]["accepted_rows_by_utc_month"].pop("2000-02"))
        for count in (True, -1, "1", 1.5):
            with self.assertRaisesRegex(ValueError, "COUNT_INVALID"):
                self.mock_plan(lambda e, rows, c: e["archives"][0]["profile"]["accepted_rows_by_utc_month"].update({"2000-02": count}))

    def test_bucket_queries_are_an_exhaustive_partition_not_approximate_hash_comparison(self):
        queries = [r.capacity_sql(self.report(), 32, b)[0] for b in "0123456789abcdef"]
        self.assertEqual(16, len(set(queries)))
        for b, query in zip("0123456789abcdef", queries):
            self.assertEqual(4, query.count("coalesce(e.canonical_event_id,'')"))
            self.assertIn("='" + b + "'", query); self.assertEqual(2, query.count(" EXCEPT "))
        for b in (True, 0, "00", "A", "g", "0' OR true"):
            with self.assertRaises(ValueError): r.capacity_sql(self.report(), 32, b)

    def test_invalid_strategy_or_probe_budget_cannot_contact_docker(self):
        with patch.object(r.subprocess, "run", side_effect=AssertionError("unexpected Docker")):
            for seconds in (True, 0, 301, "90"):
                with self.assertRaisesRegex(ValueError, "BUDGET_INVALID"): r.probe(ROOT, ROOT, 32, query_seconds=seconds)
            with self.assertRaisesRegex(ValueError, "STRATEGY_INVALID"): r.probe(ROOT, ROOT, 32, strategy="unbounded")

    def test_metadata_duplicate_keys_nonfinite_and_byte_bounds_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp).resolve() / "metadata.json"
            for raw in ('{"pin":1,"pin":2}', '{"count":NaN}'):
                file.write_text(raw)
                with self.assertRaises(ValueError): r.load(file)
            file.write_text('{"ok":1}')
            with self.assertRaisesRegex(ValueError, "BOUND"): r.load(file, 4)

    def run_probe(self, results, strategy="full", postflight_changed=False):
        with tempfile.TemporaryDirectory() as temp:
            deployment = Path(temp).resolve(); runtime = deployment / "staging/dat03-continuation" / ("a" * 32) / "runtime"
            runtime.mkdir(parents=True); r.save(runtime / "report.json", self.report()); r.save(runtime / "acceptance-recovery.json", {"status": "fixture"})
            evidence = {"checkpoint_recovery": {"attempt_id": "a" * 32, "report_sha256": r.profile.sha(runtime / "report.json"),
                                                "acceptance_sha256": r.profile.sha(runtime / "acceptance-recovery.json")}}
            original_load = r.load
            def read(path, *args):
                return evidence if Path(path).name == "DAT-03_CONTINUATION.json" else original_load(path, *args)
            with patch.object(r, "load", side_effect=read), patch.object(r, "services", side_effect=[{"same": 1}, {"same": 2 if postflight_changed else 1}]), \
                    patch.object(r, "idle"), patch.object(r, "trino_queries_idle"), patch.object(r.subprocess, "run", side_effect=results) as call:
                result = r.probe(deployment, runtime, 32, strategy)
            self.assertEqual(1, len(list((deployment / "staging/dat03-capacity").glob("*/receipt.json"))))
            return result, call.call_count

    def test_client_timeout_retains_partial_logs_and_never_resubmits(self):
        result, calls = self.run_probe([subprocess.TimeoutExpired("trino", 130, output=b"partial", stderr=b"partial error")])
        self.assertEqual(1, calls); self.assertIn("UNKNOWN_QUERY_OUTCOME", result["status"])
        self.assertEqual("PASSED_UNCHANGED", result["postflight_status"]); self.assertFalse(result["data_mutations"])
        self.assertFalse(result["lease_acquired_or_released"]); self.assertFalse(result["full_capacity_accepted"])

    def test_all_sixteen_buckets_must_succeed_and_service_restart_cannot_be_accepted(self):
        good = subprocess.CompletedProcess([], 0, '{"changed_rows":0}', "")
        result, calls = self.run_probe([good] * 16, strategy="buckets-v1")
        self.assertEqual(16, calls); self.assertEqual(16, result["completed_query_count"]); self.assertEqual("STRESS_PROBE_PASSED", result["status"])
        self.assertFalse(result["research_ready"]); self.assertFalse(result["full_capacity_accepted"])
        bad = subprocess.CompletedProcess([], 1, "", "Query exceeded maximum time limit")
        result, calls = self.run_probe([good, bad], strategy="buckets-v1")
        self.assertEqual(2, calls); self.assertEqual(16, result["required_query_count"]); self.assertEqual("BLOCKED_TIME", result["status"])
        result, calls = self.run_probe([good], postflight_changed=True)
        self.assertEqual("POSTFLIGHT_FAILED_NOT_ACCEPTED", result["status"]); self.assertEqual("SERVICE_CHANGED", result["postflight_status"])


if __name__ == "__main__":
    unittest.main()
