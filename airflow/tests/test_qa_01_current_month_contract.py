"""Design-only metadata policy regressions, never proof of a live daily run."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "airflow/dags"))
import current_month_contract as contract
import real_etl_runtime as legacy


class CurrentMonthContractTest(unittest.TestCase):
    def setUp(self):
        self.raw = (ROOT / "config/orchestration/current_month_asof_v1.json").read_bytes()
        self.policy = contract.load_policy(self.raw)
        self.run = {"dag_id": "orc_02_daily_sources", "run_id": "scheduled__2026-10-21T00:15:00+00:00",
                    "run_type": "scheduled", "data_interval_start": "2026-10-21T00:15:00Z",
                    "data_interval_end": "2026-10-22T00:15:00Z", "conf": {}}
        self.now = datetime(2026, 12, 1, tzinfo=timezone.utc)

    def plan(self, run=None):
        return contract.plan_scheduled_run(run or self.run, self.policy, now=self.now)

    def set_dates(self, start, end):
        self.run["run_id"] = "scheduled__" + start.replace("Z", "+00:00")
        self.run["data_interval_start"] = start
        self.run["data_interval_end"] = end

    def test_current_month_requires_bootstrap_not_just_three_day_delta(self):
        result = self.plan()
        self.assertEqual(result["revision_start_utc"], "2026-10-19T00:00:00Z")
        self.assertEqual(result["event_cutoff_utc"], "2026-10-22T00:00:00Z")
        scope, = result["required_capture_scopes"]
        self.assertEqual(scope["event_start_utc"], "2026-10-01T00:00:00Z")
        self.assertEqual(scope["event_end_utc_exclusive"], result["event_cutoff_utc"])
        self.assertIsNone(scope["observation_cutoff_utc"])

    def test_rollover_rechecks_previous_month_and_new_month(self):
        self.set_dates("2026-11-01T00:15:00Z", "2026-11-02T00:15:00Z")
        scopes = self.plan()["required_capture_scopes"]
        self.assertEqual([s["month"] for s in scopes], ["2026-10", "2026-11"])
        self.assertEqual(scopes[0]["event_end_utc_exclusive"], "2026-11-01T00:00:00Z")
        self.assertEqual(scopes[1]["event_end_utc_exclusive"], "2026-11-02T00:00:00Z")

    def test_first_day_has_no_fake_empty_new_month(self):
        self.set_dates("2026-10-31T00:15:00Z", "2026-11-01T00:15:00Z")
        self.assertEqual([s["month"] for s in self.plan()["required_capture_scopes"]], ["2026-10"])

    def test_leap_year_and_year_rollover(self):
        self.set_dates("2024-02-29T00:15:00Z", "2024-03-01T00:15:00Z")
        self.assertEqual(self.plan()["required_capture_scopes"][0]["event_end_utc_exclusive"], "2024-03-01T00:00:00Z")
        self.set_dates("2025-01-01T00:15:00Z", "2025-01-02T00:15:00Z")
        self.assertEqual([s["month"] for s in self.plan()["required_capture_scopes"]], ["2024-12", "2025-01"])

    def test_seed_is_clamped_and_first_target_day_is_valid(self):
        self.set_dates("2023-01-01T00:15:00Z", "2023-01-02T00:15:00Z")
        self.assertEqual(self.plan()["revision_start_utc"], "2023-01-01T00:00:00Z")

    def test_deterministic_identity_and_input_not_mutated(self):
        original, policy = deepcopy(self.run), deepcopy(self.policy)
        first = self.plan()
        self.assertEqual(first, self.plan())
        self.assertEqual(self.run, original)
        self.assertEqual(self.policy, policy)
        self.assertEqual(first["plan_sha256"], contract.digest({k:v for k,v in first.items() if k != "plan_sha256"}))
        changed = deepcopy(self.run)
        changed["run_id"] = "scheduled__2026-10-21T00:15:00Z"
        self.assertNotEqual(first["plan_sha256"], self.plan(changed)["plan_sha256"])

    def test_no_plan_is_publication_or_research_evidence(self):
        result = self.plan()
        for name in ("deployable", "published", "ml_upstream_eligible", "research_ready", "observed_coverage_proven"):
            self.assertIs(result[name], False)
        self.assertEqual(result["status"], "DESIGN_PLAN_ONLY")
        self.assertTrue(all(s["coverage_mode"] != "COMPLETE_MONTHS" for s in result["required_capture_scopes"]))

    def test_policy_cannot_be_promoted_by_a_boolean_flag(self):
        for name in ("research_ready", "publication_allowed", "absence_is_tombstone", "ml_upstream_eligible"):
            with self.subTest(name=name):
                original = self.policy[name]; self.policy[name] = True
                with self.assertRaisesRegex(ValueError, "DESIGN_MUST_NOT_AUTHORIZE_PUBLICATION"):
                    self.plan()
                self.policy[name] = original

    def test_invalid_limits_source_and_unknown_fields_fail_closed(self):
        for name, value in (("revision_window_days", True), ("max_rows", 100001),
                            ("required_event_sources", ["USGS", "JMA_BULLETIN"]),
                            ("release_status", "ENABLED"), ("arbitrary", "value")):
            with self.subTest(name=name), self.assertRaises(ValueError):
                contract.validate_policy({**self.policy, name: value})

    def test_duplicate_keys_nonfinite_and_oversized_policy_rejected(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b" " * 8193):
            with self.subTest(raw=raw[:20]), self.assertRaises(ValueError):
                contract.load_policy(raw)

    def test_manual_run_conf_missing_identity_and_future_interval_rejected(self):
        for patch in ({"run_type":"manual"}, {"conf":{"research_ready":True}}, {"dag_id":"other"},
                      {"run_id":True}, {"run_id":"scheduled__2026-99-21T00:15:00Z"},
                      {"run_id":"scheduled__2026-10-21T00:15:00+07:00"}):
            with self.subTest(patch=patch), self.assertRaises(ValueError):
                self.plan({**self.run, **patch})
        with self.assertRaisesRegex(ValueError, "CLOSED_DAILY_INTERVAL_REQUIRED"):
            contract.plan_scheduled_run(self.run, self.policy, now=datetime(2026, 10, 21, tzinfo=timezone.utc))

    def test_naive_non_z_and_short_intervals_are_rejected(self):
        for end in ("2026-10-22T00:15:00", "2026-10-22T00:15:00+00:00", "2026-10-21T12:15:00Z"):
            with self.subTest(end=end), self.assertRaises(ValueError):
                self.plan({**self.run, "data_interval_end":end})

    def test_midnight_schedule_and_non_minute_boundaries_are_rejected(self):
        for start, end in (("2026-10-21T00:00:00Z", "2026-10-22T00:00:00Z"),
                           ("2026-10-21T00:15:01Z", "2026-10-22T00:15:01Z")):
            with self.subTest(end=end), self.assertRaises(ValueError):
                self.plan({**self.run, "data_interval_start":start, "data_interval_end":end})

    def test_naive_clock_and_before_seed_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "AWARE_CLOCK_REQUIRED"):
            contract.plan_scheduled_run(self.run, self.policy, now=datetime(2026, 12, 1))
        self.set_dates("2022-12-31T00:15:00Z", "2023-01-01T00:15:00Z")
        with self.assertRaisesRegex(ValueError, "TARGET_PRECEDES_SOURCE_BASELINE"):
            self.plan()

    def test_existing_runtime_does_not_silently_enable_new_policy(self):
        self.assertFalse(legacy.enabled({}))
        self.assertFalse(legacy.enabled({"CURRENT_MONTH_POLICY": "current-month-asof-v1"}))
        with self.assertRaises(ValueError):
            contract.validate_policy({**self.policy, "coverage_mode":"COMPLETE_MONTHS"})


if __name__ == "__main__":
    unittest.main()
