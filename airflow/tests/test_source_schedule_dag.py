"""Offline graph test; real DagBag and scheduler evidence are separate."""
from datetime import datetime
import importlib.util
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch


class ScheduleDagTest(unittest.TestCase):
    def setUp(self):
        self.tasks = {}; self.edges = set(); self.options = {}
        test = self
        class Ref:
            def __init__(self, name): self.name = name
            def __rshift__(self, other): test.edges.add((self.name, other.name)); return other
        def dag(**options):
            self.options = options
            return lambda function: function
        def task(**options):
            def decorate(function):
                def bind(*args):
                    name = options["task_id"]
                    self.tasks[name] = {**self.options["default_args"], **options}
                    for value in args:
                        if isinstance(value, Ref): self.edges.add((value.name, name))
                    return Ref(name)
                return bind
            return decorate
        sdk = ModuleType("airflow.sdk"); sdk.dag = dag; sdk.task = task
        sdk.get_current_context = lambda: self.fail("task ran at import time")
        pendulum = ModuleType("pendulum"); pendulum.datetime = lambda *args, **kwargs: datetime(*args)
        interval = ModuleType("airflow.timetables.interval")
        interval.CronDataIntervalTimetable = lambda cron, **kwargs: {"cron": cron, **kwargs}
        path = Path(__file__).parents[1] / "dags/orc_02_daily_sources.py"
        spec = importlib.util.spec_from_file_location("offline_orc02", path)
        with patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.sdk": sdk,
                                      "airflow.timetables.interval": interval, "pendulum": pendulum}), patch.dict(
                                          "os.environ", {"SOURCE_SCHEDULE_PROFILE": "multi-source"}):
            sys.path.insert(0, str(path.parent))
            try: spec.loader.exec_module(importlib.util.module_from_spec(spec))
            finally: sys.path.pop(0)

    def test_explicit_data_interval_timetable_timezone_paused_single_run(self):
        self.assertEqual({"cron": "15 7 * * *", "timezone": "Asia/Ho_Chi_Minh"}, self.options["schedule"])
        self.assertFalse(self.options["catchup"]); self.assertTrue(self.options["is_paused_upon_creation"])
        self.assertEqual(1, self.options["max_active_runs"]); self.assertEqual(1, self.options["max_active_tasks"])
        self.assertEqual(datetime(2023, 1, 1), self.options["start_date"])

    def test_lease_encloses_sources_and_cleanup_is_not_success_leaf(self):
        self.assertEqual({("resolve_daily", "acquire_source_lease"), ("resolve_daily", "jma_readiness"),
                          ("acquire_source_lease", "jma_readiness"), ("resolve_daily", "usgs_bronze"),
                          ("jma_readiness", "usgs_bronze"), ("resolve_daily", "sources_ready"),
                          ("jma_readiness", "sources_ready"), ("usgs_bronze", "sources_ready"),
                          ("sources_ready", "release_source_lease"), ("sources_ready", "completion_gate"),
                          ("release_source_lease", "completion_gate"),
                          ("resolve_daily", "completion_gate")}, self.edges)
        self.assertEqual({"completion_gate"}, set(self.tasks) - {parent for parent, child in self.edges})
        self.assertEqual("all_done", self.tasks["release_source_lease"]["trigger_rule"])
        self.assertNotIn("trigger_rule", self.tasks["completion_gate"])  # Airflow default all_success.
        self.assertEqual(0, self.tasks["acquire_source_lease"]["retries"])

    def test_all_real_source_dags_share_guard_and_strict_completion(self):
        root = Path(__file__).parents[1] / "dags"
        for name in ("usg_04_usgs_ingest.py", "jma_04_year_backfill.py", "orc_02_daily_sources.py"):
            source = (root / name).read_text()
            self.assertIn('lease("acquire", owner(get_current_context()))', source)
            self.assertIn('lease("release", owner(get_current_context()))', source)
            self.assertIn('task_id="completion_gate"', source)


if __name__ == "__main__": unittest.main()
