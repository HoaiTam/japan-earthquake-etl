"""Offline Airflow SDK graph test: importing a DAG must not execute the plan."""
from datetime import datetime
import importlib.util
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch


class BackfillDagTest(unittest.TestCase):
    def test_manual_paused_single_task_lease_and_strict_sole_completion_leaf(self):
        tasks = {}; edges = set(); options = {}
        class Ref:
            def __init__(self, name): self.name = name
            def __rshift__(self, other): edges.add((self.name, other.name)); return other
        def dag(**settings):
            options.update(settings); return lambda function: function
        def task(**settings):
            def decorate(function):
                def bind(*args):
                    name = settings["task_id"]; tasks[name] = {**options["default_args"], **settings}
                    for arg in args:
                        if isinstance(arg, Ref): edges.add((arg.name, name))
                    return Ref(name)
                return bind
            return decorate
        sdk = ModuleType("airflow.sdk"); sdk.dag = dag; sdk.task = task
        sdk.get_current_context = lambda: self.fail("executed at import")
        pendulum = ModuleType("pendulum"); pendulum.datetime = lambda *args, **kwargs: datetime(*args)
        path = Path(__file__).parents[1] / "dags/orc_03_backfill.py"
        spec = importlib.util.spec_from_file_location("offline_orc03", path)
        with patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.sdk": sdk, "pendulum": pendulum}):
            spec.loader.exec_module(importlib.util.module_from_spec(spec))
        self.assertIsNone(options["schedule"]); self.assertFalse(options["catchup"])
        self.assertTrue(options["is_paused_upon_creation"])
        self.assertEqual(1, options["max_active_runs"]); self.assertEqual(1, options["max_active_tasks"])
        self.assertEqual({("resolve_plan", "acquire_source_lease"), ("resolve_plan", "execute_scope"),
            ("resolve_plan", "release_source_lease"),
            ("acquire_source_lease", "execute_scope"), ("execute_scope", "release_source_lease"),
            ("resolve_plan", "completion_gate"), ("execute_scope", "completion_gate"),
            ("release_source_lease", "completion_gate")}, edges)
        self.assertEqual({"completion_gate"}, set(tasks) - {parent for parent, child in edges})
        self.assertEqual("all_done", tasks["release_source_lease"]["trigger_rule"])
        self.assertNotIn("trigger_rule", tasks["completion_gate"])
        self.assertTrue(all(task["retries"] == 0 for task in tasks.values()))


if __name__ == "__main__": unittest.main()
