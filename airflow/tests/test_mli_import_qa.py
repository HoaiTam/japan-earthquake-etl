"""Offline safety/receipt checks; storage acceptance is an explicit separate smoke."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import mli_import_qa as qa


class MliImportQaTests(unittest.TestCase):
    def report(self, namespace="iceberg.ml_qa_" + "a" * 32):
        return {"task": "MLI-03", "status": "PASSED", "namespace": namespace,
                "synthetic_component_qa": True, "scientific_approval": False,
                "checks": dict.fromkeys(qa.CHECKS, True),
                "publication": {"experiment_status": "CANDIDATE", "membership_snapshot_id": 11,
                                "summary_snapshot_id": 12, "experiment_snapshot_id": 13,
                                "readback": {"sequence_membership": 2, "sequence_summary": 1, "experiment_run": 1}}}

    def test_exact_pins_counts_and_component_scope_required(self):
        valid = self.report()
        qa.validate_report(valid, valid["namespace"])
        mutations = [("scientific_approval", True), ("synthetic_component_qa", False),
                     ("namespace", "iceberg.ml"), ("status", "COMPLETE")]
        for key, value in mutations:
            with self.subTest(key=key):
                bad = copy.deepcopy(valid); bad[key] = value
                with self.assertRaises(RuntimeError): qa.validate_report(bad, valid["namespace"])
        for key in qa.CHECKS:
            bad = copy.deepcopy(valid); del bad["checks"][key]
            with self.subTest(check=key), self.assertRaises(RuntimeError): qa.validate_report(bad, valid["namespace"])
        for value in (0, True, None, "11"):
            bad = copy.deepcopy(valid); bad["publication"]["membership_snapshot_id"] = value
            with self.subTest(pin=value), self.assertRaises(RuntimeError): qa.validate_report(bad, valid["namespace"])
        bad = copy.deepcopy(valid); bad["publication"]["readback"]["sequence_membership"] = 3
        with self.assertRaises(RuntimeError): qa.validate_report(bad, valid["namespace"])
        bad = copy.deepcopy(valid); bad["publication"]["readback"]["experiment_run"] = True
        with self.assertRaises(RuntimeError): qa.validate_report(bad, valid["namespace"])

    def exercise(self, outcome):
        actions = []
        def guard(action, owner, env):
            actions.append(action)
            return {"status": {"acquire": "ACQUIRED", "assert": "HELD", "release": "RELEASED"}[action], **owner}
        def process(command, **kwargs):
            self.assertEqual(command[command.index("--master") + 1], "local[1]")
            self.assertFalse(kwargs.get("shell", False))
            request_path = Path(command[-1]); request = json.loads(request_path.read_text())
            self.assertRegex(request["namespace"], r"^iceberg\.ml_qa_[0-9a-f]{32}$")
            if outcome == "timeout": raise subprocess.TimeoutExpired(command, 1200)
            if outcome == "failed": return subprocess.CompletedProcess(command, 1)
            (request_path.parent / "registry").mkdir()
            (request_path.parent / "registry/dataset.json").write_text("{}")
            (request_path.parent / "acceptance-report.json").write_text(json.dumps(self.report(request["namespace"])))
            return subprocess.CompletedProcess(command, 0)
        with tempfile.TemporaryDirectory() as root, patch.object(qa, "STAGING", Path(root)), \
                patch.object(qa, "lease", side_effect=guard), patch.object(qa, "java_identity", side_effect=lambda env, root: env), \
                patch.object(qa.subprocess, "run", side_effect=process), \
                patch.object(qa, "run_import", return_value={"status": "CANDIDATE", "readback": self.report()["publication"]["readback"]}):
            if outcome == "success": self.assertEqual(qa.run({"ICEBERG_CATALOG_NAME": "iceberg"})["status"], "PASSED")
            else:
                with self.assertRaises(RuntimeError): qa.run({"ICEBERG_CATALOG_NAME": "iceberg"})
        return actions

    def test_success_releases_only_own_confirmed_lease(self):
        self.assertEqual(self.exercise("success"), ["acquire", "assert", "release"])

    def test_exited_failed_local_process_releases_own_lease(self):
        self.assertEqual(self.exercise("failed"), ["acquire", "assert", "release"])

    def test_timeout_keeps_lease(self):
        self.assertEqual(self.exercise("timeout"), ["acquire"])


if __name__ == "__main__": unittest.main()
