"""QA-01 CLI cannot silently rebuild/trigger, or count incomplete evidence as passed."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location("qa01_cli", ROOT / "scripts/qa-01.py")
cli = importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)
RUN = "scheduled__2026-10-15T00:15:00+00:00"


class Qa01CliTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); (self.root / "airflow/dags").mkdir(parents=True)
        (self.root / "airflow/dags/qa_01_daily_e2e.py").write_text("fixture")
        self.health = Mock(returncode=0, stdout="\n".join(s + ":running:healthy" for s in cli.SERVICES).encode())

    def invoke(self, report, code):
        result = Mock(returncode=code, stdout=b"Airflow informational log\nQA01_REPORT=" + json.dumps(report).encode() + b"\n", stderr=b"")
        with patch.object(cli, "ROOT", self.root), patch.object(cli, "execute", side_effect=[self.health, result]) as command, contextlib.redirect_stdout(io.StringIO()):
            exit_code = cli.collect(RUN, ".env", "compose.yaml")
        return exit_code, command

    def test_not_ready_writes_report_and_nonzero_without_mutating_runtime(self):
        code, command = self.invoke({"task_id": "QA-01", "report_version": "qa-01-v1", "status": "NOT_READY",
                                    "runtime_complete": False, "ml_upstream_eligible": False,
                                    "reason": "QA01_REAL_ETL_EVIDENCE_MISSING"}, 1)
        self.assertEqual(1, code)
        calls = [c.args[0] for c in command.call_args_list]
        self.assertTrue(all(not ({"build", "up", "pull", "run", "start", "trigger", "clear"} & set(c)) for c in calls))
        self.assertIn("exec", calls[-1]); self.assertIn(RUN, calls[-1])
        self.assertEqual(1, len(list((self.root / "staging/qa-01").glob("*/report.json"))))
        log = next((self.root / "staging/qa-01").glob("*/collector.private.log"))
        self.assertEqual(0o600, log.stat().st_mode & 0o777)

    def test_unhealthy_stack_fails_before_exec(self):
        with patch.object(cli, "execute", return_value=Mock(returncode=0, stdout=b"")) as command:
            with self.assertRaisesRegex(RuntimeError, "STACK_NOT_HEALTHY"): cli.collect(RUN, ".env", "compose.yaml")
            self.assertEqual(1, command.call_count)

    def test_manual_or_shell_syntax_refused_before_docker(self):
        with patch.object(cli, "execute") as command:
            for value in ("manual__2026", "", RUN + "; echo bad", "../../other"):
                with self.subTest(value=value), self.assertRaises(RuntimeError): cli.collect(value, ".env", "compose.yaml")
            command.assert_not_called()

    def test_passed_requires_success_exit_and_exact_scheduled_identity(self):
        report = {"task_id": "QA-01", "report_version": "qa-01-v1", "status": "PASSED", "runtime_complete": True,
                  "scheduled_run": True, "run": {"run_id": RUN}, "ml_upstream_eligible": True, "sql_readback": [{}] * 17}
        self.assertEqual(0, self.invoke(report, 0)[0])
        for field, value in (("runtime_complete", False), ("scheduled_run", False), ("run", {"run_id": "other"}), ("sql_readback", [])):
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "REPORT_INVALID"):
                self.invoke({**report, field: value}, 0)
        with self.assertRaisesRegex(RuntimeError, "REPORT_INVALID"): self.invoke(report, 1)

    def test_collector_missing_or_conflicting_output_is_not_passed(self):
        with patch.object(cli, "ROOT", self.root), patch.object(cli, "execute", side_effect=[self.health, Mock(returncode=0, stdout=b"", stderr=b"private")]):
            with self.assertRaisesRegex(RuntimeError, "COLLECTOR_REPORT_MISSING"): cli.collect(RUN, ".env", "compose.yaml")


if __name__ == "__main__": unittest.main()
