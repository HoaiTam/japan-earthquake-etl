"""ORC-07 host preparation vs QA separation; no Docker/network in these tests."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location("orc07_cli", ROOT / "scripts/real-etl.py")
cli = importlib.util.module_from_spec(spec); spec.loader.exec_module(cli)


class RealEtlCliTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); (self.root / "staging").mkdir()
        self.captures = self.root / "captures"; self.captures.mkdir()
        for name in ("usgs-2023-09.geojson", "jma-2023.zip"): (self.captures / name).write_bytes(b"fixture bytes")

    def command(self, action, effects):
        with patch.object(cli, "ROOT", self.root), patch.object(cli.lifecycle, "source_sha", return_value="a" * 64), \
             patch.object(cli.subprocess, "run", side_effect=effects) as run, patch("sys.argv", ["real-etl", action, "--captures", str(self.captures)]), \
             contextlib.redirect_stdout(io.StringIO()):
            cli.run()
        return run

    def test_prepare_only_builds_never_starts(self):
        run = self.command("prepare", [Mock(returncode=0)])
        args = run.call_args.args[0]
        self.assertIn("build", args); self.assertNotIn("up", args); self.assertNotIn("run", args)

    def test_qa_uses_existing_image_and_no_implicit_build_pull_start(self):
        image = json.dumps([{"Config": {"Labels": {cli.lifecycle.BUILD_LABEL: "a" * 64}}}]).encode()
        health = "\n".join(name + ":running:healthy" for name in ("minio", "airflow-postgres", "airflow-api-server", "iceberg-rest", "trino")).encode()
        run = self.command("qa", [Mock(returncode=0, stdout=image), Mock(stdout=health), Mock(returncode=0)])
        commands = [call.args[0] for call in run.call_args_list]
        self.assertTrue(all("build" not in argv and "up" not in argv and "pull" not in argv for argv in commands))
        self.assertIn("--no-deps", commands[-1]); self.assertNotIn("--no-build", commands[-1])
        self.assertEqual("never", commands[-1][commands[-1].index("--pull") + 1])
        self.assertEqual(str(self.captures.resolve()), run.call_args_list[1].kwargs["env"]["ORC07_CAPTURE_ROOT"])
        self.assertIn("pull_policy: never", (ROOT / "compose.yaml").read_text().split("  real-etl-qa:", 1)[1].split("  spark-gold-qa:", 1)[0])

    def test_missing_or_stale_image_fails_before_any_run(self):
        with self.assertRaisesRegex(RuntimeError, "IMAGE_STALE"):
            self.command("qa", [Mock(returncode=1)])

    def test_acceptance_seed_has_provided_dependencies_and_no_mark_success_shortcut(self):
        source = (ROOT / "airflow/dags/real_etl_acceptance.py").read_text()
        self.assertIn("/opt/spark/jars/*:/opt/spark/jobs/iceberg/*", source)
        self.assertIn("QA_LOGICAL_DATE_ALREADY_EXISTS", source)
        self.assertNotIn("mark_success_pattern=", source)


if __name__ == "__main__": unittest.main()
