"""ORC-06 runtime control-plane guards; no Docker, source network or business-data processing."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("month_runtime", ROOT / "scripts/full-month-state.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class FullMonthRuntimeTest(unittest.TestCase):
    def test_prepared_artifact_is_required_no_implicit_build(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(runtime.runtime, "RECEIPT", Path(folder) / "missing.json"):
            with self.assertRaisesRegex(ValueError, "PREPARE_REQUIRED"):
                runtime.verify_runtime()

    def test_existing_capture_is_reused_without_network(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(runtime.subprocess, "run") as run:
            path = Path(folder) / "capture"; path.write_bytes(b"original public bytes")
            runtime.capture(runtime.USGS_URL, path)
            run.assert_not_called(); self.assertEqual(path.read_bytes(), b"original public bytes")

    def test_tls_limits_and_arguments_are_fixed_no_shell(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "capture"
            with patch.object(runtime.subprocess, "run", return_value=Mock(returncode=0)) as run:
                runtime.capture(runtime.USGS_URL, path)
            argv = run.call_args.args[0]
            self.assertNotIn("--insecure", argv); self.assertNotIn("-k", argv)
            self.assertIn("--max-filesize", argv); self.assertIn(str(32 * 1024 * 1024), argv)
            self.assertIn("--max-time", argv); self.assertEqual(argv[-1], runtime.USGS_URL)
            self.assertNotIn("shell", run.call_args.kwargs)

    def test_failed_capture_retains_partial_and_does_not_echo_external_error(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "capture"
            with patch.object(runtime.subprocess, "run", return_value=Mock(returncode=60, stderr=b"private-config")):
                with self.assertRaisesRegex(ValueError, "curl_exit=60") as error:
                    runtime.capture(runtime.JMA_URL, path)
            self.assertNotIn("private-config", str(error.exception))
            self.assertTrue(path.with_suffix(".partial").exists()); self.assertFalse(path.exists())

    def test_partial_or_symlink_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(runtime.subprocess, "run") as run:
            path = Path(folder) / "capture"; partial = path.with_suffix(".partial"); partial.write_bytes(b"retained")
            with self.assertRaises(FileExistsError): runtime.capture(runtime.JMA_URL, path)
            self.assertEqual(partial.read_bytes(), b"retained"); run.assert_not_called()
            path.symlink_to(partial)
            with self.assertRaisesRegex(ValueError, "UNSAFE_CAPTURE_PATH"): runtime.capture(runtime.JMA_URL, path)

    def test_live_download_urls_match_java_acceptance_scope_and_no_docker_recipe(self):
        source = (ROOT / "spark/src/main/java/ie212/earthquake/spark/usgs/FullMonthAcceptanceJob.java").read_text()
        self.assertIn("2023-08-31T23%3A59%3A44Z", source)
        self.assertIn("2023-10-01T00%3A00%3A15.999Z", source)
        script = (ROOT / "scripts/full-month-state.py").read_text()
        self.assertNotIn('["docker"', script)
        self.assertNotIn('"docker compose', script)
        # Guard the test recipe: runtime preparation is a separate explicit command.
        makefile = (ROOT / "Makefile").read_text()
        recipe = makefile.split("qa-full-month-live:", 1)[1].split("\n\n", 1)[0]
        self.assertNotIn("prepare-full-month", recipe); self.assertNotIn("mvnw", recipe)


if __name__ == "__main__": unittest.main()
