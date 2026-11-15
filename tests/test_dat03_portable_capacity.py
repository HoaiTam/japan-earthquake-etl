"""Launch-profile regression, no Spark/Docker/network."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("capacity", Path(__file__).resolve().parents[1] / "scripts/dat03-portable-capacity.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


class CapacityControllerTest(unittest.TestCase):
    def test_local_dependencies_do_not_use_driver_http_server(self):
        root = Path("/opt/pipeline/staging/dat03-portable-capacity/" + "a" * 32)
        with patch.object(m, "load", return_value={"request_sha256": "b" * 64}): cmd = m.command(root)
        self.assertEqual(cmd[cmd.index("--master") + 1], "local[1]")
        self.assertEqual(cmd[cmd.index("--driver-memory") + 1], "768m")
        self.assertTrue(all(s.startswith("local:/") for s in cmd[cmd.index("--jars") + 1].split(",")))
        self.assertIn("local:" + str(root / "runner.jar"), cmd)
        self.assertNotIn("--packages", cmd)

    def test_other_runtime_root_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "EXACT_RUNTIME_ROOT_REQUIRED"): m.command(Path.cwd() / "unapproved")

    def test_bronze_provenance_uses_only_the_fixed_read_only_java_entrypoint(self):
        root = Path("/opt/pipeline/staging/dat03-portable-capacity/" + "a" * 32)
        with patch.object(m, "load", return_value={"request_sha256": "b" * 64, "job": "bronze-provenance"}): cmd = m.command(root)
        self.assertEqual(cmd[:2], ["java", "-Xmx768m"])
        self.assertIn(m.BRONZE_CLASS, cmd); self.assertNotIn("spark-submit", cmd)

    def test_unknown_job_cannot_supply_an_arbitrary_entrypoint(self):
        root = Path("/opt/pipeline/staging/dat03-portable-capacity/" + "a" * 32)
        with patch.object(m, "load", return_value={"job": "write-gold"}):
            with self.assertRaisesRegex(ValueError, "READ_ONLY_JOB_REQUIRED"): m.command(root)

    def test_existing_receipt_is_never_overwritten(self):
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as tmp:
            p = Path(tmp) / "receipt.json"; m.save(p, {"accepted": False})
            with self.assertRaises(FileExistsError): m.save(p, {"accepted": True})
            self.assertIs(m.load(p)["accepted"], False)

    def test_duplicate_and_nonfinite_json_are_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as tmp:
            p = Path(tmp) / "input.json"
            for value in ('{"a":1,"a":2}', '{"a":NaN}'):
                p.write_text(value)
                with self.assertRaises(ValueError): m.load(p)

    def test_symlink_input_is_rejected(self):
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as tmp:
            p = Path(tmp); (p / "real").write_text("{}")
            (p / "link").symlink_to(p / "real")
            with self.assertRaisesRegex(ValueError, "SAFE_PATH_REQUIRED"): m.load(p / "link")


if __name__ == "__main__": unittest.main()
