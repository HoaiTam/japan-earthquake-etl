"""Read-only checkpoint partitioning tests; never starts Java or Docker."""
import importlib.util
from pathlib import Path
import unittest
import tests.test_dat03_portable_plan as plan_fixture

spec = importlib.util.spec_from_file_location("batches", Path(__file__).resolve().parents[1] / "scripts/readback-dat03-worker.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class ReadbackBatchTest(unittest.TestCase):
    def setUp(self):
        self.helper = plan_fixture.PortablePlanTest(methodName="runTest")
        self.helper.setUp(); self.addCleanup(self.helper.doCleanups)

    def test_month_range_crosses_year(self):
        self.assertEqual(m.months_between("2022-12", "2023-02"), ["2022-12", "2023-01", "2023-02"])

    def test_reversed_range_fails(self):
        with self.assertRaisesRegex(ValueError, "NONEMPTY_RANGE_REQUIRED"): m.months_between("2023-02", "2023-01")

    def test_one_group_is_not_silently_truncated(self):
        config = self.helper.fixture(months=["2023-01", "2023-02"])
        batches = m.partition(config, ["2023-01", "2023-02"])
        self.assertEqual(batches, [(["fixture-run"], ["2023-01", "2023-02"])])

    def test_missing_coverage_does_not_produce_batches(self):
        config = self.helper.fixture()
        with self.assertRaisesRegex(ValueError, "EXACT_COVERAGE_REQUIRED"): m.partition(config, ["2023-01", "2023-02"])

    def test_existing_checkpoint_is_not_overwritten(self):
        path = self.helper.root / "checkpoint.json"
        m.save(path, {"accepted": 1}); original = path.read_bytes()
        with self.assertRaises(FileExistsError): m.save(path, {"accepted": 999})
        self.assertEqual(path.read_bytes(), original)


if __name__ == "__main__": unittest.main()
