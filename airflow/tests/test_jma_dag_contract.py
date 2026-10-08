"""Static DAG/deployment contract; not a claim of live scheduler execution."""
import ast
from pathlib import Path
import unittest

REPOSITORY = Path(__file__).parents[2]


class JmaDagContractTest(unittest.TestCase):
    def setUp(self):
        self.source = (REPOSITORY / "airflow/dags/jma_04_year_backfill.py").read_text()
        self.tree = ast.parse(self.source)

    def test_manual_paused_no_catchup_and_bounded_concurrency(self):
        for expression in ('dag_id="jma_04_year_backfill"', "schedule=None", "catchup=False",
                           "is_paused_upon_creation=True", "max_active_runs=1",
                           "max_active_tis_per_dag=MAX_CONCURRENCY", "1 <= MAX_CONCURRENCY <= 4"):
            self.assertIn(expression, self.source)

    def test_dynamic_mapping_and_all_done_summary_before_strict_gate(self):
        self.assertIn('@task_group(group_id="jma_year_backfill")', self.source)
        self.assertIn("ingest_archive.partial(plan=plan).expand(archive=archives)", self.source)
        self.assertIn('trigger_rule="all_done"', self.source)
        self.assertIn("ingested >> summary", self.source)
        self.assertIn("ready = gate(summary)", self.source)
        self.assertIn("completion_gate(ready, acquired, released)", self.source)
        self.assertIn("ready >> released", self.source)
        self.assertIn('return [] if plan["preview"] else plan["archives"]', self.source)
        self.assertIn('get_current_context()["ti"].try_number', self.source)
        self.assertIn("retries=2", self.source)

    def test_import_has_no_source_storage_or_full_inventory_calls(self):
        top_calls = [node for node in self.tree.body if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)]
        self.assertEqual(["jma_backfill"], [node.value.func.id for node in top_calls])
        for unwanted in ("requests.", "boto3", "zipfile", "print("):
            self.assertNotIn(unwanted, self.source)

    def test_java_command_and_inventory_are_packaged_and_configured(self):
        dockerfile = (REPOSITORY / "compose/airflow/Dockerfile").read_text()
        wrapper = (REPOSITORY / "compose/airflow/jma-runner.sh").read_text()
        compose = (REPOSITORY / "compose.yaml").read_text()
        self.assertIn("compose/airflow/jma-runner.sh", dockerfile)
        self.assertIn("config/jma/hypocenter_archives_v1.csv", dockerfile)
        self.assertIn("COPY config/jma ./config/jma", dockerfile)
        dockerignore = (REPOSITORY / ".dockerignore").read_text()
        for path in ("!config/", "!config/jma/", "!config/jma/hypocenter_archives_v1.csv", "!compose/airflow/jma-runner.sh"):
            self.assertIn(path, dockerignore)
        self.assertIn("ie212.earthquake.spark.jma.JmaYearIngestRunner", wrapper)
        for key in ("JMA_INGEST_RUNNER_COMMAND", "JMA_INGEST_RUNNER_TIMEOUT_SECONDS", "JMA_STAGING_ROOT",
                    "JMA_INVENTORY_PATH", "JMA_MAX_ARCHIVE_BYTES", "JMA_HTTP_TIMEOUT_MS", "JMA_BACKFILL_MAX_CONCURRENCY"):
            self.assertIn(key + ":", compose)


if __name__ == "__main__": unittest.main()
