"""Offline SQLite regression tests for BI-05 publication preflight SQL."""
from __future__ import annotations

from pathlib import Path
import sqlite3
import unittest

from tests.test_bi01_connectivity import POWERBI_DIR, POWER_QUERY_DIR, extract_sql_from_m_script

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PREFLIGHT_SQL_PATH = PROJECT_ROOT / "sql" / "gold" / "bi05_publication_preflight.sql"


class TestBi05PublicationPreflight(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        schema_sql = (POWERBI_DIR / "sample_schema.sql").read_text(encoding="utf-8")
        self.conn.executescript(schema_sql)
        self.conn.commit()
        self.preflight_sql = PREFLIGHT_SQL_PATH.read_text(encoding="utf-8").replace("iceberg.gold.", "")

    def tearDown(self) -> None:
        self.conn.close()

    def test_preflight_separates_absent_id_from_published_qa(self) -> None:
        rows = {row["case_name"]: dict(row) for row in self.conn.execute(self.preflight_sql).fetchall()}
        self.assertEqual(set(rows), {"published_qa", "absent_control_id"})

        published = rows["published_qa"]
        self.assertEqual(published["expected_eligible"], 1)
        self.assertEqual(published["matching_publication_rows"], 1)
        self.assertEqual(published["eligible_publications"], 1)
        self.assertEqual(published["catalog_non_passed_publications"], 0)
        self.assertEqual(published["bundle_sha256"], "bndl20230201sha256test01")
        self.assertEqual(published["event_count"], 9)

        absent = rows["absent_control_id"]
        self.assertEqual(absent["expected_eligible"], 0)
        self.assertEqual(absent["matching_publication_rows"], 0)
        self.assertEqual(absent["eligible_publications"], 0)
        self.assertEqual(absent["catalog_non_passed_publications"], 0)
        self.assertIsNone(absent["bundle_sha256"])
        self.assertIsNone(absent["serving_view"])
        self.assertIsNone(absent["event_count"])

    def test_preflight_and_active_bundle_distinguish_existing_non_passed_publication(self) -> None:
        for idx, bad_status in enumerate(("PENDING", "FAILED"), start=1):
            pub_id = f"pub-202302-unverified-{idx}"
            self.conn.execute(
                """
                INSERT INTO publication_status VALUES (
                    ?, 'gold-run-unverified', 99999999999999,
                    'iceberg.gold.earthquake_event_current', 'iceberg.gold.event_current',
                    '2023-02-08T01:00:00Z', '2023-02-08T01:01:00Z', '2023-02-08T01:02:00Z',
                    '2023-02-08T00:00:00Z', 10, '1.0', ?, 'bndl_unpublished_sha',
                    '{}', '{}', 'iceberg.gold.v_bndl_unpublished_earthquake_event_current'
                )
                """,
                (pub_id, bad_status),
            )
        self.conn.commit()

        rows = {row["case_name"]: dict(row) for row in self.conn.execute(self.preflight_sql)}
        self.assertEqual(set(rows), {"published_qa", "absent_control_id", "existing_non_passed_control"})
        self.assertEqual(rows["absent_control_id"]["matching_publication_rows"], 0)
        pending = rows["existing_non_passed_control"]
        self.assertEqual(pending["pinned_id"], "pub-202302-unverified-1")
        self.assertEqual(pending["expected_eligible"], 0)
        self.assertEqual(pending["matching_publication_rows"], 1)
        self.assertEqual(pending["eligible_publications"], 0)
        self.assertEqual(pending["catalog_non_passed_publications"], 2)
        self.assertIsNone(pending["bundle_sha256"])
        self.assertIsNone(pending["serving_view"])
        self.assertIsNone(pending["event_count"])

        active_bundle_m = (POWER_QUERY_DIR / "ActiveBundle.m").read_text(encoding="utf-8")
        for pub_id in ("qa-bi05-absent-control", "pub-202302-unverified-1", "pub-202302-unverified-2"):
            query = extract_sql_from_m_script(active_bundle_m, publication_id=pub_id)
            self.assertIsNone(self.conn.execute(query).fetchone(), pub_id)


if __name__ == "__main__":
    unittest.main()
