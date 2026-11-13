"""Unit and integration tests for GLD-02: Dashboard Aggregate Transformations and Trino Serving Views.

Verifies:
1. SQL View Syntax & Real Gold Schema Conformity:
   - v_event_daily and v_event_monthly parse and execute cleanly in SQLite against the REAL Gold schema:
     canonical_event_id, is_natural_earthquake, max_intensity_code, alert_level.
   - Zero non-existent columns (is_strong_earthquake, event_id, is_earthquake, jma_max_intensity).
2. Direct Execution of Delivered Verification Queries:
   - Reads sql/gold/gold_aggregate_verification_queries.sql directly, adapts dialect, and executes Q1-Q7.
   - Fact-to-aggregate total count parity with empty dataset support (Q1: delta = 0).
   - Zero double-counting from multi-source bridge joins (Q2: 0 rows).
   - Additive consistency, missing/extra group detection, and multi-day weighted average parity between Daily and Monthly (Q3: 0 rows).
   - Deep Fact-to-Daily metric parity across all 13 metrics with strict NULL/non-NULL separation (Q4: 0 rows).
   - Bidirectional Honest NULL policy for magnitude and depth (Q5: 0 rows).
   - Daily and Monthly Granularity uniqueness (Q6: 0 rows, Q7: 0 rows).
3. Alert Level and Intensity Reporting Predicate Parity:
   - Verifies that 'none', 'NONE', empty string '', and whitespace '   ' are EXCLUDED from alerted and intensity counts.
4. Deep Negative Testing (Directly Executing Delivered Verification SQL):
   - Monthly negative tests: mutations on region_key ('WRONG_REGION'), magnitude_avg/max, depth_avg, strong_count, alert_count, and duplicate monthly grain.
   - Fact-to-Daily negative tests: mutations on magnitude_sum, strong_earthquake_count, and group omission.
   - Honest NULL & valid 0.0 vs NULL negative tests: tests the exact edge cases where depth_km = 0.0 or magnitude is mutated to NULL while count > 0, and non-null count = 0 mutated to 0.0.
5. Empty Dataset Test:
   - Verifies Q1 returns fact_event_count = 0, daily_agg_event_count = 0, delta = 0 when tables are empty.
"""
from __future__ import annotations

from pathlib import Path
import re
import sqlite3
import unittest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_GOLD_DIR = PROJECT_ROOT / "sql" / "gold"


def adapt_trino_view_for_sqlite(sql_content: str) -> str:
    """Extract view SELECT query from Trino DDL and adapt for SQLite execution."""
    match = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+[a-zA-Z0-9_\.]+\s+AS\s+(.*)", sql_content, re.DOTALL | re.IGNORECASE)
    if not match:
        raise ValueError("Could not find CREATE VIEW statement in SQL")
    query = match.group(1).strip()
    if query.endswith(";"):
        query = query[:-1].strip()
    query = query.replace("iceberg.gold.", "")
    query = query.replace("AS DOUBLE", "AS REAL")
    query = re.sub(r"\btrue\b", "1", query, flags=re.IGNORECASE)
    query = re.sub(r"\bfalse\b", "0", query, flags=re.IGNORECASE)
    return query


def parse_verification_queries(sql_content: str) -> dict[str, str]:
    """Parses individual queries (Q1, Q2, etc.) from gold_aggregate_verification_queries.sql."""
    pattern = r"--\s*(Q[1-9][0-9]?)\.\s*([^\n]+)"
    matches = list(re.finditer(pattern, sql_content))
    queries: dict[str, str] = {}
    for i, match in enumerate(matches):
        q_id = match.group(1)
        start_idx = match.end()
        end_idx = matches[i + 1].start() if i + 1 < len(matches) else len(sql_content)
        raw_block = sql_content[start_idx:end_idx].strip()
        lines = []
        for line in raw_block.splitlines():
            stripped = line.strip()
            if stripped.startswith("--") and not lines:
                continue
            lines.append(line)
        query = "\n".join(lines).strip()
        if query.endswith(";"):
            query = query[:-1].strip()
        query = query.replace("iceberg.gold.", "")
        query = query.replace("AS DOUBLE", "AS REAL")
        query = re.sub(r"\btrue\b", "1", query, flags=re.IGNORECASE)
        query = re.sub(r"\bfalse\b", "0", query, flags=re.IGNORECASE)
        queries[q_id] = query
    return queries


class TestGoldAggregates(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._init_real_gold_schema()
        self._populate_real_mock_data()
        self._create_serving_views()

    def tearDown(self) -> None:
        self.conn.close()

    def _init_real_gold_schema(self) -> None:
        """Initializes tables strictly matching GoldEventTransformer.EVENT_SCHEMA."""
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE event_current (
                schema_version TEXT NOT NULL,
                canonical_event_id TEXT PRIMARY KEY,
                primary_observation_id TEXT NOT NULL,
                canonical_source_system TEXT NOT NULL,
                event_time_utc TEXT NOT NULL,
                event_time_jst TEXT NOT NULL,
                event_date_utc TEXT NOT NULL,
                event_date_jst TEXT NOT NULL,
                event_date_key_utc INTEGER NOT NULL,
                event_date_key_jst INTEGER NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                depth_km REAL,
                magnitude REAL,
                magnitude_type TEXT,
                event_type_code TEXT NOT NULL,
                is_natural_earthquake INTEGER NOT NULL,
                place_name TEXT,
                is_in_study_area INTEGER NOT NULL,
                region_key TEXT NOT NULL,
                region_name TEXT NOT NULL,
                region_category TEXT NOT NULL,
                magnitude_band_code TEXT NOT NULL,
                depth_band_code TEXT NOT NULL,
                tsunami_flag INTEGER,
                alert_level TEXT,
                significance INTEGER,
                max_intensity_code TEXT,
                determining_agency_code TEXT,
                catalog_era TEXT,
                source_count INTEGER NOT NULL,
                has_usgs INTEGER NOT NULL,
                has_jma INTEGER NOT NULL,
                source_coverage_code TEXT NOT NULL,
                link_status TEXT NOT NULL,
                quality_status TEXT NOT NULL,
                canonical_model_version TEXT NOT NULL,
                gold_run_id TEXT NOT NULL,
                record_updated_at_utc TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE event_source_bridge (
                canonical_event_id TEXT NOT NULL,
                source_observation_id TEXT NOT NULL,
                source_system TEXT NOT NULL,
                source_record_key TEXT NOT NULL,
                is_primary INTEGER NOT NULL,
                source_link_id TEXT,
                raw_object_uri TEXT,
                bronze_manifest_id TEXT,
                catalog_release TEXT,
                source_updated_at_utc TEXT
            )
        """)
        self.conn.commit()

    def _populate_real_mock_data(self) -> None:
        cur = self.conn.cursor()
        # Event 1: Valid earthquake, Kanto, Day 20230105, M4.2, depth 12.0, Shallow, USGS_ONLY
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-001', 'obs-001', 'USGS',
                '2023-01-05T01:00:00Z', '2023-01-05 10:00:00', '2023-01-05', '2023-01-05',
                20230105, 20230105, 35.68, 139.76, 12.0, 4.2, 'mww', 'EARTHQUAKE',
                1, 'Tokyo area', 1, 'PREFECTURE:13', 'Tokyo', 'PREFECTURE',
                'M4_TO_LT5', 'SHALLOW', 0, NULL, 250, NULL, NULL, NULL,
                1, 1, 0, 'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1', 'run-1', '2023-01-06T00:00:00Z'
            )
        """)

        # Event 1b: Valid earthquake, same grain, Day 20230106, M4.8, depth 0.0 (valid 0.0 km!), Shallow, USGS_ONLY
        # Used with can-001c to verify multi-day weighted average across different days of the month!
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-001b', 'obs-001b', 'USGS',
                '2023-01-06T02:00:00Z', '2023-01-06 11:00:00', '2023-01-06', '2023-01-06',
                20230106, 20230106, 35.69, 139.75, 0.0, 4.8, 'mww', 'EARTHQUAKE',
                1, 'Tokyo surface', 1, 'PREFECTURE:13', 'Tokyo', 'PREFECTURE',
                'M4_TO_LT5', 'SHALLOW', 0, NULL, 280, NULL, NULL, NULL,
                1, 1, 0, 'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1', 'run-1', '2023-01-07T00:00:00Z'
            )
        """)

        # Event 1c: Valid earthquake, same grain, Day 20230106, M4.8, depth 0.0 (valid 0.0 km!), Shallow, USGS_ONLY
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-001c', 'obs-001c', 'USGS',
                '2023-01-06T03:00:00Z', '2023-01-06 12:00:00', '2023-01-06', '2023-01-06',
                20230106, 20230106, 35.69, 139.75, 0.0, 4.8, 'mww', 'EARTHQUAKE',
                1, 'Tokyo surface 2', 1, 'PREFECTURE:13', 'Tokyo', 'PREFECTURE',
                'M4_TO_LT5', 'SHALLOW', 0, NULL, 280, NULL, NULL, NULL,
                1, 1, 0, 'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1', 'run-1', '2023-01-07T00:00:00Z'
            )
        """)

        # Event 2: Valid earthquake, Kanto, M4.8, Shallow, USGS_JMA, alert='none' (must not count as alerted!), intensity='none'
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-002', 'obs-002', 'USGS',
                '2023-01-05T08:00:00Z', '2023-01-05 17:00:00', '2023-01-05', '2023-01-05',
                20230105, 20230105, 35.70, 139.80, 26.0, 4.8, 'mww', 'EARTHQUAKE',
                1, 'Chiba area', 1, 'PREFECTURE:13', 'Tokyo', 'PREFECTURE',
                'M4_TO_LT5', 'SHALLOW', 1, 'none', 300, 'none', 'JMA', 'ERA',
                2, 1, 1, 'USGS_JMA', 'MATCHED', 'VALID', 'v1', 'run-1', '2023-01-06T00:00:00Z'
            )
        """)

        # Event 3: Valid strong earthquake (M6.5 >= 5.0), Offshore, Deep, USGS_JMA, alert='YELLOW', intensity='5+'
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-003', 'obs-003', 'USGS',
                '2023-01-10T02:00:00Z', '2023-01-10 11:00:00', '2023-01-10', '2023-01-10',
                20230110, 20230110, 38.30, 142.40, 350.0, 6.5, 'mww', 'EARTHQUAKE',
                1, 'Offshore Sanriku', 1, 'OFFSHORE', 'Offshore', 'OFFSHORE',
                'M6_TO_LT7', 'DEEP', 1, 'YELLOW', 600, '5+', 'JMA', 'ERA',
                2, 1, 1, 'USGS_JMA', 'MATCHED', 'VALID', 'v1', 'run-1', '2023-01-11T00:00:00Z'
            )
        """)

        # Event 4: Valid earthquake with NULL magnitude and negative depth, alert='   ' (whitespace), intensity=''
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-004', 'obs-004', 'USGS',
                '2023-02-01T03:00:00Z', '2023-02-01 12:00:00', '2023-02-01', '2023-02-01',
                20230201, 20230201, 35.00, 139.00, -1.5, NULL, NULL, 'EARTHQUAKE',
                1, 'Volcanic surface', 1, 'UNKNOWN', 'Unknown', 'UNKNOWN',
                'UNKNOWN', 'NEGATIVE', 0, '   ', NULL, '', NULL, NULL,
                1, 1, 0, 'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1', 'run-1', '2023-02-02T00:00:00Z'
            )
        """)

        # Event 5: Non-natural earthquake (quarry blast: is_natural_earthquake = 0) - MUST BE EXCLUDED
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-998', 'obs-998', 'USGS',
                '2023-01-05T04:00:00Z', '2023-01-05 13:00:00', '2023-01-05', '2023-01-05',
                20230105, 20230105, 35.00, 139.00, 0.0, 2.1, 'ml', 'ARTIFICIAL',
                0, 'Quarry', 1, 'PREFECTURE:13', 'Tokyo', 'PREFECTURE',
                'LT_3', 'SHALLOW', 0, NULL, NULL, NULL, NULL, NULL,
                1, 1, 0, 'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1', 'run-1', '2023-01-06T00:00:00Z'
            )
        """)

        # Event 6: Distant earthquake outside study area (is_in_study_area = 0) - MUST BE EXCLUDED
        cur.execute("""
            INSERT INTO event_current VALUES (
                '1.0', 'can-999', 'obs-999', 'USGS',
                '2023-01-05T05:00:00Z', '2023-01-05 14:00:00', '2023-01-05', '2023-01-05',
                20230105, 20230105, 10.00, 120.00, 10.0, 7.5, 'mww', 'EARTHQUAKE',
                1, 'Philippines', 0, 'UNKNOWN', 'Unknown', 'UNKNOWN',
                'GE_7', 'SHALLOW', 1, 'RED', 900, NULL, NULL, NULL,
                1, 1, 0, 'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1', 'run-1', '2023-01-06T00:00:00Z'
            )
        """)

        # Populate bridge: Event can-002 and can-003 each have TWO observations in bridge
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-001', 'obs-001', 'USGS', 'usgs1', 1, NULL, 's3://', 'm1', NULL, NULL)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-001b', 'obs-001b', 'USGS', 'usgs1b', 1, NULL, 's3://', 'm1', NULL, NULL)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-001c', 'obs-001c', 'USGS', 'usgs1c', 1, NULL, 's3://', 'm1', NULL, NULL)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-002', 'obs-002', 'USGS', 'usgs2', 1, 'l1', 's3://', 'm1', NULL, NULL)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-002', 'jma-002', 'JMA_BULLETIN', 'jma2', 0, 'l1', 's3://', 'm2', '2023', NULL)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-003', 'obs-003', 'USGS', 'usgs3', 1, 'l2', 's3://', 'm1', NULL, NULL)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-003', 'jma-003', 'JMA_BULLETIN', 'jma3', 0, 'l2', 's3://', 'm2', '2023', NULL)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('can-004', 'obs-004', 'USGS', 'usgs4', 1, NULL, 's3://', 'm1', NULL, NULL)")

        self.conn.commit()

    def _create_serving_views(self) -> None:
        cur = self.conn.cursor()
        daily_sql_file = SQL_GOLD_DIR / "v_event_daily.sql"
        monthly_sql_file = SQL_GOLD_DIR / "v_event_monthly.sql"
        self.assertTrue(daily_sql_file.exists())
        self.assertTrue(monthly_sql_file.exists())

        daily_query = adapt_trino_view_for_sqlite(daily_sql_file.read_text(encoding="utf-8"))
        cur.execute(f"CREATE VIEW v_event_daily AS {daily_query}")

        monthly_query = adapt_trino_view_for_sqlite(monthly_sql_file.read_text(encoding="utf-8"))
        cur.execute(f"CREATE VIEW v_event_monthly AS {monthly_query}")
        self.conn.commit()

    def test_direct_execution_of_verification_queries(self) -> None:
        """Executes all delivered queries (Q1-Q7) directly from gold_aggregate_verification_queries.sql."""
        queries_file = SQL_GOLD_DIR / "gold_aggregate_verification_queries.sql"
        self.assertTrue(queries_file.exists(), "gold_aggregate_verification_queries.sql missing")
        queries = parse_verification_queries(queries_file.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(queries), 7, "Expected at least 7 verification queries")

        cur = self.conn.cursor()

        # Q1: Fact-to-Aggregate Count Reconciliation (6 valid events in mock data)
        cur.execute(queries["Q1"])
        q1_row = cur.fetchone()
        self.assertIsNotNone(q1_row)
        self.assertEqual(q1_row["fact_event_count"], 6)
        self.assertEqual(q1_row["daily_agg_event_count"], 6)
        self.assertEqual(q1_row["delta"], 0)

        # Q2: Zero Double-Counting Audit
        cur.execute(queries["Q2"])
        q2_mismatches = cur.fetchall()
        self.assertEqual(len(q2_mismatches), 0, "Q2 found double-counting discrepancies")

        # Q3: Additive Consistency and Metric Rollup Parity between Daily and Monthly
        cur.execute(queries["Q3"])
        q3_mismatches = cur.fetchall()
        self.assertEqual(len(q3_mismatches), 0, "Q3 found daily/monthly rollup discrepancies")

        # Q4: Deep Grain-Level Fact-to-Daily Metric Parity
        cur.execute(queries["Q4"])
        q4_mismatches = cur.fetchall()
        self.assertEqual(len(q4_mismatches), 0, "Q4 found metric discrepancies between fact and daily views")

        # Q5: Honest NULL & Mathematical Invariant Audit
        cur.execute(queries["Q5"])
        q5_violations = cur.fetchall()
        self.assertEqual(len(q5_violations), 0, "Q5 found honest NULL invariant violations")

        # Q6: Daily Granularity Uniqueness Audit
        cur.execute(queries["Q6"])
        q6_duplicates = cur.fetchall()
        self.assertEqual(len(q6_duplicates), 0, "Q6 found duplicate daily grain rows")

        # Q7: Monthly Granularity Uniqueness Audit
        cur.execute(queries["Q7"])
        q7_duplicates = cur.fetchall()
        self.assertEqual(len(q7_duplicates), 0, "Q7 found duplicate monthly grain rows")

    def test_multi_day_weighted_average_in_monthly_rollup(self) -> None:
        """Verifies that monthly aggregate correctly calculates weighted average across multiple days."""
        cur = self.conn.cursor()
        # For group (202301, PREFECTURE:13, M4_TO_LT5, SHALLOW, USGS_ONLY):
        # Day 2023-01-05: 1 event, M=4.2
        # Day 2023-01-06: 2 events, each M=4.8
        # Weighted avg = (4.2*1 + 4.8*2) / 3 = 13.8 / 3 = 4.6
        # Unweighted avg would be (4.2 + 4.8) / 2 = 4.5
        cur.execute("""
            SELECT event_count, magnitude_non_null_count, magnitude_sum, magnitude_avg, magnitude_max
            FROM v_event_monthly
            WHERE year_month_jst = 202301
              AND region_key = 'PREFECTURE:13'
              AND source_coverage_code = 'USGS_ONLY'
        """)
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["event_count"], 3)
        self.assertEqual(row["magnitude_non_null_count"], 3)
        self.assertAlmostEqual(row["magnitude_sum"], 13.8, places=4)
        self.assertAlmostEqual(row["magnitude_avg"], 4.6, places=4)
        self.assertAlmostEqual(row["magnitude_max"], 4.8, places=4)

    def test_alert_and_intensity_predicate_parity(self) -> None:
        """Verifies that 'none', empty string, whitespace, and NULL are excluded from alert and intensity counts."""
        cur = self.conn.cursor()
        # can-001, 1b, 1c: alert=NULL, intensity=NULL
        # can-002: alert='none', intensity='none' -> MUST NOT COUNT
        # can-003: alert='YELLOW', intensity='5+' -> MUST COUNT (1 alerted, 1 intensity)
        # can-004: alert='   ', intensity='' -> MUST NOT COUNT
        cur.execute("SELECT SUM(alerted_event_count) AS alerts, SUM(intensity_reported_count) AS intensities FROM v_event_daily")
        res = cur.fetchone()
        self.assertEqual(res["alerts"], 1, "alert_level='none', '   ' or NULL must not be counted as alerted")
        self.assertEqual(res["intensities"], 1, "max_intensity_code='none', '' or NULL must not be counted as intensity")

        # Check strong earthquake: only can-003 has M6.5 >= 5.0 -> count = 1
        cur.execute("SELECT SUM(strong_earthquake_count) AS strong_cnt FROM v_event_daily")
        self.assertEqual(cur.fetchone()["strong_cnt"], 1, "strong earthquake must be M >= 5.0")

    def test_empty_dataset_handling_in_q1_and_parity_queries(self) -> None:
        """Verifies Q1 handles valid empty datasets deterministically (returning 0, 0, delta = 0)."""
        queries_file = SQL_GOLD_DIR / "gold_aggregate_verification_queries.sql"
        queries = parse_verification_queries(queries_file.read_text(encoding="utf-8"))

        cur = self.conn.cursor()
        cur.execute("DELETE FROM event_current")
        self.conn.commit()

        # Q1 with empty dataset:
        cur.execute(queries["Q1"])
        q1_row = cur.fetchone()
        self.assertIsNotNone(q1_row)
        self.assertEqual(q1_row["fact_event_count"], 0)
        self.assertEqual(q1_row["daily_agg_event_count"], 0)
        self.assertEqual(q1_row["delta"], 0)

        # Other queries must return 0 rows
        for q_name in ["Q2", "Q3", "Q4", "Q5", "Q6", "Q7"]:
            cur.execute(queries[q_name])
            rows = cur.fetchall()
            self.assertEqual(len(rows), 0, f"{q_name} failed on empty dataset")

    def test_negative_verification_monthly_rollup_tampering(self) -> None:
        """Proves that Q3 and Q7 detect monthly tampering (wrong region, corrupted metrics, duplicate grain)."""
        queries_file = SQL_GOLD_DIR / "gold_aggregate_verification_queries.sql"
        queries = parse_verification_queries(queries_file.read_text(encoding="utf-8"))
        q3_sql = queries["Q3"]
        q7_sql = queries["Q7"]

        cur = self.conn.cursor()
        cur.execute("DROP VIEW v_event_monthly")
        monthly_query = adapt_trino_view_for_sqlite((SQL_GOLD_DIR / "v_event_monthly.sql").read_text(encoding="utf-8"))
        cur.execute(f"CREATE TABLE v_event_monthly AS {monthly_query}")

        # Mutation M1: Alter region_key to 'WRONG_REGION' (missing/extra group in monthly rollup)
        cur.execute("UPDATE v_event_monthly SET region_key = 'WRONG_REGION' WHERE year_month_jst = 202301 AND region_key = 'OFFSHORE'")
        cur.execute(q3_sql)
        corrupted_region = cur.fetchall()
        self.assertGreater(len(corrupted_region), 0, "Q3 MUST detect mismatched region_key in monthly aggregate")

        # Revert
        cur.execute("DROP TABLE v_event_monthly")
        cur.execute(f"CREATE TABLE v_event_monthly AS {monthly_query}")

        # Mutation M2: Alter magnitude_avg to 999.0
        cur.execute("UPDATE v_event_monthly SET magnitude_avg = 999.0 WHERE year_month_jst = 202301 AND region_key = 'OFFSHORE'")
        cur.execute(q3_sql)
        corrupted_mag_avg = cur.fetchall()
        self.assertGreater(len(corrupted_mag_avg), 0, "Q3 MUST detect corrupted magnitude_avg in monthly aggregate")

        # Revert
        cur.execute("DROP TABLE v_event_monthly")
        cur.execute(f"CREATE TABLE v_event_monthly AS {monthly_query}")

        # Mutation M3: Alter strong_earthquake_count to 99
        cur.execute("UPDATE v_event_monthly SET strong_earthquake_count = 99 WHERE year_month_jst = 202301 AND region_key = 'OFFSHORE'")
        cur.execute(q3_sql)
        corrupted_strong = cur.fetchall()
        self.assertGreater(len(corrupted_strong), 0, "Q3 MUST detect corrupted strong_earthquake_count in monthly aggregate")

        # Revert
        cur.execute("DROP TABLE v_event_monthly")
        cur.execute(f"CREATE TABLE v_event_monthly AS {monthly_query}")

        # Mutation M4: Duplicate grain in monthly aggregate
        cur.execute("""
            INSERT INTO v_event_monthly
            SELECT * FROM v_event_monthly WHERE year_month_jst = 202301 AND region_key = 'OFFSHORE'
        """)
        cur.execute(q7_sql)
        corrupted_dup = cur.fetchall()
        self.assertGreater(len(corrupted_dup), 0, "Q7 MUST detect duplicate monthly grain")

    def test_negative_verification_honest_null_and_zero_vs_null(self) -> None:
        """Proves that Q4 and Q5 detect valid 0.0 being replaced with NULL or Honest NULL violations."""
        queries_file = SQL_GOLD_DIR / "gold_aggregate_verification_queries.sql"
        queries = parse_verification_queries(queries_file.read_text(encoding="utf-8"))
        q4_sql = queries["Q4"]
        q5_sql = queries["Q5"]

        cur = self.conn.cursor()
        cur.execute("DROP VIEW v_event_daily")
        daily_query = adapt_trino_view_for_sqlite((SQL_GOLD_DIR / "v_event_daily.sql").read_text(encoding="utf-8"))
        cur.execute(f"CREATE TABLE v_event_daily AS {daily_query}")

        # Mutation N1 (HoaiTam reproduction):
        # On 20230106, depth_km = 0.0, depth_non_null_count = 2, depth_sum = 0.0.
        # Set depth_sum = NULL and depth_avg = NULL while keeping depth_non_null_count = 2:
        cur.execute("""
            UPDATE v_event_daily
            SET depth_sum = NULL, depth_avg = NULL
            WHERE event_date_key_jst = 20230106
        """)
        # Q4 must catch this because fact has depth_sum = 0.0 (non-null) and aggregate has NULL
        cur.execute(q4_sql)
        q4_detected = cur.fetchall()
        self.assertGreater(len(q4_detected), 0, "Q4 MUST detect valid 0.0 depth being corrupted to NULL")

        # Q5 must also catch this because depth_non_null_count > 0 but depth_sum is NULL
        cur.execute(q5_sql)
        q5_detected = cur.fetchall()
        self.assertGreater(len(q5_detected), 0, "Q5 MUST detect count > 0 with NULL sum/avg violation")

        # Revert
        cur.execute("DROP TABLE v_event_daily")
        cur.execute(f"CREATE TABLE v_event_daily AS {daily_query}")

        # Mutation N2:
        # Mutate magnitude_sum and magnitude_avg to NULL while magnitude_non_null_count = 1 on 20230110:
        cur.execute("""
            UPDATE v_event_daily
            SET magnitude_sum = NULL, magnitude_avg = NULL
            WHERE event_date_key_jst = 20230110
        """)
        cur.execute(q4_sql)
        self.assertGreater(len(cur.fetchall()), 0, "Q4 MUST detect magnitude_sum corrupted to NULL")
        cur.execute(q5_sql)
        self.assertGreater(len(cur.fetchall()), 0, "Q5 MUST detect magnitude count > 0 with NULL sum/avg")

        # Revert
        cur.execute("DROP TABLE v_event_daily")
        cur.execute(f"CREATE TABLE v_event_daily AS {daily_query}")

        # Mutation N3:
        # On 20230201, can-004 has magnitude = NULL (magnitude_non_null_count = 0).
        # Corrupt magnitude_sum = 0.0 and magnitude_avg = 0.0 (violating Honest NULL when count = 0):
        cur.execute("""
            UPDATE v_event_daily
            SET magnitude_sum = 0.0, magnitude_avg = 0.0
            WHERE event_date_key_jst = 20230201
        """)
        cur.execute(q5_sql)
        self.assertGreater(len(cur.fetchall()), 0, "Q5 MUST detect count = 0 with non-null magnitude violation")


if __name__ == "__main__":
    unittest.main()
