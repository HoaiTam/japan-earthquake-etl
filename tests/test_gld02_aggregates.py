"""Unit and integration tests for GLD-02: Dashboard Aggregate Transformations and Trino Serving Views.

Verifies:
1. SQL View Syntax & Real Schema Conformity:
   - v_event_daily and v_event_monthly parse and execute cleanly in SQLite.
   - Non-natural earthquakes (is_earthquake = false) and out-of-study-area events are excluded.
2. Fact-to-Aggregate Count Parity:
   - sum(event_count) across daily aggregates equals exact count of eligible fact records.
3. Zero Double-Counting:
   - Multi-source linked events in event_source_bridge do NOT inflate event counts.
4. Additive Rollup Parity:
   - Monthly rollup matches daily totals exactly for every year_month.
5. Honest NULL Policy:
   - Missing magnitudes and depths result in NULL averages, never coerced to 0.0.
6. Verification Queries:
   - All 5 queries in sql/gold/gold_aggregate_verification_queries.sql pass with 0 discrepancies.
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
    # Replace iceberg.gold. with empty prefix so it references SQLite tables/views
    query = query.replace("iceberg.gold.", "")
    # In SQLite, double precision CAST is REAL or DOUBLE
    query = query.replace("AS DOUBLE", "AS REAL")
    return query


class TestGoldAggregates(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def tearDown(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        # Create mock event_current table
        cur.execute("""
            CREATE TABLE event_current (
                event_id TEXT PRIMARY KEY,
                event_date_key_jst INTEGER NOT NULL,
                event_date_jst TEXT NOT NULL,
                event_time_utc TEXT NOT NULL,
                region_key TEXT,
                region_name TEXT,
                region_category TEXT,
                magnitude REAL,
                depth_km REAL,
                magnitude_band_code TEXT,
                depth_band_code TEXT,
                source_coverage_code TEXT NOT NULL,
                is_earthquake INTEGER NOT NULL,
                is_in_study_area INTEGER NOT NULL,
                is_strong_earthquake INTEGER NOT NULL,
                tsunami_flag INTEGER,
                alert_level TEXT,
                jma_max_intensity TEXT
            )
        """)
        # Create mock event_source_bridge table to test zero double-counting
        cur.execute("""
            CREATE TABLE event_source_bridge (
                event_id TEXT NOT NULL,
                source_observation_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                is_primary_source INTEGER NOT NULL
            )
        """)
        self.conn.commit()

    def _populate_mock_data(self) -> None:
        cur = self.conn.cursor()
        # Event 1: Valid earthquake in study area (Jan 5, 2023)
        cur.execute("""
            INSERT INTO event_current VALUES (
                'e1', 20230105, '2023-01-05', '2023-01-04T16:00:00Z',
                'reg-kanto', 'Kanto Region', 'INLAND',
                4.2, 12.0, 'M4_TO_LT5', 'SHALLOW', 'USGS_ONLY',
                1, 1, 0, 0, NULL, NULL
            )
        """)
        # Event 2: Valid earthquake with JMA link and tsunami flag (Jan 5, 2023, same day & band)
        cur.execute("""
            INSERT INTO event_current VALUES (
                'e2', 20230105, '2023-01-05', '2023-01-05T08:00:00Z',
                'reg-kanto', 'Kanto Region', 'INLAND',
                4.8, 26.0, 'M4_TO_LT5', 'SHALLOW', 'BOTH_USGS_AND_JMA',
                1, 1, 0, 1, 'YELLOW', '3'
            )
        """)
        # Event 3: Valid strong earthquake (Jan 10, 2023)
        cur.execute("""
            INSERT INTO event_current VALUES (
                'e3', 20230110, '2023-01-10', '2023-01-10T02:00:00Z',
                'reg-tohoku', 'Tohoku Region', 'OFFSHORE',
                6.5, 45.0, 'M6_TO_LT7', 'SHALLOW', 'BOTH_USGS_AND_JMA',
                1, 1, 1, 0, 'RED', '5-'
            )
        """)
        # Event 4: Valid earthquake with NULL magnitude and negative depth (Feb 1, 2023)
        cur.execute("""
            INSERT INTO event_current VALUES (
                'e4', 20230201, '2023-02-01', '2023-02-01T03:00:00Z',
                'UNKNOWN', 'Unknown', 'UNKNOWN',
                NULL, -1.5, 'UNKNOWN', 'NEGATIVE', 'USGS_ONLY',
                1, 1, 0, 0, NULL, NULL
            )
        """)
        # Event 5: Quarry blast (non-natural: is_earthquake = false) - MUST BE EXCLUDED
        cur.execute("""
            INSERT INTO event_current VALUES (
                'e5', 20230105, '2023-01-05', '2023-01-05T04:00:00Z',
                'reg-kanto', 'Kanto Region', 'INLAND',
                2.1, 0.0, 'LT_3', 'SHALLOW', 'USGS_ONLY',
                0, 1, 0, 0, NULL, NULL
            )
        """)
        # Event 6: Distant earthquake outside study area - MUST BE EXCLUDED
        cur.execute("""
            INSERT INTO event_current VALUES (
                'e6', 20230105, '2023-01-05', '2023-01-05T05:00:00Z',
                'UNKNOWN', 'Unknown', 'UNKNOWN',
                7.2, 10.0, 'GE_7', 'SHALLOW', 'USGS_ONLY',
                1, 0, 1, 1, 'RED', NULL
            )
        """)

        # Populate bridge: Event e2 has TWO observations in bridge (USGS and JMA)
        cur.execute("INSERT INTO event_source_bridge VALUES ('e1', 'usgs-1', 'USGS', 1)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('e2', 'usgs-2', 'USGS', 1)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('e2', 'jma-2', 'JMA', 0)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('e3', 'usgs-3', 'USGS', 1)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('e3', 'jma-3', 'JMA', 0)")
        cur.execute("INSERT INTO event_source_bridge VALUES ('e4', 'usgs-4', 'USGS', 1)")

        self.conn.commit()

    def test_sql_views_creation_and_parity(self) -> None:
        self._populate_mock_data()
        cur = self.conn.cursor()

        # 1. Create v_event_daily
        daily_sql_file = SQL_GOLD_DIR / "v_event_daily.sql"
        self.assertTrue(daily_sql_file.exists(), "v_event_daily.sql missing")
        daily_query = adapt_trino_view_for_sqlite(daily_sql_file.read_text(encoding="utf-8"))
        cur.execute(f"CREATE VIEW v_event_daily AS {daily_query}")

        # 2. Create v_event_monthly
        monthly_sql_file = SQL_GOLD_DIR / "v_event_monthly.sql"
        self.assertTrue(monthly_sql_file.exists(), "v_event_monthly.sql missing")
        monthly_query = adapt_trino_view_for_sqlite(monthly_sql_file.read_text(encoding="utf-8"))
        cur.execute(f"CREATE VIEW v_event_monthly AS {monthly_query}")

        # 3. Check Q1: Fact-to-Aggregate Count Reconciliation
        # Out of 6 events: e5 is non-earthquake, e6 is out-of-study-area -> 4 eligible events
        cur.execute("""
            SELECT
                f.fact_event_count,
                a.daily_agg_event_count,
                (f.fact_event_count - a.daily_agg_event_count) AS delta
            FROM (
                SELECT COUNT(*) AS fact_event_count
                FROM event_current
                WHERE is_earthquake = 1 AND is_in_study_area = 1
            ) f
            CROSS JOIN (
                SELECT SUM(event_count) AS daily_agg_event_count
                FROM v_event_daily
            ) a
        """)
        row = cur.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["fact_event_count"], 4)
        self.assertEqual(row["daily_agg_event_count"], 4)
        self.assertEqual(row["delta"], 0)

        # 4. Check Q2: Zero Double-Counting Audit
        cur.execute("""
            SELECT
                e.event_date_key_jst,
                e.distinct_event_count,
                d.daily_agg_event_count,
                (e.distinct_event_count - d.daily_agg_event_count) AS delta
            FROM (
                SELECT event_date_key_jst, COUNT(DISTINCT event_id) AS distinct_event_count
                FROM event_current
                WHERE is_earthquake = 1 AND is_in_study_area = 1
                GROUP BY event_date_key_jst
            ) e
            JOIN (
                SELECT event_date_key_jst, SUM(event_count) AS daily_agg_event_count
                FROM v_event_daily
                GROUP BY event_date_key_jst
            ) d
              ON e.event_date_key_jst = d.event_date_key_jst
            WHERE e.distinct_event_count <> d.daily_agg_event_count
        """)
        mismatches = cur.fetchall()
        self.assertEqual(len(mismatches), 0, "Double-counting detected in daily aggregates")

        # 5. Check Q3: Additive Consistency between Daily and Monthly
        cur.execute("""
            SELECT
                d.year_month_jst,
                d.daily_total_events,
                m.monthly_total_events,
                (d.daily_total_events - m.monthly_total_events) AS delta
            FROM (
                SELECT year_month_jst, SUM(event_count) AS daily_total_events
                FROM v_event_daily
                GROUP BY year_month_jst
            ) d
            JOIN (
                SELECT year_month_jst, SUM(event_count) AS monthly_total_events
                FROM v_event_monthly
                GROUP BY year_month_jst
            ) m
              ON d.year_month_jst = m.year_month_jst
            WHERE d.daily_total_events <> m.monthly_total_events
        """)
        rollup_diffs = cur.fetchall()
        self.assertEqual(len(rollup_diffs), 0, "Monthly rollup count mismatch")

        # 6. Check Q4: Honest NULL policy
        # Event e4 has NULL magnitude: verify magnitude_avg is NULL for that row
        cur.execute("""
            SELECT
                event_date_key_jst,
                magnitude_non_null_count,
                magnitude_avg,
                magnitude_max,
                depth_avg
            FROM v_event_daily
            WHERE event_date_key_jst = 20230201
        """)
        feb_row = cur.fetchone()
        self.assertIsNotNone(feb_row)
        self.assertEqual(feb_row["magnitude_non_null_count"], 0)
        self.assertIsNone(feb_row["magnitude_avg"], "Missing magnitude must not average to 0.0")
        self.assertIsNone(feb_row["magnitude_max"])
        self.assertAlmostEqual(feb_row["depth_avg"], -1.5, places=2)

        # 7. Check Q5: Granularity Uniqueness
        cur.execute("""
            SELECT
                event_date_key_jst,
                region_key,
                magnitude_band_code,
                depth_band_code,
                source_coverage_code,
                COUNT(*) AS cnt
            FROM v_event_daily
            GROUP BY
                event_date_key_jst,
                region_key,
                magnitude_band_code,
                depth_band_code,
                source_coverage_code
            HAVING COUNT(*) > 1
        """)
        duplicate_grains = cur.fetchall()
        self.assertEqual(len(duplicate_grains), 0, "Duplicate grain rows found in v_event_daily")


if __name__ == "__main__":
    unittest.main()
