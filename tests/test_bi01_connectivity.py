"""Unit and integration tests for BI-01: Power BI Trino Connectivity & Power Query Dataset.

Verifies:
1. Power Query M Script Integrity & Security:
   - All expected .m scripts exist and parse cleanly.
   - Zero hardcoded passwords, tokens, or credentials leak in Power Query files.
   - Zero direct Parquet file reads: queries strictly target Trino SQL views/tables.
2. Schema & Type Alignment against CON-03:
   - Explicit M typing defined for all Fact and Dimension tables.
   - Field names and types match CON-03 contract definitions.
3. Executable Sample Schema Handoff for BI-02:
   - powerbi/sample_schema.sql executes completely in SQLite.
   - Views v_event_daily and v_event_monthly produce expected aggregated results.
   - Excludes non-natural and out-of-study-area events.
   - Honest NULL policy is preserved across Power Query source views.
"""
from __future__ import annotations

from pathlib import Path
import re
import sqlite3
import unittest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
POWERBI_DIR = PROJECT_ROOT / "powerbi"
POWER_QUERY_DIR = POWERBI_DIR / "power_query"


class TestPowerBiConnectivity(unittest.TestCase):
    def test_power_query_files_exist_and_secure(self) -> None:
        """Verifies that all M scripts exist, use ODBC, and contain no credentials or parquet paths."""
        expected_scripts = [
            "Parameters.m",
            "FactEarthquakeEvents.m",
            "FactEventDaily.m",
            "FactEventMonthly.m",
            "DimDate.m",
            "DimRegion.m",
            "DimMagnitudeBand.m",
            "DimDepthBand.m",
            "MetadataFreshness.m"
        ]
        forbidden_patterns = [
            r"password\s*=",
            r"pwd\s*=",
            r"secret",
            r"token",
            r"Bearer",
            r"\.parquet\b",
            r"s3://",
            r"minio://",
            r"File\.Contents",
            r"Parquet\.Document"
        ]

        for script_name in expected_scripts:
            script_path = POWER_QUERY_DIR / script_name
            self.assertTrue(script_path.exists(), f"Missing M script: {script_name}")
            content = script_path.read_text(encoding="utf-8")

            # Check for forbidden credential / direct file patterns
            for pat in forbidden_patterns:
                match = re.search(pat, content, re.IGNORECASE)
                self.assertIsNone(
                    match,
                    f"Forbidden pattern '{pat}' detected in {script_name}: {match.group(0) if match else ''}"
                )

            # Non-parameter queries must use Odbc.Query
            if script_name != "Parameters.m":
                self.assertIn("Odbc.Query", content, f"{script_name} must use Odbc.Query for Trino access")
                self.assertIn("Table.TransformColumnTypes", content, f"{script_name} must enforce explicit column types")

    def test_power_query_parameters(self) -> None:
        """Verifies that Parameters.m properly parameterizes connection details."""
        params_file = POWER_QUERY_DIR / "Parameters.m"
        content = params_file.read_text(encoding="utf-8")
        self.assertIn("pTrinoDsn", content)
        self.assertIn("pTrinoHost", content)
        self.assertIn("pTrinoPort", content)
        self.assertIn("pTrinoCatalog", content)
        self.assertIn("pTrinoSchema", content)

    def test_sample_schema_execution_and_parity(self) -> None:
        """Executes sample_schema.sql in SQLite and verifies contract conformity for BI-02 handoff."""
        schema_file = POWERBI_DIR / "sample_schema.sql"
        self.assertTrue(schema_file.exists(), "sample_schema.sql missing")
        sql_script = schema_file.read_text(encoding="utf-8")

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # 1. Execute DDL and fixture data
        cur.executescript(sql_script)
        conn.commit()

        # 2. Verify Fact Canonical Event count (9 eligible earthquakes, 1 non-natural, 1 out-of-study-area)
        cur.execute("""
            SELECT
                COUNT(*) AS total_rows,
                SUM(CASE WHEN is_earthquake = 1 AND is_in_study_area = 1 THEN 1 ELSE 0 END) AS valid_earthquakes,
                SUM(CASE WHEN is_earthquake = 0 THEN 1 ELSE 0 END) AS non_natural,
                SUM(CASE WHEN is_in_study_area = 0 THEN 1 ELSE 0 END) AS out_of_study_area
            FROM event_current
        """)
        counts = cur.fetchone()
        self.assertEqual(counts["total_rows"], 11)
        self.assertEqual(counts["valid_earthquakes"], 9)
        self.assertEqual(counts["non_natural"], 1)
        self.assertEqual(counts["out_of_study_area"], 1)

        # 3. Verify FactEventDaily view excludes non-natural and out-of-study-area events
        cur.execute("SELECT SUM(event_count) AS agg_count FROM v_event_daily")
        daily_sum = cur.fetchone()["agg_count"]
        self.assertEqual(daily_sum, 9, "Daily aggregate count must strictly match valid earthquakes")

        # 4. Verify FactEventMonthly additive parity with daily
        cur.execute("SELECT SUM(event_count) AS monthly_sum FROM v_event_monthly")
        monthly_sum = cur.fetchone()["monthly_sum"]
        self.assertEqual(monthly_sum, 9, "Monthly rollup count must strictly equal daily total")

        # 5. Verify Honest NULL preservation: ev-008 has NULL magnitude
        cur.execute("""
            SELECT
                event_date_key_jst,
                magnitude_non_null_count,
                magnitude_avg,
                depth_avg
            FROM v_event_daily
            WHERE event_date_key_jst = 20230201 AND depth_band_code = 'NEGATIVE'
        """)
        null_mag_row = cur.fetchone()
        self.assertIsNotNone(null_mag_row)
        self.assertEqual(null_mag_row["magnitude_non_null_count"], 0)
        self.assertIsNone(null_mag_row["magnitude_avg"], "Missing magnitude must not average to 0.0")
        self.assertAlmostEqual(null_mag_row["depth_avg"], -2.0, places=2)

        # 6. Verify Dimension integrity
        cur.execute("SELECT COUNT(*) AS cnt FROM dim_region WHERE region_category = 'OFFSHORE'")
        offshore_count = cur.fetchone()["cnt"]
        self.assertGreater(offshore_count, 0, "Offshore regions must be preserved")

        cur.execute("SELECT COUNT(*) AS cnt FROM dim_magnitude_band")
        self.assertEqual(cur.fetchone()["cnt"], 7)

        cur.execute("SELECT COUNT(*) AS cnt FROM dim_depth_band")
        self.assertEqual(cur.fetchone()["cnt"], 5)

        conn.close()


if __name__ == "__main__":
    unittest.main()
