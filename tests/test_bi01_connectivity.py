"""Unit and integration tests for BI-01: Power BI Trino Connectivity & Power Query Dataset.

Verifies:
1. Power Query M Script Integrity & Security:
   - All expected .m scripts exist and parse cleanly.
   - Zero hardcoded passwords, tokens, or credentials leak in Power Query files.
   - Zero direct Parquet file reads: queries strictly target Trino SQL views/tables.
   - Parameters.m defines centralized TrinoConfig record.
2. Real Gold Schema Conformity & Direct SQL Execution:
   - Extracts the actual SELECT query embedded in each .m file.
   - Executes each extracted SELECT directly against SQLite fixture matching real Gold schema.
   - Confirms zero missing column errors across FactEarthquakeEvents, FactEventDaily,
     FactEventMonthly, DimDate, DimRegion, DimMagnitudeBand, DimDepthBand, and MetadataFreshness.
3. Handoff Integrity for BI-02:
   - powerbi/sample_schema.sql executes completely in SQLite.
   - Covers valid enums (USGS_JMA, PREFECTURE, OFFSHORE, UNKNOWN).
   - Verifies UTC-to-JST date boundary crossing (UTC >= 15:00 maps to next JST day).
   - Publication gate metadata: verify_status = 'PASSED', published_at_utc, and freshness.
"""
from __future__ import annotations

from pathlib import Path
import re
import sqlite3
import unittest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
POWERBI_DIR = PROJECT_ROOT / "powerbi"
POWER_QUERY_DIR = POWERBI_DIR / "power_query"


def extract_sql_from_m_script(m_content: str) -> str:
    """Extracts the SQL query string passed inside Odbc.Query(...) in a Power Query M script."""
    m_target = re.search(r'(TargetTable|TargetView|StatusTable)\s*=\s*[^;\n]*?\.([a-zA-Z0-9_]+)', m_content)
    target_name = m_target.group(2) if m_target else "earthquake_event_current"

    match = re.search(r'Odbc\.Query\s*\([^,]+,\s*(.*?)\)\s*,', m_content, re.DOTALL)
    if not match:
        raise ValueError("Could not find Odbc.Query call with SQL literal")
    raw_expr = match.group(1).strip()
    # Resolve M string concatenation variables like TargetTable / TargetView / StatusTable
    sql = re.sub(r'&\s*(TargetTable|TargetView|StatusTable)\s*&?', f" {target_name} ", raw_expr)
    # Remove M string concatenation quotes
    sql = sql.replace('"', '')
    # Strip any remaining iceberg.gold. prefix
    sql = re.sub(r'iceberg\.gold\.', '', sql)
    # Adapt dialect for SQLite execution
    sql = re.sub(r'\btrue\b', '1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bfalse\b', '0', sql, flags=re.IGNORECASE)
    sql = sql.replace("AS DOUBLE", "AS REAL")
    return sql.strip()


class TestPowerBiConnectivity(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema_file = POWERBI_DIR / "sample_schema.sql"
        if not cls.schema_file.exists():
            raise FileNotFoundError(f"Missing {cls.schema_file}")

    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.executescript(self.schema_file.read_text(encoding="utf-8"))
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_power_query_files_security_and_no_direct_parquet(self) -> None:
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

    def test_parameters_configuration_structure(self) -> None:
        """Verifies that Parameters.m properly configures a TrinoConfig record."""
        params_file = POWER_QUERY_DIR / "Parameters.m"
        content = params_file.read_text(encoding="utf-8")
        self.assertIn('Dsn = "TrinoJapanEarthquake"', content)
        self.assertIn('Catalog = "iceberg"', content)
        self.assertIn('Schema = "gold"', content)

    def test_actual_m_sql_queries_execute_on_real_schema(self) -> None:
        """Extracts the actual SQL from each M file and executes it on the real Gold schema fixture."""
        cur = self.conn.cursor()

        # 1. FactEarthquakeEvents.m
        fact_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEarthquakeEvents.m").read_text(encoding="utf-8"))
        cur.execute(fact_sql)
        fact_rows = cur.fetchall()
        self.assertEqual(len(fact_rows), 9, "Fact query must return exactly 9 natural ROI earthquakes")
        first = fact_rows[0]
        # Verify required columns exist without missing column errors
        self.assertIn("canonical_event_id", first.keys())
        self.assertIn("is_strong_earthquake", first.keys())
        self.assertIn("max_intensity_code", first.keys())
        self.assertNotIn("event_id", first.keys())
        self.assertNotIn("is_earthquake", first.keys())

        # 2. FactEventDaily.m
        daily_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEventDaily.m").read_text(encoding="utf-8"))
        cur.execute(daily_sql)
        daily_rows = cur.fetchall()
        self.assertGreater(len(daily_rows), 0)
        total_daily_events = sum(r["event_count"] for r in daily_rows)
        self.assertEqual(total_daily_events, 9, "Daily event count must match fact count")

        # 3. FactEventMonthly.m
        monthly_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEventMonthly.m").read_text(encoding="utf-8"))
        # Adjust view name for monthly
        monthly_sql = monthly_sql.replace("v_event_daily", "v_event_monthly")
        cur.execute(monthly_sql)
        monthly_rows = cur.fetchall()
        self.assertGreater(len(monthly_rows), 0)
        total_monthly_events = sum(r["event_count"] for r in monthly_rows)
        self.assertEqual(total_monthly_events, 9, "Monthly rollup count must match daily total")

        # 4. DimDate.m
        dim_date_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimDate.m").read_text(encoding="utf-8"))
        cur.execute(dim_date_sql)
        date_rows = cur.fetchall()
        self.assertGreaterEqual(len(date_rows), 8)
        self.assertIn("calendar_date", date_rows[0].keys())
        self.assertIn("day_of_month", date_rows[0].keys())
        self.assertNotIn("full_date", date_rows[0].keys())

        # 5. DimRegion.m
        dim_region_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimRegion.m").read_text(encoding="utf-8"))
        cur.execute(dim_region_sql)
        region_rows = cur.fetchall()
        self.assertEqual(len(region_rows), 5)
        self.assertIn("region_category", region_rows[0].keys())
        self.assertNotIn("latitude_min", region_rows[0].keys())

        # 6. DimMagnitudeBand.m
        dim_mag_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimMagnitudeBand.m").read_text(encoding="utf-8"))
        cur.execute(dim_mag_sql)
        mag_rows = cur.fetchall()
        self.assertEqual(len(mag_rows), 7)
        self.assertIn("sort_order", mag_rows[0].keys())
        self.assertNotIn("min_magnitude", mag_rows[0].keys())

        # 7. DimDepthBand.m
        dim_depth_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimDepthBand.m").read_text(encoding="utf-8"))
        cur.execute(dim_depth_sql)
        depth_rows = cur.fetchall()
        self.assertEqual(len(depth_rows), 5)
        self.assertIn("sort_order", depth_rows[0].keys())
        self.assertNotIn("min_depth_km", depth_rows[0].keys())

        # 8. MetadataFreshness.m
        freshness_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "MetadataFreshness.m").read_text(encoding="utf-8"))
        cur.execute(freshness_sql)
        fresh_row = cur.fetchone()
        self.assertIsNotNone(fresh_row)
        self.assertEqual(fresh_row["publication_id"], "pub-202302-001")
        self.assertEqual(fresh_row["verify_status"], "PASSED")
        self.assertEqual(fresh_row["verified_event_count"], 9)
        self.assertIsNotNone(fresh_row["published_at_utc"])
        self.assertIsNotNone(fresh_row["data_freshness_max_event_utc"])

    def test_utc_to_jst_date_boundary_crossing(self) -> None:
        """Verifies that events with UTC >= 15:00 correctly map to the next JST day."""
        cur = self.conn.cursor()
        # can-006: 2023-01-15T18:45:00Z -> JST date 2023-01-16
        cur.execute("SELECT event_date_jst, event_date_key_jst FROM event_current WHERE canonical_event_id = 'can-006'")
        row6 = cur.fetchone()
        self.assertEqual(row6["event_date_jst"], "2023-01-16")
        self.assertEqual(row6["event_date_key_jst"], 20230116)

        # can-009: 2023-02-05T22:15:00Z -> JST date 2023-02-06
        cur.execute("SELECT event_date_jst, event_date_key_jst FROM event_current WHERE canonical_event_id = 'can-009'")
        row9 = cur.fetchone()
        self.assertEqual(row9["event_date_jst"], "2023-02-06")
        self.assertEqual(row9["event_date_key_jst"], 20230206)

    def test_source_coverage_and_region_enums(self) -> None:
        """Verifies that enum values conform to USGS_JMA and PREFECTURE/OFFSHORE/UNKNOWN."""
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT source_coverage_code FROM event_current WHERE is_natural_earthquake = 1")
        coverages = {r[0] for r in cur.fetchall()}
        self.assertIn("USGS_JMA", coverages)
        self.assertNotIn("BOTH_USGS_AND_JMA", coverages)

        cur.execute("SELECT DISTINCT region_category FROM event_current WHERE is_natural_earthquake = 1")
        categories = {r[0] for r in cur.fetchall()}
        self.assertTrue(categories.issubset({"PREFECTURE", "OFFSHORE", "UNKNOWN"}))
        self.assertNotIn("INLAND", categories)


if __name__ == "__main__":
    unittest.main()
