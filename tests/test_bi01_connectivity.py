"""Unit and integration tests for BI-01: Power BI Trino Connectivity & Power Query Dataset.

Verifies:
1. Power Query M Script Integrity & Security:
   - All expected .m scripts exist and parse cleanly (including ActiveBundle.m).
   - Zero hardcoded passwords, tokens, or credentials leak in Power Query files.
   - Zero direct Parquet file reads: queries strictly target Trino SQL views/tables.
   - Parameters.m defines centralized TrinoConfig record and PinnedPublicationId support.
2. Real Gold Schema Conformity & Direct SQL Execution:
   - Extracts the actual SELECT query embedded in each .m file.
   - Executes each extracted SELECT directly against SQLite fixture matching real Gold schema.
   - Confirms zero missing column errors across FactEarthquakeEvents, FactEventDaily,
     FactEventMonthly, DimDate, DimRegion, DimMagnitudeBand, DimDepthBand, MetadataFreshness,
     and ActiveBundle.
3. Regression & Lakehouse Snapshot Invariants:
   - Commit-before-publish immunity: unverified commits in physical tables are never exposed.
   - Publication-before-alias-switch split-brain immunity: all queries pin to the same PASSED bundle.
   - Refresh across alias switch consistency: mid-refresh alias switches cannot corrupt active session.
4. Handoff Integrity for BI-02:
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


def extract_sql_from_m_script(
    m_content: str,
    bundle_suffix: str = "bndl20230201sha256test01",
    publication_id: str = "pub-202302-001"
) -> str:
    """Extracts and normalizes the SQL query from a Power Query M script for SQLite execution."""
    m_target = re.search(r'TargetView\s*=\s*"v_"\s*&\s*BundleSuffix\s*&\s*"_([a-zA-Z0-9_]+)"', m_content)
    if m_target:
        target_name = f"v_{bundle_suffix}_{m_target.group(1)}"
    else:
        m_old = re.search(r'(TargetTable|TargetView|StatusTable)\s*=\s*[^;\n]*?\.([a-zA-Z0-9_]+)', m_content)
        target_name = m_old.group(2) if m_old else f"v_{bundle_suffix}_earthquake_event_current"

    if "QuerySql" in m_content:
        # Check if conditional expression in ActiveBundle.m
        m_else = re.search(r'else\s*("SELECT[\s\S]*?),\s*Source', m_content)
        if m_else:
            raw_expr = m_else.group(1)
        else:
            m_q = re.search(r'QuerySql\s*=\s*("SELECT[\s\S]*?),\s*(?:Source|\w+\s*=)', m_content)
            if m_q:
                raw_expr = m_q.group(1)
            else:
                match = re.search(r'Odbc\.Query\s*\([^,]+,\s*(.*?)\)\s*,', m_content, re.DOTALL)
                raw_expr = match.group(1).strip() if match else ""
    else:
        match = re.search(r'Odbc\.Query\s*\([^,]+,\s*(.*?)\)\s*,', m_content, re.DOTALL)
        if not match:
            raise ValueError("Could not find Odbc.Query call with SQL literal")
        raw_expr = match.group(1).strip()

    sql = raw_expr
    # Replace TargetView
    sql = re.sub(r'&\s*TargetView\b', f" {target_name} ", sql)
    sql = re.sub(r'\bTargetView\s*&', f" {target_name} ", sql)
    # Strip Catalog & Schema prefixes
    sql = re.sub(r'(?:CatalogName|Parameters\[Catalog\])\s*&\s*"\."\s*&\s*(?:SchemaName|Parameters\[Schema\])\s*&\s*"\."\s*&?', " ", sql)
    sql = re.sub(r'(?:CatalogName|Parameters\[Catalog\])\s*&\s*"\."\s*&\s*(?:SchemaName|Parameters\[Schema\])\s*&\s*"\."?', " ", sql)
    # Handle publication ID string literal concatenation cleanly without spaces
    sql = re.sub(r'\'"\s*&\s*Text\.Replace\s*\(\s*(?:PubId|PinnedId)[^)]+\)\s*&\s*"\'', f"'{publication_id}'", sql)
    sql = re.sub(r'Text\.Replace\s*\(\s*(?:PubId|PinnedId)[^)]+\)', f"'{publication_id}'", sql)
    # Remove quotes and M concatenation operators
    sql = sql.replace('"', '')
    sql = sql.replace('&', '')
    # Dialect translation for SQLite execution
    sql = re.sub(r'iceberg\.gold\.', '', sql)
    sql = re.sub(r'\btrue\b', '1', sql, flags=re.IGNORECASE)
    sql = re.sub(r'\bfalse\b', '0', sql, flags=re.IGNORECASE)
    sql = sql.replace("AS DOUBLE", "AS REAL")
    sql = sql.replace("AS VARCHAR", "AS TEXT")
    sql = re.sub(r'[ \t]+', ' ', sql)
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
            "DiscoveryLatestPublication.m",
            "ActiveBundle.m",
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
            if script_name not in ("Parameters.m", "ActiveBundle.m"):
                self.assertIn("Table.TransformColumnTypes", content, f"{script_name} must enforce explicit column types")

    def test_parameters_configuration_structure(self) -> None:
        """Verifies that Parameters.m properly configures a TrinoConfig record with mandatory PinnedPublicationId."""
        params_file = POWER_QUERY_DIR / "Parameters.m"
        content = params_file.read_text(encoding="utf-8")
        self.assertIn('Dsn = "TrinoJapanEarthquake"', content)
        self.assertIn('Catalog = "iceberg"', content)
        self.assertIn('Schema = "gold"', content)
        self.assertIn('PinnedPublicationId = "pub-202302-001"', content)

    def test_actual_m_sql_queries_execute_on_real_schema(self) -> None:
        """Extracts the actual SQL from each M file and executes it on the real Gold schema fixture."""
        cur = self.conn.cursor()

        # 1. DiscoveryLatestPublication.m
        discovery_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DiscoveryLatestPublication.m").read_text(encoding="utf-8"))
        cur.execute(discovery_sql)
        discovery_row = cur.fetchone()
        self.assertIsNotNone(discovery_row)
        self.assertEqual(discovery_row["publication_id"], "pub-202302-001")
        self.assertEqual(discovery_row["bundle_suffix"], "bndl20230201sha256test01"[:24])

        # 2. ActiveBundle.m (pinned to pub-202302-001)
        bundle_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "ActiveBundle.m").read_text(encoding="utf-8"))
        cur.execute(bundle_sql)
        bundle_row = cur.fetchone()
        self.assertIsNotNone(bundle_row)
        self.assertEqual(bundle_row["publication_id"], "pub-202302-001")
        self.assertEqual(bundle_row["bundle_sha256"], "bndl20230201sha256test01")
        self.assertEqual(bundle_row["bundle_suffix"], "bndl20230201sha256test01"[:24])

        # 3. FactEarthquakeEvents.m
        fact_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEarthquakeEvents.m").read_text(encoding="utf-8"))
        cur.execute(fact_sql)
        fact_rows = cur.fetchall()
        self.assertEqual(len(fact_rows), 9, "Fact query must return exactly 9 natural ROI earthquakes")
        first = fact_rows[0]
        # Verify required columns exist without missing column errors
        self.assertIn("canonical_event_id", first.keys())
        self.assertIn("source_coverage_code", first.keys())
        self.assertIn("max_intensity_code", first.keys())
        self.assertNotIn("event_id", first.keys())
        self.assertNotIn("is_earthquake", first.keys())

        # 4. FactEventDaily.m
        daily_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEventDaily.m").read_text(encoding="utf-8"))
        cur.execute(daily_sql)
        daily_rows = cur.fetchall()
        self.assertGreater(len(daily_rows), 0)
        total_daily_events = sum(r["event_count"] for r in daily_rows)
        self.assertEqual(total_daily_events, 9, "Daily event count must match fact count")

        # 5. FactEventMonthly.m
        monthly_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEventMonthly.m").read_text(encoding="utf-8"))
        cur.execute(monthly_sql)
        monthly_rows = cur.fetchall()
        self.assertGreater(len(monthly_rows), 0)
        total_monthly_events = sum(r["event_count"] for r in monthly_rows)
        self.assertEqual(total_monthly_events, 9, "Monthly rollup count must match daily total")

        # 6. DimDate.m
        dim_date_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimDate.m").read_text(encoding="utf-8"))
        cur.execute(dim_date_sql)
        date_rows = cur.fetchall()
        self.assertGreaterEqual(len(date_rows), 8)
        self.assertIn("calendar_date", date_rows[0].keys())
        self.assertIn("day_of_month", date_rows[0].keys())
        self.assertNotIn("full_date", date_rows[0].keys())

        # 7. DimRegion.m
        dim_region_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimRegion.m").read_text(encoding="utf-8"))
        cur.execute(dim_region_sql)
        region_rows = cur.fetchall()
        self.assertEqual(len(region_rows), 5)
        self.assertIn("region_category", region_rows[0].keys())
        self.assertNotIn("latitude_min", region_rows[0].keys())

        # 8. DimMagnitudeBand.m
        dim_mag_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimMagnitudeBand.m").read_text(encoding="utf-8"))
        cur.execute(dim_mag_sql)
        mag_rows = cur.fetchall()
        self.assertEqual(len(mag_rows), 7)
        self.assertIn("sort_order", mag_rows[0].keys())
        self.assertNotIn("min_magnitude", mag_rows[0].keys())

        # 9. DimDepthBand.m
        dim_depth_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimDepthBand.m").read_text(encoding="utf-8"))
        cur.execute(dim_depth_sql)
        depth_rows = cur.fetchall()
        self.assertEqual(len(depth_rows), 5)
        self.assertIn("sort_order", depth_rows[0].keys())
        self.assertNotIn("min_depth_km", depth_rows[0].keys())

        # 10. MetadataFreshness.m
        freshness_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "MetadataFreshness.m").read_text(encoding="utf-8"))
        cur.execute(freshness_sql)
        fresh_row = cur.fetchone()
        self.assertIsNotNone(fresh_row)
        self.assertEqual(fresh_row["publication_id"], "pub-202302-001")
        self.assertEqual(fresh_row["published_event_count"], 9)
        self.assertIsNotNone(fresh_row["published_at_utc"])
        self.assertIsNotNone(fresh_row["data_freshness_max_event_utc"])

    def test_active_bundle_fail_closed_validation(self) -> None:
        """Verifies that ActiveBundle enforces fail-closed validation when PinnedPublicationId is empty or unverified."""
        active_bundle_m = (POWER_QUERY_DIR / "ActiveBundle.m").read_text(encoding="utf-8")
        # 1. M script must enforce non-empty PinnedPublicationId check and raise error
        self.assertIn('RawPinnedId = null or Text.Trim(RawPinnedId) = ""', active_bundle_m)
        self.assertIn('error "PinnedPublicationId parameter is mandatory', active_bundle_m)
        # 2. Must NOT contain auto-resolve LIMIT 1 fallback
        self.assertNotIn('ORDER BY published_at_utc DESC LIMIT 1', active_bundle_m)

        # 3. SQL query for non-existent publication ID must yield zero rows
        cur = self.conn.cursor()
        invalid_sql = extract_sql_from_m_script(active_bundle_m, publication_id="pub-nonexistent")
        cur.execute(invalid_sql)
        self.assertIsNone(cur.fetchone(), "Non-existent publication ID must yield zero rows")

        # 4. Unverified publication (verify_status != 'PASSED') must also yield zero rows
        cur.execute("""
            INSERT INTO publication_status VALUES (
                'pub-failed-001', 'run-failed', 111, 'tbl', 'p_tbl',
                '2023-01-01', '2023-01-01', '2023-01-01', '2023-01-01',
                0, '1.0', 'FAILED', 'bndl_failed_sha256', '{}', '{}', 'v_failed'
            )
        """)
        self.conn.commit()
        failed_sql = extract_sql_from_m_script(active_bundle_m, publication_id="pub-failed-001")
        cur.execute(failed_sql)
        self.assertIsNone(cur.fetchone(), "Unverified publication must yield zero rows")

    def test_regression_commit_before_publish(self) -> None:
        """Regression test: Unverified commits in physical tables must never be exposed to pinned queries."""
        cur = self.conn.cursor()
        # 1. Simulate an unverified commit directly inserting into physical event_current and dim_region
        cur.execute("""
            INSERT INTO event_current (
                schema_version, canonical_event_id, primary_observation_id, canonical_source_system,
                event_time_utc, event_time_jst, event_date_utc, event_date_jst,
                event_date_key_utc, event_date_key_jst, latitude, longitude, depth_km, magnitude,
                magnitude_type, event_type_code, is_natural_earthquake, place_name,
                is_in_study_area, region_key, region_name, region_category, magnitude_band_code,
                depth_band_code, tsunami_flag, alert_level, significance, max_intensity_code,
                determining_agency_code, catalog_era, source_count, has_usgs, has_jma,
                source_coverage_code, link_status, quality_status, canonical_model_version,
                gold_run_id, record_updated_at_utc
            ) VALUES (
                '1.0', 'can-unverified-999', 'obs-999', 'USGS',
                '2023-02-07T12:00:00Z', '2023-02-07 21:00:00', '2023-02-07', '2023-02-07',
                20230207, 20230207, 36.0, 140.0, 30.0, 5.0,
                'mww', 'EARTHQUAKE', 1, 'Unverified commit',
                1, 'PREFECTURE:99', 'Unverified Province', 'PREFECTURE', 'M5_TO_LT6',
                'SHALLOW', 0, NULL, 300, '2',
                'J', 'UNIFIED', 1, 1, 0,
                'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1',
                'gold-run-unverified', '2023-02-07T12:05:00Z'
            )
        """)
        cur.execute("INSERT INTO dim_region VALUES ('PREFECTURE:99', 'Unverified Province', 'PREFECTURE')")
        self.conn.commit()

        # Physical table has 10 natural earthquakes and 6 regions now
        cur.execute("SELECT COUNT(*) FROM event_current WHERE is_natural_earthquake = 1 AND is_in_study_area = 1")
        self.assertEqual(cur.fetchone()[0], 10)
        cur.execute("SELECT COUNT(*) FROM dim_region")
        self.assertEqual(cur.fetchone()[0], 6)

        # 2. But ActiveBundle is pinned to pub-202302-001 (bndl20230201sha256test01)
        fact_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEarthquakeEvents.m").read_text(encoding="utf-8"))
        cur.execute(fact_sql)
        fact_rows = cur.fetchall()
        self.assertEqual(len(fact_rows), 9, "Pinned Fact query must ignore unverified physical commits")

        daily_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "FactEventDaily.m").read_text(encoding="utf-8"))
        cur.execute(daily_sql)
        total_daily_events = sum(r["event_count"] for r in cur.fetchall())
        self.assertEqual(total_daily_events, 9, "Pinned Daily aggregate must ignore unverified physical commits")

        dim_region_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DimRegion.m").read_text(encoding="utf-8"))
        cur.execute(dim_region_sql)
        region_rows = cur.fetchall()
        self.assertEqual(len(region_rows), 5, "Pinned DimRegion must ignore unverified physical commits")

        freshness_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "MetadataFreshness.m").read_text(encoding="utf-8"))
        cur.execute(freshness_sql)
        fresh_row = cur.fetchone()
        self.assertEqual(fresh_row["published_event_count"], 9)

    def test_regression_publication_before_alias_switch(self) -> None:
        """Regression test: A new PASSED publication before alias switch does not cause split-brain across queries."""
        cur = self.conn.cursor()
        # 1. Add 10th event to physical table
        cur.execute("""
            INSERT INTO event_current (
                schema_version, canonical_event_id, primary_observation_id, canonical_source_system,
                event_time_utc, event_time_jst, event_date_utc, event_date_jst,
                event_date_key_utc, event_date_key_jst, latitude, longitude, depth_km, magnitude,
                magnitude_type, event_type_code, is_natural_earthquake, place_name,
                is_in_study_area, region_key, region_name, region_category, magnitude_band_code,
                depth_band_code, tsunami_flag, alert_level, significance, max_intensity_code,
                determining_agency_code, catalog_era, source_count, has_usgs, has_jma,
                source_coverage_code, link_status, quality_status, canonical_model_version,
                gold_run_id, record_updated_at_utc
            ) VALUES (
                '1.0', 'can-010', 'obs-010', 'USGS',
                '2023-02-08T00:00:00Z', '2023-02-08 09:00:00', '2023-02-08', '2023-02-08',
                20230208, 20230208, 36.5, 140.5, 30.0, 5.0,
                'mww', 'EARTHQUAKE', 1, 'Ibaraki',
                1, 'PREFECTURE:13', 'Tokyo', 'PREFECTURE', 'M5_TO_LT6',
                'SHALLOW', 0, NULL, 300, '2',
                'J', 'UNIFIED', 1, 1, 0,
                'USGS_ONLY', 'SINGLE_SOURCE', 'VALID', 'v1',
                'gold-run-002', '2023-02-08T00:05:00Z'
            )
        """)
        # 2. New publication passes verification and is recorded in publication_status
        new_suffix = "bndl20230208sha256test02"
        cur.execute(f"""
            INSERT INTO publication_status VALUES (
                'pub-202302-002', 'gold-run-002', 98765432109876,
                'iceberg.gold.earthquake_event_current', 'iceberg.gold.event_current',
                '2023-02-08T01:00:00Z', '2023-02-08T01:05:00Z', '2023-02-08T01:10:00Z',
                '2023-02-08T00:00:00Z', 10, '1.0', 'PASSED', '{new_suffix}',
                '{{}}', '{{}}', 'iceberg.gold.v_{new_suffix}_earthquake_event_current'
            )
        """)
        # 3. Create the immutable versioned view for the new bundle
        cur.execute(f"""
            CREATE VIEW v_{new_suffix}_earthquake_event_current AS
            SELECT * FROM event_current WHERE is_natural_earthquake = 1 AND is_in_study_area = 1
        """)
        cur.execute(f"CREATE VIEW v_{new_suffix}_dim_date AS SELECT * FROM dim_date")
        cur.execute(f"CREATE VIEW v_{new_suffix}_dim_region AS SELECT * FROM dim_region")
        cur.execute(f"CREATE VIEW v_{new_suffix}_dim_magnitude_band AS SELECT * FROM dim_magnitude_band")
        cur.execute(f"CREATE VIEW v_{new_suffix}_dim_depth_band AS SELECT * FROM dim_depth_band")
        self.conn.commit()

        # NOTE: Notice that the alias `earthquake_event_current` has NOT yet been switched!
        # It still points to v_bndl20230201sha256test01_earthquake_event_current (9 rows)
        cur.execute("SELECT COUNT(*) FROM earthquake_event_current")
        self.assertEqual(cur.fetchone()[0], 9)

        # 4. Now simulate 2-step Power BI workflow:
        # Step 1: Discovery query identifies the latest verified publication: pub-202302-002
        discovery_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DiscoveryLatestPublication.m").read_text(encoding="utf-8"))
        cur.execute(discovery_sql)
        discovered = cur.fetchone()
        self.assertEqual(discovered["publication_id"], "pub-202302-002")
        self.assertEqual(discovered["bundle_suffix"], new_suffix[:24])

        # Step 2: Analyst pins PinnedPublicationId = "pub-202302-002" in Parameters.m
        bundle_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "ActiveBundle.m").read_text(encoding="utf-8"),
            bundle_suffix=new_suffix[:24],
            publication_id="pub-202302-002"
        )
        cur.execute(bundle_sql)
        resolved_bundle = cur.fetchone()
        self.assertEqual(resolved_bundle["publication_id"], "pub-202302-002")
        self.assertEqual(resolved_bundle["bundle_suffix"], new_suffix[:24])

        # All queries use the resolved bundle_suffix and publication_id:
        fact_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "FactEarthquakeEvents.m").read_text(encoding="utf-8"),
            bundle_suffix=new_suffix[:24],
            publication_id="pub-202302-002"
        )
        cur.execute(fact_sql)
        self.assertEqual(len(cur.fetchall()), 10, "Fact query reads new verified bundle")

        daily_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "FactEventDaily.m").read_text(encoding="utf-8"),
            bundle_suffix=new_suffix[:24],
            publication_id="pub-202302-002"
        )
        cur.execute(daily_sql)
        self.assertEqual(sum(r["event_count"] for r in cur.fetchall()), 10, "Daily aggregate matches 10 events")

        freshness_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "MetadataFreshness.m").read_text(encoding="utf-8"),
            bundle_suffix=new_suffix[:24],
            publication_id="pub-202302-002"
        )
        cur.execute(freshness_sql)
        fresh_row = cur.fetchone()
        self.assertEqual(fresh_row["publication_id"], "pub-202302-002")
        self.assertEqual(fresh_row["published_event_count"], 10)
        # All queries remain 100% consistent on pub-202302-002 despite alias switch not yet happening!

    def test_regression_refresh_across_alias_switch(self) -> None:
        """Regression test: Mid-refresh arrival of a new PASSED publication does not split-brain pinned consumers."""
        cur = self.conn.cursor()
        # Refresh session starts: Parameters[PinnedPublicationId] is pinned to pub-202302-001
        pinned_pub_id = "pub-202302-001"
        pinned_bundle_suffix = "bndl20230201sha256test01"

        # 1. Fact table evaluates ActiveBundle with pinned_pub_id
        fact_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "FactEarthquakeEvents.m").read_text(encoding="utf-8"),
            bundle_suffix=pinned_bundle_suffix,
            publication_id=pinned_pub_id
        )
        cur.execute(fact_sql)
        self.assertEqual(len(cur.fetchall()), 9)

        # 2. Mid-refresh: A new publication pub-202302-002 is committed with later timestamp!
        # Also alias is switched to point to the raw table or new bundle.
        new_suffix = "bndl20230208sha256test02"
        cur.execute(f"""
            INSERT INTO publication_status VALUES (
                'pub-202302-002', 'gold-run-002', 9999999999,
                'iceberg.gold.earthquake_event_current', 'iceberg.gold.event_current',
                '2023-02-08T01:00:00Z', '2023-02-08T01:05:00Z', '2023-02-08T01:10:00Z',
                '2023-02-08T00:00:00Z', 10, '1.0', 'PASSED', '{new_suffix}',
                '{{}}', '{{}}', 'iceberg.gold.v_{new_suffix}_earthquake_event_current'
            )
        """)
        cur.execute("DROP VIEW earthquake_event_current")
        cur.execute("CREATE VIEW earthquake_event_current AS SELECT * FROM event_current")  # Points to raw
        self.conn.commit()

        # Notice that DiscoveryLatestPublication will now see pub-202302-002 (latest)
        discovery_sql = extract_sql_from_m_script((POWER_QUERY_DIR / "DiscoveryLatestPublication.m").read_text(encoding="utf-8"))
        cur.execute(discovery_sql)
        self.assertEqual(cur.fetchone()["publication_id"], "pub-202302-002")

        # 3. But in the active refresh session, all consumer queries continue evaluating with pinned_pub_id!
        # Because ActiveBundle enforces PinnedPublicationId and has NO auto-resolve fallback,
        # every query strictly evaluates only pub-202302-001:
        dim_region_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "DimRegion.m").read_text(encoding="utf-8"),
            bundle_suffix=pinned_bundle_suffix,
            publication_id=pinned_pub_id
        )
        cur.execute(dim_region_sql)
        self.assertEqual(len(cur.fetchall()), 5)

        daily_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "FactEventDaily.m").read_text(encoding="utf-8"),
            bundle_suffix=pinned_bundle_suffix,
            publication_id=pinned_pub_id
        )
        cur.execute(daily_sql)
        self.assertEqual(sum(r["event_count"] for r in cur.fetchall()), 9)

        freshness_sql = extract_sql_from_m_script(
            (POWER_QUERY_DIR / "MetadataFreshness.m").read_text(encoding="utf-8"),
            bundle_suffix=pinned_bundle_suffix,
            publication_id=pinned_pub_id
        )
        cur.execute(freshness_sql)
        fresh_row = cur.fetchone()
        self.assertEqual(fresh_row["publication_id"], pinned_pub_id)
        self.assertEqual(fresh_row["published_event_count"], 9)
        # 100% immune to split-brain across independent evaluations!

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

    def test_trino_proxy_json_query_extraction(self) -> None:
        """Verifies that Trino proxy extracts raw SQL from Starburst JSON payload and leaves plain SQL intact."""
        import sys
        sys.path.insert(0, str(PROJECT_ROOT))
        from scripts.trino_proxy import extract_query_from_body

        # Starburst JSON wrapper
        starburst_json = b'{"query":"SELECT count(*) FROM iceberg.gold.publication_status","preparedStatements":{}}'
        extracted = extract_query_from_body(starburst_json)
        self.assertEqual(extracted, b"SELECT count(*) FROM iceberg.gold.publication_status")

        # Plain SQL passthrough
        plain_sql = b"SELECT 1"
        self.assertEqual(extract_query_from_body(plain_sql), b"SELECT 1")
        self.assertEqual(extract_query_from_body(b""), b"")

    def test_trino_proxy_uri_rewriting(self) -> None:
        """Verifies that Trino proxy rewrites nextUri and cancelUri in JSON responses back to proxy port."""
        import sys
        sys.path.insert(0, str(PROJECT_ROOT))
        from scripts.trino_proxy import rewrite_uris_in_json_response

        upstream_resp = (
            b'{"id":"20261114_1","nextUri":"http://127.0.0.1:8081/v1/statement/queued/20261114_1/1",'
            b'"infoUri":"http://127.0.0.1:8081/ui/query.html?20261114_1"}'
        )
        rewritten = rewrite_uris_in_json_response(
            upstream_resp,
            upstream_base="http://127.0.0.1:8081",
            proxy_base="http://127.0.0.1:8082"
        )
        self.assertIn(b"http://127.0.0.1:8082/v1/statement/queued/20261114_1/1", rewritten)
        self.assertIn(b"http://127.0.0.1:8082/ui/query.html?20261114_1", rewritten)
        self.assertNotIn(b"http://127.0.0.1:8081", rewritten)


if __name__ == "__main__":
    unittest.main()
