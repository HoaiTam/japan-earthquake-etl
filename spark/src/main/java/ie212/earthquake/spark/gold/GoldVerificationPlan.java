package ie212.earthquake.spark.gold;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** GLD-04 draft SQL plan and fixture harness. There is no live Trino/publication adapter here. */
public final class GoldVerificationPlan {
    public static final Set<String> REQUIRED_CHECKS = Set.of("readable", "counts_reconciled", "canonical_unique",
            "required_fields_valid", "scope_respected", "lineage_resolved");
    private final Map<String, Long> snapshots;
    private final Map<String, Long> expectedCounts;
    private final String eventTable;
    private final String bridgeTable;

    public GoldVerificationPlan(String catalog, Map<String, Long> snapshots, Map<String, Long> expectedCounts) {
        if (catalog == null || !catalog.matches("[a-z][a-z0-9_]*")) throw new IllegalArgumentException("invalid catalog");
        this.snapshots = Map.copyOf(snapshots);
        this.expectedCounts = Map.copyOf(expectedCounts);
        eventTable = catalog + ".gold.event_current";
        bridgeTable = catalog + ".gold.event_source_bridge";
        if (!snapshots.keySet().containsAll(List.of(eventTable, bridgeTable)) || !snapshots.keySet().equals(expectedCounts.keySet())
                || snapshots.keySet().stream().anyMatch(name -> !name.matches(catalog + "\\.gold\\.[a-z][a-z0-9_]*"))
                || snapshots.values().stream().anyMatch(id -> id <= 0) || expectedCounts.values().stream().anyMatch(count -> count < 0)) {
            throw new IllegalArgumentException("exact committed table/snapshot/count bundle required");
        }
    }
    public Map<String, Long> snapshots() { return snapshots; }

    public String snapshotReference(String table) {
        if (!snapshots.containsKey(table)) throw new IllegalArgumentException("table outside pinned bundle");
        return table + " FOR VERSION AS OF " + snapshots.get(table);
    }

    /** Draft queries. A production harness must execute all, compare counts and inspect schema/types. */
    public Map<String, String> sql() {
        Map<String, String> result = new LinkedHashMap<>();
        snapshots.keySet().stream().sorted().forEach(table -> result.put("count:" + table, "SELECT count(*) FROM " + snapshotReference(table)));
        String e = snapshotReference(eventTable), b = snapshotReference(bridgeTable);
        result.put("canonical_duplicates", "SELECT count(*) - count(DISTINCT canonical_event_id) FROM " + e);
        result.put("bridge_duplicates", "SELECT count(*) FROM (SELECT canonical_event_id, source_observation_id FROM " + b
                + " GROUP BY canonical_event_id, source_observation_id HAVING count(*) > 1) d");
        String[] fields = {"schema_version", "canonical_event_id", "primary_observation_id", "canonical_source_system", "event_time_utc",
                "event_time_jst", "event_date_utc", "event_date_jst", "event_date_key_utc", "event_date_key_jst", "latitude", "longitude",
                "event_type_code", "is_natural_earthquake", "is_in_study_area", "region_key", "region_name", "region_category",
                "magnitude_band_code", "depth_band_code", "source_count", "has_usgs", "has_jma", "source_coverage_code", "link_status",
                "quality_status", "canonical_model_version", "gold_run_id", "record_updated_at_utc"};
        String nulls = String.join(" OR ", java.util.Arrays.stream(fields).map(field -> field + " IS NULL").toList());
        result.put("required_fields", "SELECT count(*) FROM " + e + " WHERE " + nulls
                + " OR latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180"
                + " OR source_count NOT IN (1, 2) OR quality_status NOT IN ('VALID', 'WARNING')"
                + " OR (has_jma AND (catalog_era IS NULL OR catalog_era NOT IN ('LEGACY', 'UNIFIED')))"
                + " OR source_coverage_code <> CASE WHEN has_usgs AND has_jma THEN 'USGS_JMA' WHEN has_usgs THEN 'USGS_ONLY' ELSE 'JMA_ONLY' END");
        result.put("bridge_orphans", "SELECT count(*) FROM " + b + " b LEFT JOIN " + e
                + " e ON b.canonical_event_id = e.canonical_event_id WHERE e.canonical_event_id IS NULL"
                + " OR b.source_observation_id IS NULL OR b.bronze_manifest_id IS NULL OR b.raw_object_uri IS NULL");
        result.put("bridge_source_counts", "SELECT count(*) FROM " + e
                + " e LEFT JOIN (SELECT canonical_event_id, count(*) AS n, count_if(is_primary) AS p FROM " + b
                + " GROUP BY canonical_event_id) b ON e.canonical_event_id = b.canonical_event_id"
                + " WHERE b.n IS NULL OR b.n <> e.source_count OR b.p <> 1");
        result.put("primary_provenance", "SELECT count(*) FROM " + e + " e LEFT JOIN " + b
                + " b ON e.canonical_event_id = b.canonical_event_id AND b.is_primary"
                + " WHERE b.source_observation_id IS NULL OR b.source_observation_id <> e.primary_observation_id"
                + " OR b.source_system <> e.canonical_source_system");
        result.put("natural_roi_count", "SELECT count(*) FROM " + e + " WHERE is_natural_earthquake AND is_in_study_area");
        return Map.copyOf(result);
    }

    /** Compare full event rows outside affected months against an exact baseline, never latest. */
    public String outsideScopeSql(long baselineSnapshot, List<Integer> affectedYearMonths) {
        if (baselineSnapshot <= 0 || affectedYearMonths.isEmpty() || affectedYearMonths.stream().anyMatch(value ->
                value / 100 < 1900 || value / 100 > 9999 || value % 100 < 1 || value % 100 > 12)) {
            throw new IllegalArgumentException("exact baseline and month scope required");
        }
        String months = String.join(",", affectedYearMonths.stream().distinct().sorted().map(Object::toString).toList());
        String outside = " WHERE (year(event_time_utc) * 100 + month(event_time_utc)) NOT IN (" + months + ")";
        String before = "SELECT * FROM " + eventTable + " FOR VERSION AS OF " + baselineSnapshot + outside;
        String after = "SELECT * FROM " + snapshotReference(eventTable) + outside;
        return "SELECT count(*) FROM ((" + before + " EXCEPT " + after + ") UNION ALL (" + after + " EXCEPT " + before + ")) d";
    }

    public String servingViewSql() {
        return "CREATE OR REPLACE VIEW " + eventTable.replace(".event_current", ".earthquake_event_current")
                + " AS SELECT * FROM " + eventTable + " WHERE is_natural_earthquake AND is_in_study_area";
    }

    /** Fixture evaluation cannot be passed to publication as real Trino evidence. */
    public FixtureReport evaluateFixture(Map<String, Long> observedSnapshots, Map<String, Long> observedCounts, Map<String, Boolean> checks) {
        if (!snapshots.equals(observedSnapshots) || !expectedCounts.equals(observedCounts) || !checks.keySet().equals(REQUIRED_CHECKS)
                || checks.values().stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalArgumentException("fixture receipt snapshot/count/check mismatch");
        }
        return new FixtureReport(snapshots, checks, checks.values().stream().allMatch(Boolean.TRUE::equals));
    }
    public record FixtureReport(Map<String, Long> snapshots, Map<String, Boolean> checks, boolean fixturePassed) {
        public FixtureReport { snapshots = Map.copyOf(snapshots); checks = Map.copyOf(checks); }
        public boolean canPublish() { return false; }
        public String engine() { return "MOCK_TRINO"; }
    }
}
