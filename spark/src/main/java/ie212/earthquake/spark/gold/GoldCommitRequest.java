package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.HexFormat;

/** Immutable interface fixture for a bounded multi-table commit; baseline 0 means table absent. */
public record GoldCommitRequest(String operationId, String runId, Instant windowStartUtc, Instant windowEndUtc,
        LocalDate processingDate, boolean isBackfill, String configVersion, List<String> silverManifestUris,
        String catalogName, Map<String, Long> baselineSnapshots, List<AffectedPartition> affectedPartitions,
        Map<String, Long> expectedRowCounts, boolean qualityPassed) {
    public record AffectedPartition(int year, int month) {
        public AffectedPartition {
            if (year < 1900 || year > 9999 || month < 1 || month > 12) throw new IllegalArgumentException("invalid partition");
        }
    }
    public GoldCommitRequest {
        if (operationId == null || operationId.isBlank() || runId == null || runId.isBlank()
                || windowStartUtc == null || windowEndUtc == null || !windowStartUtc.isBefore(windowEndUtc)
                || processingDate == null || configVersion == null || configVersion.isBlank()
                || catalogName == null || !catalogName.matches("[a-z][a-z0-9_]*") || !qualityPassed) {
            throw new IllegalArgumentException("invalid context or failed quality gate");
        }
        silverManifestUris = List.copyOf(silverManifestUris);
        baselineSnapshots = Map.copyOf(baselineSnapshots);
        affectedPartitions = List.copyOf(affectedPartitions);
        expectedRowCounts = Map.copyOf(expectedRowCounts);
        if (silverManifestUris.isEmpty() || silverManifestUris.stream().anyMatch(uri -> !uri.startsWith("s3://")
                || uri.contains("*") || uri.contains("?") || uri.contains("@") || uri.contains(".."))
                || silverManifestUris.stream().distinct().count() != silverManifestUris.size()
                || affectedPartitions.isEmpty() || affectedPartitions.stream().distinct().count() != affectedPartitions.size()) {
            throw new IllegalArgumentException("exact unique input and affected scope required");
        }
        if (!baselineSnapshots.keySet().containsAll(List.of(catalogName + ".gold.event_current", catalogName + ".gold.event_source_bridge"))
                || !expectedRowCounts.keySet().equals(baselineSnapshots.keySet())
                || baselineSnapshots.keySet().stream().anyMatch(table -> !table.matches(catalogName + "\\.gold\\.[a-z][a-z0-9_]*"))
                || baselineSnapshots.values().stream().anyMatch(id -> id < 0)
                || expectedRowCounts.values().stream().anyMatch(count -> count < 0)) {
            throw new IllegalArgumentException("invalid required table/baseline/count scope");
        }
    }

    /** Canonical hash excludes attempt/time/staging location; rerun must inspect this exact operation. */
    public String identitySha256() {
        try {
            Map<String, Object> value = new TreeMap<>();
            value.put("operation_id", operationId); value.put("run_id", runId);
            value.put("window_start_utc", windowStartUtc.toString()); value.put("window_end_utc", windowEndUtc.toString());
            value.put("processing_date", processingDate.toString()); value.put("is_backfill", isBackfill);
            value.put("config_version", configVersion); value.put("silver_manifests", silverManifestUris.stream().sorted().toList());
            value.put("baseline_snapshots", new TreeMap<>(baselineSnapshots)); value.put("expected_row_counts", new TreeMap<>(expectedRowCounts));
            value.put("affected_partitions", affectedPartitions.stream().map(p -> String.format("%04d-%02d", p.year(), p.month())).sorted().toList());
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(new ObjectMapper().writeValueAsString(value).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception exception) { throw new IllegalStateException("cannot hash commit interface payload", exception); }
    }
}
