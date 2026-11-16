package ie212.earthquake.spark.gold;

import java.time.YearMonth;
import java.util.List;
import java.util.Map;

/** Complete replacement months, explicitly resolved upstream; never inferred from incoming rows. */
public record GoldCommitScope(GoldRunContext context, String silverBundleSha256,
        String codeVersion, List<String> affectedMonths, Map<String, Long> baselineSnapshots, PreparedGoldProof preparedProof) {
    public GoldCommitScope(GoldRunContext context, String silverBundleSha256, String codeVersion,
            List<String> affectedMonths, Map<String, Long> baselineSnapshots) {
        this(context, silverBundleSha256, codeVersion, affectedMonths, baselineSnapshots, null);
    }
    public GoldCommitScope {
        if (context == null || !context.runId().matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
                || (preparedProof == null ? silverBundleSha256 == null || !silverBundleSha256.matches("[a-f0-9]{64}") : silverBundleSha256 != null)
                || codeVersion == null || !codeVersion.matches("[A-Za-z0-9._-]{1,128}")
                || affectedMonths == null || affectedMonths.isEmpty() || affectedMonths.size() > 480
                || baselineSnapshots == null) throw new IllegalArgumentException("EXACT_GOLD_SCOPE_REQUIRED");
        if (preparedProof != null && (!PreparedGoldProof.VERSION.equals(codeVersion)
                || !affectedMonths.equals(PortableImportContract.ALL_MONTHS)
                || !context.windowStartUtc().equals(java.time.Instant.parse("2000-01-01T00:00:00Z"))
                || !context.windowEndUtc().equals(java.time.Instant.parse("2024-01-01T00:00:00Z"))
                || !context.isBackfill() || !PreparedGoldProof.VERSION.equals(context.configVersion())
                || !baselineSnapshots.keySet().equals(new java.util.HashSet<>(GoldIcebergWriter.TABLES))
                || baselineSnapshots.values().stream().anyMatch(id -> id == null || id != 0)))
            throw new IllegalArgumentException("EXACT_PREPARED_FULL_CATALOG_SCOPE_REQUIRED");
        affectedMonths = affectedMonths.stream().distinct().sorted().toList();
        affectedMonths.forEach(YearMonth::parse);
        baselineSnapshots = Map.copyOf(baselineSnapshots);
        if (baselineSnapshots.values().stream().anyMatch(id -> id == null || id < 0))
            throw new IllegalArgumentException("INVALID_BASELINE_SNAPSHOT");
    }
}
