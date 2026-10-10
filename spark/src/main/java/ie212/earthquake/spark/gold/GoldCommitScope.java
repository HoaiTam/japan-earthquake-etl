package ie212.earthquake.spark.gold;

import java.time.YearMonth;
import java.util.List;
import java.util.Map;

/** Complete replacement months, explicitly resolved upstream; never inferred from incoming rows. */
public record GoldCommitScope(GoldRunContext context, String silverBundleSha256,
        String codeVersion, List<String> affectedMonths, Map<String, Long> baselineSnapshots) {
    public GoldCommitScope {
        if (context == null || !context.runId().matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}")
                || silverBundleSha256 == null || !silverBundleSha256.matches("[a-f0-9]{64}")
                || codeVersion == null || !codeVersion.matches("[A-Za-z0-9._-]{1,128}")
                || affectedMonths == null || affectedMonths.isEmpty() || affectedMonths.size() > 480
                || baselineSnapshots == null) throw new IllegalArgumentException("EXACT_GOLD_SCOPE_REQUIRED");
        affectedMonths = affectedMonths.stream().distinct().sorted().toList();
        affectedMonths.forEach(YearMonth::parse);
        baselineSnapshots = Map.copyOf(baselineSnapshots);
        if (baselineSnapshots.values().stream().anyMatch(id -> id == null || id < 0))
            throw new IllegalArgumentException("INVALID_BASELINE_SNAPSHOT");
    }
}
