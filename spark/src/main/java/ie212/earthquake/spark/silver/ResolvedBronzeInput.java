package ie212.earthquake.spark.silver;

import java.nio.file.Path;

public record ResolvedBronzeInput(
        String manifestId,
        String sourceSystem,
        String catalogRelease,
        String runId,
        String rawObjectUri,
        String rawObjectKey,
        String sha256,
        long contentLengthBytes,
        Path stagedObject,
        Path stagedManifest,
        boolean idempotentReuse) {
}
