package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Result of publishing Silver partitions and manifests (CON-03 1.0, SLV-08, SLV-09).
 */
public record SilverWriteResult(
        String silverStatus,
        String runId,
        List<SilverPartitionManifest> publishedPartitions,
        int totalObservations,
        int totalRejects,
        int totalLinks,
        int totalMemberships,
        boolean idempotentReuse,
        List<String> rejectFiles,
        List<String> linkFiles,
        List<String> membershipFiles) implements Serializable {

    public SilverWriteResult {
        Objects.requireNonNull(silverStatus, "silverStatus");
        Objects.requireNonNull(runId, "runId");
        publishedPartitions = publishedPartitions != null
                ? Collections.unmodifiableList(publishedPartitions)
                : List.of();
        rejectFiles = rejectFiles != null
                ? Collections.unmodifiableList(rejectFiles)
                : List.of();
        linkFiles = linkFiles != null
                ? Collections.unmodifiableList(linkFiles)
                : List.of();
        membershipFiles = membershipFiles != null
                ? Collections.unmodifiableList(membershipFiles)
                : List.of();
    }

    public SilverWriteResult(
            String silverStatus,
            String runId,
            List<SilverPartitionManifest> publishedPartitions,
            int totalObservations,
            int totalRejects,
            boolean idempotentReuse,
            List<String> rejectFiles) {
        this(silverStatus, runId, publishedPartitions, totalObservations, totalRejects, 0, 0, idempotentReuse, rejectFiles, List.of(), List.of());
    }
}
