package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

/**
 * Deterministic source-local deduplication and revision resolution (CON-03, CON-04, SLV-06).
 * <p>
 * Partition key: (source_system, source_record_key).
 * USGS tie-break:
 *   1. source_updated_at_utc DESC (nulls last)
 *   2. processed_at_utc DESC
 *   3. raw_record_hash ASC
 * JMA tie-break:
 *   1. catalog_release_at_utc DESC (nulls last)
 *   2. catalog_release normalized inventory/version order DESC
 *   3. processed_at_utc DESC
 *   4. raw_record_hash ASC
 * <p>
 * Ensures exact duplicates and superseded older revisions are identified,
 * while preserving full history for downstream auditability without cross-source confusion.
 */
public final class SourceDedupTransformer implements Serializable {

    private final JmaReleaseComparator jmaReleaseComparator;

    public SourceDedupTransformer() {
        this(new JmaReleaseComparator());
    }

    public SourceDedupTransformer(JmaReleaseComparator jmaReleaseComparator) {
        this.jmaReleaseComparator = Objects.requireNonNull(jmaReleaseComparator, "jmaReleaseComparator");
    }

    /**
     * Executes source-local deduplication and revision selection on a list of observations.
     *
     * @param observations input observation collection (must not be null and must not contain null elements)
     * @return SourceDedupResult containing current observations, history observations, all observations, and metrics
     */
    public SourceDedupResult deduplicate(List<SilverObservation> observations) {
        if (observations == null) {
            throw new IllegalArgumentException("observations must not be null");
        }
        if (observations.isEmpty()) {
            return new SourceDedupResult(
                    List.of(),
                    List.of(),
                    List.of(),
                    SourceDedupMetrics.of(0, 0, 0, 0, Map.of()));
        }

        // Group observations by (source_system, source_record_key)
        Map<SourcePartitionKey, List<SilverObservation>> groups = new LinkedHashMap<>();
        for (SilverObservation obs : observations) {
            if (obs == null) {
                throw new IllegalArgumentException("observation elements must not be null");
            }
            SourcePartitionKey key = new SourcePartitionKey(obs.sourceSystem(), obs.sourceRecordKey());
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(obs);
        }

        List<SilverObservation> currentObservations = new ArrayList<>(groups.size());
        List<SilverObservation> historyObservations = new ArrayList<>();
        List<SilverObservation> allObservations = new ArrayList<>(observations.size());

        Map<String, int[]> sourceCounts = new LinkedHashMap<>(); // sourceSystem -> [input, current, duplicate, superseded]

        for (Map.Entry<SourcePartitionKey, List<SilverObservation>> entry : groups.entrySet()) {
            SourcePartitionKey key = entry.getKey();
            List<SilverObservation> group = entry.getValue();
            String sourceSystem = key.sourceSystem();

            int[] counts = sourceCounts.computeIfAbsent(sourceSystem, s -> new int[4]);
            counts[0] += group.size(); // inputCount

            // Sort within group using source-specific deterministic comparator
            Comparator<SilverObservation> comparator = comparatorFor(sourceSystem);
            group.sort(comparator);

            // Winner is the current revision (rank 1)
            SilverObservation winner = group.get(0).withCurrentSourceRevision(true);
            currentObservations.add(winner);
            allObservations.add(winner);
            counts[1]++; // currentCount

            // Process loser records (rank 2..N)
            Set<String> seenHashesInGroup = new HashSet<>();
            seenHashesInGroup.add(winner.rawRecordHash());

            for (int i = 1; i < group.size(); i++) {
                SilverObservation loser = group.get(i).withCurrentSourceRevision(false);
                historyObservations.add(loser);
                allObservations.add(loser);

                if (seenHashesInGroup.contains(loser.rawRecordHash())) {
                    counts[2]++; // duplicateCount
                } else {
                    counts[3]++; // supersededCount
                    seenHashesInGroup.add(loser.rawRecordHash());
                }
            }
        }

        // Deterministic sorting of results for repeatable output regardless of input permutations
        Comparator<SilverObservation> presentationOrder = Comparator
                .comparing(SilverObservation::eventTimeUtc)
                .thenComparing(SilverObservation::sourceSystem)
                .thenComparing(SilverObservation::sourceRecordKey)
                .thenComparing(SilverObservation::sourceObservationId);

        currentObservations.sort(presentationOrder);
        historyObservations.sort(presentationOrder);

        Comparator<SilverObservation> allOrder = Comparator
                .comparing(SilverObservation::eventTimeUtc)
                .thenComparing(SilverObservation::sourceSystem)
                .thenComparing(SilverObservation::sourceRecordKey)
                .thenComparing(SilverObservation::isCurrentSourceRevision, Comparator.reverseOrder())
                .thenComparing(SilverObservation::sourceObservationId);

        allObservations.sort(allOrder);

        // Build metrics
        int totalInput = observations.size();
        int totalCurrent = currentObservations.size();
        int totalDuplicate = 0;
        int totalSuperseded = 0;

        Map<String, SourceDedupMetrics> bySource = new LinkedHashMap<>();
        for (Map.Entry<String, int[]> sc : sourceCounts.entrySet()) {
            int in = sc.getValue()[0];
            int cur = sc.getValue()[1];
            int dup = sc.getValue()[2];
            int sup = sc.getValue()[3];
            totalDuplicate += dup;
            totalSuperseded += sup;
            bySource.put(sc.getKey(), SourceDedupMetrics.of(in, cur, dup, sup));
        }

        SourceDedupMetrics metrics = SourceDedupMetrics.of(
                totalInput, totalCurrent, totalDuplicate, totalSuperseded, bySource);

        return new SourceDedupResult(currentObservations, allObservations, historyObservations, metrics);
    }

    /**
     * Resolves the deterministic tie-break comparator for a source system.
     */
    private Comparator<SilverObservation> comparatorFor(String sourceSystem) {
        if ("USGS".equalsIgnoreCase(sourceSystem)) {
            return usgsComparator();
        } else if ("JMA_BULLETIN".equalsIgnoreCase(sourceSystem)) {
            return jmaComparator();
        } else {
            return fallbackComparator();
        }
    }

    /**
     * USGS tie-break comparator:
     * 1. source_updated_at_utc DESC (nulls last)
     * 2. processed_at_utc DESC
     * 3. raw_record_hash ASC
     * 4. source_observation_id ASC (stable fallback)
     */
    private Comparator<SilverObservation> usgsComparator() {
        return (o1, o2) -> {
            // 1. source_updated_at_utc DESC
            int updatedCompare = compareNullLastDesc(o1.sourceUpdatedAtUtc(), o2.sourceUpdatedAtUtc());
            if (updatedCompare != 0) {
                return updatedCompare;
            }
            // 2. processed_at_utc DESC
            int processedCompare = o2.processedAtUtc().compareTo(o1.processedAtUtc());
            if (processedCompare != 0) {
                return processedCompare;
            }
            // 3. raw_record_hash ASC (tie-break xác định tăng dần)
            int hashCompare = o1.rawRecordHash().compareTo(o2.rawRecordHash());
            if (hashCompare != 0) {
                return hashCompare;
            }
            // 4. source_observation_id ASC
            return o1.sourceObservationId().compareTo(o2.sourceObservationId());
        };
    }

    /**
     * JMA tie-break comparator:
     * 1. catalog_release_at_utc DESC (nulls last)
     * 2. catalog_release normalized inventory/version order DESC
     * 3. processed_at_utc DESC
     * 4. raw_record_hash ASC
     * 5. source_observation_id ASC (stable fallback)
     */
    private Comparator<SilverObservation> jmaComparator() {
        return (o1, o2) -> {
            // 1. catalog_release_at_utc DESC
            int releaseTimeCompare = compareNullLastDesc(o1.catalogReleaseAtUtc(), o2.catalogReleaseAtUtc());
            if (releaseTimeCompare != 0) {
                return releaseTimeCompare;
            }
            // 2. catalog_release DESC (jmaReleaseComparator compares ascending, so o2 vs o1 for DESC)
            int releaseCompare = jmaReleaseComparator.compare(o2.catalogRelease(), o1.catalogRelease());
            if (releaseCompare != 0) {
                return releaseCompare;
            }
            // 3. processed_at_utc DESC
            int processedCompare = o2.processedAtUtc().compareTo(o1.processedAtUtc());
            if (processedCompare != 0) {
                return processedCompare;
            }
            // 4. raw_record_hash ASC (tie-break xác định tăng dần)
            int hashCompare = o1.rawRecordHash().compareTo(o2.rawRecordHash());
            if (hashCompare != 0) {
                return hashCompare;
            }
            // 5. source_observation_id ASC
            return o1.sourceObservationId().compareTo(o2.sourceObservationId());
        };
    }

    private Comparator<SilverObservation> fallbackComparator() {
        return (o1, o2) -> {
            int updatedCompare = compareNullLastDesc(o1.sourceUpdatedAtUtc(), o2.sourceUpdatedAtUtc());
            if (updatedCompare != 0) {
                return updatedCompare;
            }
            int processedCompare = o2.processedAtUtc().compareTo(o1.processedAtUtc());
            if (processedCompare != 0) {
                return processedCompare;
            }
            int hashCompare = o1.rawRecordHash().compareTo(o2.rawRecordHash());
            if (hashCompare != 0) {
                return hashCompare;
            }
            return o1.sourceObservationId().compareTo(o2.sourceObservationId());
        };
    }

    private static int compareNullLastDesc(Instant t1, Instant t2) {
        if (t1 == null && t2 == null) {
            return 0;
        }
        if (t1 == null) {
            return 1; // t2 is non-null, so t2 comes first
        }
        if (t2 == null) {
            return -1; // t1 is non-null, so t1 comes first
        }
        return t2.compareTo(t1); // DESC
    }

    /**
     * Spark Window specification for USGS observations:
     * PARTITION BY source_system, source_record_key
     * ORDER BY source_updated_at_utc DESC NULLS LAST, processed_at_utc DESC, raw_record_hash ASC
     */
    public static WindowSpec usgsWindowSpec() {
        return Window.partitionBy("source_system", "source_record_key")
                .orderBy(
                        functions.col("source_updated_at_utc").desc_nulls_last(),
                        functions.col("processed_at_utc").desc(),
                        functions.col("raw_record_hash").asc());
    }

    /**
     * Spark Window specification for JMA observations:
     * PARTITION BY source_system, source_record_key
     * ORDER BY catalog_release_at_utc DESC NULLS LAST, catalog_release DESC NULLS LAST, processed_at_utc DESC, raw_record_hash ASC
     */
    public static WindowSpec jmaWindowSpec() {
        return Window.partitionBy("source_system", "source_record_key")
                .orderBy(
                        functions.col("catalog_release_at_utc").desc_nulls_last(),
                        functions.col("catalog_release").desc_nulls_last(),
                        functions.col("processed_at_utc").desc(),
                        functions.col("raw_record_hash").asc());
    }

    private record SourcePartitionKey(String sourceSystem, String sourceRecordKey) implements Serializable {
        SourcePartitionKey {
            Objects.requireNonNull(sourceSystem, "sourceSystem");
            Objects.requireNonNull(sourceRecordKey, "sourceRecordKey");
        }
    }
}
