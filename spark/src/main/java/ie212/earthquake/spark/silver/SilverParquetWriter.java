package ie212.earthquake.spark.silver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Deterministic Parquet writer for Silver observation and reject datasets (CON-03 1.0, SLV-08).
 * Ensures atomic staging, pre-publish readback verification, idempotent partition overwrite,
 * and publish marker generation (_SUCCESS and manifest.json).
 */
public final class SilverParquetWriter {

    private static final String PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet";
    private static final String JSON_CONTENT_TYPE = "application/json";
    private static final String TEXT_CONTENT_TYPE = "text/plain";
    private static final String DEFAULT_WRITER_VERSION = "slv-08";

    private final SilverObjectStore store;
    private final SilverParquetSerializer serializer;
    private final String writerVersion;

    public SilverParquetWriter(SilverObjectStore store) {
        this(store, new SilverParquetSerializer(), DEFAULT_WRITER_VERSION);
    }

    public SilverParquetWriter(SilverObjectStore store, SilverParquetSerializer serializer, String writerVersion) {
        this.store = Objects.requireNonNull(store, "store");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.writerVersion = Objects.requireNonNull(writerVersion, "writerVersion");
    }

    /** Checked integration entry point: a blocked/mismatched quality result must cause zero storage writes. */
    public SilverWriteResult write(SilverWriteRequest request, SilverQualityResult quality) throws IOException {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(quality, "quality");
        quality.requirePublishable();
        if (!request.runId().equals(quality.ingestRunId())
                || !request.observations().equals(quality.validObservations())
                || !request.rejects().equals(quality.rejectedRecords())) {
            throw new IllegalArgumentException("Silver write request must match the validated run and datasets");
        }
        return write(request);
    }

    /** Low-level persistence API; callers must supply their own run-level quality policy. */
    public SilverWriteResult write(SilverWriteRequest request) throws IOException {
        Objects.requireNonNull(request, "request");
        String runId = request.runId();

        // 1. Pre-validation: verify all records and strict UTC partition alignment
        for (SilverObservation obs : request.observations()) {
            SilverPartitionKey.from(obs); // Throws IllegalArgumentException on any UTC mismatch
        }

        // 2. Group observations by partition key
        Map<SilverPartitionKey, List<SilverObservation>> partitionMap = new TreeMap<>();
        for (SilverObservation obs : request.observations()) {
            SilverPartitionKey key = SilverPartitionKey.from(obs);
            partitionMap.computeIfAbsent(key, k -> new ArrayList<>()).add(obs);
        }

        String stagingRoot = SilverStorageLayout.stagingRootPath(runId);
        List<SilverPartitionManifest> publishedPartitions = new ArrayList<>();
        List<String> rejectFiles = new ArrayList<>();
        boolean allIdempotentReuse = !partitionMap.isEmpty();
        var touchedMarkers = new java.util.LinkedHashSet<String>();

        try {
            // 3. Process each observation partition
            for (Map.Entry<SilverPartitionKey, List<SilverObservation>> entry : partitionMap.entrySet()) {
                SilverPartitionKey partitionKey = entry.getKey();
                List<SilverObservation> obsList = entry.getValue();

                String targetPartitionPath = SilverStorageLayout.observationPartitionPath(partitionKey);
                String targetManifestPath = SilverStorageLayout.manifestPath(partitionKey);
                String targetSuccessPath = SilverStorageLayout.successMarkerPath(partitionKey);
                byte[] parquetBytes = serializer.serializeObservations(obsList);
                String sha256 = sha256(parquetBytes);

                // Check for idempotent rerun
                if (store.exists(targetManifestPath) && store.exists(targetSuccessPath)) {
                    byte[] existingManifestBytes = store.read(targetManifestPath);
                    SilverPartitionManifest existingManifest = SilverPartitionManifest.fromJson(existingManifestBytes);
                    if (existingManifest.runId().equals(runId)
                            && existingManifest.recordCount() == obsList.size()
                            && existingManifest.sourceSystem().equals(partitionKey.sourceSystem())) {
                        if (existingManifest.files().size() != 1
                                || !existingManifest.files().get(0).sha256().equals(sha256)
                                || !sha256(store.read(existingManifest.files().get(0).relativePath())).equals(sha256)) {
                            throw new IOException("Silver rerun identity/checksum conflict");
                        }
                        publishedPartitions.add(existingManifest);
                        continue; // Idempotent reuse: partition already published with matching signature
                    }
                }
                allIdempotentReuse = false;

                // Serialize observations to Parquet in-memory

                // Verify Parquet structure and row count
                SilverParquetSerializer.verifyParquet(
                        parquetBytes, obsList.size(), SilverParquetSerializer.OBSERVATION_PARQUET_SCHEMA);

                // Stage into isolated staging prefix
                String stagingPartitionPath = SilverStorageLayout.stagingObservationPartitionPath(runId, partitionKey);
                String parquetFileName = SilverStorageLayout.parquetFileName(runId, 0);
                String stagedFileKey = stagingPartitionPath + "/" + parquetFileName;

                store.put(stagedFileKey, parquetBytes, PARQUET_CONTENT_TYPE);

                // Readback checksum verification from store
                byte[] readback = store.read(stagedFileKey);
                if (readback.length != parquetBytes.length || !sha256(readback).equals(sha256)) {
                    throw new IOException("Staged Silver Parquet readback checksum mismatch for " + stagedFileKey);
                }

                // If overwriting/retrying: remove old files in the partition to prevent appending duplicates
                invalidateMarker(targetSuccessPath, touchedMarkers);
                if (request.overwritePartition() && !store.list(targetPartitionPath + "/").isEmpty()) {
                    store.deletePrefix(targetPartitionPath + "/");
                }

                // Promote staged file to final target partition
                String targetFileKey = targetPartitionPath + "/" + parquetFileName;
                store.move(stagedFileKey, targetFileKey);
                verifyFinal(targetFileKey, parquetBytes);

                // Build file metadata
                SilverFileMetadata fileMetadata = new SilverFileMetadata(
                        parquetFileName,
                        targetFileKey,
                        sha256,
                        parquetBytes.length,
                        obsList.size());

                // Build quality summary
                Map<String, Integer> qualitySummary = calculateQualitySummary(obsList);

                // Build and write partition manifest
                SilverPartitionManifest manifest = new SilverPartitionManifest(
                        "1.0",
                        "SilverReady",
                        partitionKey.sourceSystem(),
                        partitionKey.eventYearUtc(),
                        partitionKey.eventMonthUtc(),
                        targetPartitionPath,
                        runId,
                        obsList.size(),
                        List.of(fileMetadata),
                        qualitySummary,
                        request.publishedAtUtc(),
                        writerVersion);

                store.put(targetManifestPath, manifest.toJsonBytes(), JSON_CONTENT_TYPE);

                // Write publish marker (_SUCCESS)
                byte[] successBytes = formatSuccessMarker(runId, request.publishedAtUtc(), obsList.size());
                store.put(targetSuccessPath, successBytes, TEXT_CONTENT_TYPE);

                publishedPartitions.add(manifest);
            }

            // 4. Process rejects if present
            if (!request.rejects().isEmpty()) {
                Map<String, List<SilverRejectRecord>> rejectsBySource = new LinkedHashMap<>();
                for (SilverRejectRecord r : request.rejects()) {
                    rejectsBySource.computeIfAbsent(r.sourceSystem(), s -> new ArrayList<>()).add(r);
                }

                for (Map.Entry<String, List<SilverRejectRecord>> entry : rejectsBySource.entrySet()) {
                    String source = entry.getKey();
                    List<SilverRejectRecord> rejects = entry.getValue();

                    byte[] rejectBytes = serializer.serializeRejects(rejects);
                    String sha256 = sha256(rejectBytes);

                    SilverParquetSerializer.verifyParquet(
                            rejectBytes, rejects.size(), SilverParquetSerializer.REJECT_PARQUET_SCHEMA);

                    String stagingRejectPath = SilverStorageLayout.stagingRejectPartitionPath(source, runId);
                    String rejectFileName = SilverStorageLayout.parquetFileName(runId, 0);
                    String stagedRejectKey = stagingRejectPath + "/" + rejectFileName;

                    store.put(stagedRejectKey, rejectBytes, PARQUET_CONTENT_TYPE);

                    byte[] readback = store.read(stagedRejectKey);
                    if (readback.length != rejectBytes.length || !sha256(readback).equals(sha256)) {
                        throw new IOException("Staged Silver Reject readback checksum mismatch: " + stagedRejectKey);
                    }

                    String targetRejectPath = SilverStorageLayout.rejectPartitionPath(source, runId);
                    invalidateMarker(targetRejectPath + "/" + SilverStorageLayout.SUCCESS_MARKER_FILE, touchedMarkers);
                    if (request.overwritePartition() && !store.list(targetRejectPath + "/").isEmpty()) {
                        store.deletePrefix(targetRejectPath + "/");
                    }

                    String targetRejectKey = targetRejectPath + "/" + rejectFileName;
                    store.move(stagedRejectKey, targetRejectKey);
                    verifyFinal(targetRejectKey, rejectBytes);

                    byte[] successBytes = formatSuccessMarker(runId, request.publishedAtUtc(), rejects.size());
                    store.put(targetRejectPath + "/" + SilverStorageLayout.SUCCESS_MARKER_FILE, successBytes, TEXT_CONTENT_TYPE);

                    rejectFiles.add(targetRejectKey);
                }
            }

            // 5. Process cross-source links if present
            List<String> linkFiles = new ArrayList<>();
            if (!request.links().isEmpty()) {
                String targetLinkPath = SilverStorageLayout.linkPartitionPath(runId);
                String linkFileName = SilverStorageLayout.parquetFileName(runId, 0);
                String targetLinkKey = targetLinkPath + "/" + linkFileName;
                String targetSuccessPath = SilverStorageLayout.linkSuccessMarkerPath(runId);

                byte[] linkBytes = serializer.serializeSourceLinks(request.links());
                String sha256 = sha256(linkBytes);

                SilverParquetSerializer.verifyParquet(
                        linkBytes, request.links().size(), SilverParquetSerializer.SOURCE_LINK_PARQUET_SCHEMA);

                String stagingLinkPath = SilverStorageLayout.stagingLinkPartitionPath(runId);
                String stagedLinkKey = stagingLinkPath + "/" + linkFileName;

                store.put(stagedLinkKey, linkBytes, PARQUET_CONTENT_TYPE);

                byte[] readback = store.read(stagedLinkKey);
                if (readback.length != linkBytes.length || !sha256(readback).equals(sha256)) {
                    throw new IOException("Staged Silver Link readback checksum mismatch: " + stagedLinkKey);
                }

                invalidateMarker(targetSuccessPath, touchedMarkers);
                if (request.overwritePartition() && !store.list(targetLinkPath + "/").isEmpty()) {
                    store.deletePrefix(targetLinkPath + "/");
                }

                store.move(stagedLinkKey, targetLinkKey);
                verifyFinal(targetLinkKey, linkBytes);

                byte[] successBytes = formatSuccessMarker(runId, request.publishedAtUtc(), request.links().size());
                store.put(targetSuccessPath, successBytes, TEXT_CONTENT_TYPE);

                linkFiles.add(targetLinkKey);
            }

            // 6. Process canonical memberships if present
            List<String> membershipFiles = new ArrayList<>();
            if (!request.memberships().isEmpty()) {
                String targetMembershipPath = SilverStorageLayout.membershipPartitionPath(runId);
                String membershipFileName = SilverStorageLayout.parquetFileName(runId, 0);
                String targetMembershipKey = targetMembershipPath + "/" + membershipFileName;
                String targetSuccessPath = SilverStorageLayout.membershipSuccessMarkerPath(runId);

                byte[] membershipBytes = serializer.serializeCanonicalMemberships(request.memberships());
                String sha256 = sha256(membershipBytes);

                SilverParquetSerializer.verifyParquet(
                        membershipBytes, request.memberships().size(), SilverParquetSerializer.CANONICAL_MEMBERSHIP_PARQUET_SCHEMA);

                String stagingMembershipPath = SilverStorageLayout.stagingMembershipPartitionPath(runId);
                String stagedMembershipKey = stagingMembershipPath + "/" + membershipFileName;

                store.put(stagedMembershipKey, membershipBytes, PARQUET_CONTENT_TYPE);

                byte[] readback = store.read(stagedMembershipKey);
                if (readback.length != membershipBytes.length || !sha256(readback).equals(sha256)) {
                    throw new IOException("Staged Silver Membership readback checksum mismatch: " + stagedMembershipKey);
                }

                invalidateMarker(targetSuccessPath, touchedMarkers);
                if (request.overwritePartition() && !store.list(targetMembershipPath + "/").isEmpty()) {
                    store.deletePrefix(targetMembershipPath + "/");
                }

                store.move(stagedMembershipKey, targetMembershipKey);
                verifyFinal(targetMembershipKey, membershipBytes);

                byte[] successBytes = formatSuccessMarker(runId, request.publishedAtUtc(), request.memberships().size());
                store.put(targetSuccessPath, successBytes, TEXT_CONTENT_TYPE);

                membershipFiles.add(targetMembershipKey);
            }

            // 7. Clean up staging directory on successful completion
            store.deletePrefix(stagingRoot + "/");

            return new SilverWriteResult(
                    "SilverReady",
                    runId,
                    publishedPartitions,
                    request.observations().size(),
                    request.rejects().size(),
                    request.links().size(),
                    request.memberships().size(),
                    allIdempotentReuse,
                    rejectFiles,
                    linkFiles,
                    membershipFiles);

        } catch (Exception ex) {
            for (String marker : touchedMarkers) {
                try { store.delete(marker); } catch (Exception markerFailure) { ex.addSuppressed(markerFailure); }
            }
            // On failure: ensure staging area is cleaned up and target partitions are not left corrupted
            try {
                store.deletePrefix(stagingRoot + "/");
            } catch (Exception cleanEx) {
                ex.addSuppressed(cleanEx);
            }
            if (ex instanceof IOException ioException) {
                throw ioException;
            }
            throw new IOException("Silver Parquet write failed for run " + runId + ": " + ex.getMessage(), ex);
        }
    }

    private void invalidateMarker(String key, java.util.Set<String> touched) throws IOException {
        touched.add(key);
        if (store.exists(key)) { store.delete(key); }
    }

    private void verifyFinal(String key, byte[] expected) throws IOException {
        if (!java.util.Arrays.equals(store.read(key), expected)) {
            throw new IOException("Final Silver Parquet readback mismatch: " + key);
        }
    }

    private static Map<String, Integer> calculateQualitySummary(List<SilverObservation> observations) {
        int validCount = 0;
        int warningCount = 0;
        for (SilverObservation obs : observations) {
            if ("WARNING".equalsIgnoreCase(obs.qualityStatus())) {
                warningCount++;
            } else {
                validCount++;
            }
        }
        Map<String, Integer> summary = new LinkedHashMap<>();
        summary.put("valid_count", validCount);
        summary.put("warning_count", warningCount);
        return summary;
    }

    private static byte[] formatSuccessMarker(String runId, Instant publishedAtUtc, int recordCount) {
        String content = "status=SUCCESS\nrun_id=" + runId
                + "\npublished_at_utc=" + publishedAtUtc
                + "\nrecord_count=" + recordCount + "\n";
        return content.getBytes(StandardCharsets.UTF_8);
    }

    public static String sha256(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(data));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 algorithm unavailable", ex);
        }
    }
}
