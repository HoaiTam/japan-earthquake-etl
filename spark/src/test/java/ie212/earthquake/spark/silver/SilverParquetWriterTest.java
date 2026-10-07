package ie212.earthquake.spark.silver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SilverParquetWriterTest {

    private static SilverObservation sampleObservation(
            String id,
            String sourceSystem,
            Instant eventTimeUtc,
            Double depthKm,
            Double magnitude,
            String qualityStatus,
            List<String> qualityFlags) {
        int year = eventTimeUtc.atZone(ZoneOffset.UTC).getYear();
        int month = eventTimeUtc.atZone(ZoneOffset.UTC).getMonthValue();
        LocalDate eventDateUtc = eventTimeUtc.atZone(ZoneOffset.UTC).toLocalDate();
        LocalDateTime eventTimeJst = eventTimeUtc.atZone(ZoneOffset.ofHours(9)).toLocalDateTime();
        LocalDate eventDateJst = eventTimeJst.toLocalDate();

        return new SilverObservation(
                "1.0",
                "obs_" + id,
                sourceSystem,
                id,
                "rev_" + id,
                eventTimeUtc.plusSeconds(300),
                sourceSystem.equals("JMA_BULLETIN") ? "rel-2023" : null,
                sourceSystem.equals("JMA_BULLETIN") ? eventTimeUtc : null,
                true,
                eventTimeUtc,
                eventTimeJst,
                eventDateUtc,
                eventDateJst,
                year,
                month,
                35.6762,
                139.6503,
                depthKm,
                magnitude,
                "mw",
                "EARTHQUAKE",
                "Near Tokyo",
                false,
                "green",
                120,
                sourceSystem.equals("JMA_BULLETIN") ? "3" : null,
                sourceSystem.equals("JMA_BULLETIN") ? "J" : null,
                sourceSystem.equals("JMA_BULLETIN") ? "UNIFIED" : null,
                "reviewed",
                "https://earthquake.usgs.gov/earthquakes/eventpage/" + id,
                true,
                qualityStatus != null ? qualityStatus : "VALID",
                qualityFlags != null ? qualityFlags : List.of(),
                "manifest-" + id,
                "s3://bucket/bronze/" + sourceSystem.toLowerCase() + "/" + id + ".json",
                "sha256-dummy-" + id,
                "feature-0",
                "rawhash-" + id,
                "run-" + id,
                sourceSystem.equals("USGS") ? "usgs-geojson" : "jma-fixed-width",
                "1.0",
                Instant.parse("2023-09-16T12:00:00Z"));
    }

    private static SilverRejectRecord sampleReject(String id, String sourceSystem, String reasonCode) {
        return new SilverRejectRecord(
                "1.0",
                sourceSystem,
                id,
                "manifest-rej-" + id,
                "s3://bucket/bronze/" + sourceSystem.toLowerCase() + "/rej-" + id + ".bin",
                "sha256-rej-" + id,
                "line-1",
                "rawhash-rej-" + id,
                "PARSE",
                List.of(reasonCode),
                "run-rej-" + id,
                "1.0",
                Instant.parse("2023-09-16T12:00:00Z"));
    }

    @Test
    void testPartitionKeyFormatAndParsing() {
        SilverPartitionKey key = new SilverPartitionKey(2023, 9, "USGS");
        assertEquals("event_year_utc=2023/event_month_utc=09/source_system=USGS", key.partitionPath());

        SilverPartitionKey parsed = SilverPartitionKey.parse("event_year_utc=2023/event_month_utc=09/source_system=USGS");
        assertEquals(key, parsed);

        SilverPartitionKey parsedUnpadded = SilverPartitionKey.parse("event_year_utc=2023/event_month_utc=9/source_system=USGS");
        assertEquals(key, parsedUnpadded);

        assertThrows(IllegalArgumentException.class, () -> SilverPartitionKey.parse("invalid/path"));
        assertThrows(IllegalArgumentException.class, () -> new SilverPartitionKey(1800, 5, "USGS"));
        assertThrows(IllegalArgumentException.class, () -> new SilverPartitionKey(2023, 13, "USGS"));
    }

    @Test
    void testPartitionMatchesEventTimeUtc() {
        Instant time = Instant.parse("2023-09-15T10:00:00Z");
        SilverObservation obs = sampleObservation("usgs01", "USGS", time, 10.0, 5.2, "VALID", List.of());
        SilverPartitionKey key = SilverPartitionKey.from(obs);

        assertEquals(2023, key.eventYearUtc());
        assertEquals(9, key.eventMonthUtc());
        assertEquals("USGS", key.sourceSystem());
    }

    @Test
    void testPartitionMismatchThrowsFast() {
        Instant time = Instant.parse("2023-09-15T10:00:00Z");
        // Incorrectly constructed observation where event_month_utc is 8 instead of 9
        SilverObservation mismatched = new SilverObservation(
                "1.0", "obs_bad", "USGS", "bad", "rev_bad", null, null, null, true,
                time, time.atZone(ZoneOffset.ofHours(9)).toLocalDateTime(),
                LocalDate.of(2023, 9, 15), LocalDate.of(2023, 9, 15),
                2023, 8, // MISMATCH: month is 8 instead of 9
                35.0, 139.0, 10.0, 5.0, "mw", "EARTHQUAKE", null, false, null, null,
                null, null, null, null, null, true, "VALID", List.of(),
                "m1", "s3://b/1", "sha1", "loc1", "hash1", "run1", "p1", "v1", Instant.now());

        assertThrows(IllegalArgumentException.class, () -> SilverPartitionKey.from(mismatched));
    }

    @Test
    void testAtomicStagingAndPublishSuccessMarker(@TempDir Path tempDir) throws Exception {
        FileSilverObjectStore store = new FileSilverObjectStore(tempDir);
        SilverParquetWriter writer = new SilverParquetWriter(store);

        Instant time = Instant.parse("2023-09-16T08:30:00Z");
        SilverObservation obs = sampleObservation("usgs01", "USGS", time, 15.0, 6.1, "VALID", List.of());

        SilverWriteRequest request = SilverWriteRequest.of("run-success-01", List.of(obs));
        SilverWriteResult result = writer.write(request);

        assertEquals("SilverReady", result.silverStatus());
        assertEquals("run-success-01", result.runId());
        assertEquals(1, result.totalObservations());
        assertEquals(0, result.totalRejects());
        assertFalse(result.idempotentReuse());
        assertEquals(1, result.publishedPartitions().size());

        SilverPartitionManifest manifest = result.publishedPartitions().get(0);
        assertEquals(2023, manifest.eventYearUtc());
        assertEquals(9, manifest.eventMonthUtc());
        assertEquals("USGS", manifest.sourceSystem());
        assertEquals(1, manifest.recordCount());

        // Check file hierarchy
        String partitionDir = "source_observation/event_year_utc=2023/event_month_utc=09/source_system=USGS";
        String parquetFile = partitionDir + "/part-00000-run-success-01.parquet";
        String successMarker = partitionDir + "/_SUCCESS";
        String manifestFile = partitionDir + "/manifest.json";

        assertTrue(store.exists(parquetFile), "Parquet file should exist");
        assertTrue(store.exists(successMarker), "_SUCCESS marker should exist");
        assertTrue(store.exists(manifestFile), "manifest.json should exist");

        // Verify _SUCCESS marker content
        String successContent = new String(store.read(successMarker));
        assertTrue(successContent.contains("status=SUCCESS"));
        assertTrue(successContent.contains("run_id=run-success-01"));

        // Verify manifest roundtrip
        byte[] manifestBytes = store.read(manifestFile);
        SilverPartitionManifest readManifest = SilverPartitionManifest.fromJson(manifestBytes);
        assertEquals("SilverReady", readManifest.silverStatus());
        assertEquals(1, readManifest.recordCount());
        assertEquals(1, readManifest.files().size());
        assertEquals("part-00000-run-success-01.parquet", readManifest.files().get(0).fileName());
        assertEquals(manifest.files().get(0).sha256(), readManifest.files().get(0).sha256());

        // Verify staging area cleaned up
        assertFalse(store.exists("_staging/run-success-01"));
    }

    @Test
    void testNoPartialOutputOnFailure(@TempDir Path tempDir) throws Exception {
        FailingSilverObjectStore failingStore = new FailingSilverObjectStore(tempDir);
        SilverParquetWriter writer = new SilverParquetWriter(failingStore);

        Instant time = Instant.parse("2023-09-16T08:30:00Z");
        SilverObservation obs = sampleObservation("usgs01", "USGS", time, 15.0, 6.1, "VALID", List.of());

        failingStore.failOnMove = true; // Inject move failure during atomic promotion
        SilverWriteRequest request = SilverWriteRequest.of("run-fail-01", List.of(obs));

        assertThrows(IOException.class, () -> writer.write(request));

        // Acceptance criterion: No partial output in target partition, and no _SUCCESS marker
        String partitionDir = "source_observation/event_year_utc=2023/event_month_utc=09/source_system=USGS";
        assertFalse(failingStore.exists(partitionDir + "/_SUCCESS"), "No _SUCCESS on failure");
        assertFalse(failingStore.exists(partitionDir + "/manifest.json"), "No manifest on failure");
        assertFalse(failingStore.exists(partitionDir + "/part-00000-run-fail-01.parquet"), "No partial parquet on failure");
    }

    @Test
    void testRerunIdempotentReuse(@TempDir Path tempDir) throws Exception {
        FileSilverObjectStore store = new FileSilverObjectStore(tempDir);
        SilverParquetWriter writer = new SilverParquetWriter(store);

        Instant time = Instant.parse("2023-09-16T08:30:00Z");
        SilverObservation obs = sampleObservation("usgs01", "USGS", time, 15.0, 6.1, "VALID", List.of());
        SilverWriteRequest request = SilverWriteRequest.of("run-idempotent-01", List.of(obs));

        SilverWriteResult first = writer.write(request);
        assertFalse(first.idempotentReuse());

        SilverWriteResult second = writer.write(request);
        assertTrue(second.idempotentReuse());
        assertEquals(first.publishedPartitions().get(0).files().get(0).sha256(),
                second.publishedPartitions().get(0).files().get(0).sha256());
    }

    @Test
    void testRetryOverwritesPartitionWithoutAppendingDuplicate(@TempDir Path tempDir) throws Exception {
        FileSilverObjectStore store = new FileSilverObjectStore(tempDir);
        SilverParquetWriter writer = new SilverParquetWriter(store);

        Instant time = Instant.parse("2023-09-16T08:30:00Z");
        SilverObservation obs1 = sampleObservation("usgs01", "USGS", time, 15.0, 6.1, "VALID", List.of());
        SilverWriteRequest run1 = SilverWriteRequest.of("run-retry-01", List.of(obs1));
        writer.write(run1);

        String partitionDir = "source_observation/event_year_utc=2023/event_month_utc=09/source_system=USGS";
        assertTrue(store.exists(partitionDir + "/part-00000-run-retry-01.parquet"));

        // Retry with run-retry-02 having an updated revision of the observation
        SilverObservation obs2 = sampleObservation("usgs01", "USGS", time, 12.0, 6.3, "VALID", List.of());
        SilverWriteRequest run2 = SilverWriteRequest.of("run-retry-02", List.of(obs2));
        SilverWriteResult result2 = writer.write(run2);

        assertFalse(result2.idempotentReuse());
        assertEquals(1, result2.publishedPartitions().get(0).recordCount());

        // Verify that run1's parquet file was replaced, not duplicated
        assertFalse(store.exists(partitionDir + "/part-00000-run-retry-01.parquet"), "Old file replaced");
        assertTrue(store.exists(partitionDir + "/part-00000-run-retry-02.parquet"), "New file present");

        List<String> files = store.list(partitionDir);
        // files should only contain: part-00000-run-retry-02.parquet, _SUCCESS, manifest.json
        assertEquals(3, files.size(), "Partition contains exactly 1 parquet file plus markers");
    }

    @Test
    void testPreservesNullMagnitudeAndDepthAndQualityFlags(@TempDir Path tempDir) throws Exception {
        FileSilverObjectStore store = new FileSilverObjectStore(tempDir);
        SilverParquetWriter writer = new SilverParquetWriter(store);

        Instant time = Instant.parse("2023-09-16T08:30:00Z");
        SilverObservation obsWithNulls = sampleObservation(
                "usgs-nulls", "USGS", time, null, null, "WARNING", List.of("SUSPICIOUS_DEPTH", "MISSING_MAGNITUDE"));

        SilverWriteRequest request = SilverWriteRequest.of("run-nulls-01", List.of(obsWithNulls));
        SilverWriteResult result = writer.write(request);

        assertEquals(1, result.publishedPartitions().size());
        SilverPartitionManifest manifest = result.publishedPartitions().get(0);
        assertEquals(0, manifest.qualitySummary().get("valid_count"));
        assertEquals(1, manifest.qualitySummary().get("warning_count"));

        String parquetPath = "source_observation/event_year_utc=2023/event_month_utc=09/source_system=USGS/part-00000-run-nulls-01.parquet";
        byte[] parquetBytes = store.read(parquetPath);
        assertTrue(parquetBytes.length > 0);

        // Verify parquet bytes
        SilverParquetSerializer.verifyParquet(
                parquetBytes, 1, SilverParquetSerializer.OBSERVATION_PARQUET_SCHEMA);
    }

    @Test
    void testMultiPartitionAndMultiSourceRouting(@TempDir Path tempDir) throws Exception {
        FileSilverObjectStore store = new FileSilverObjectStore(tempDir);
        SilverParquetWriter writer = new SilverParquetWriter(store);

        Instant sept = Instant.parse("2023-09-16T08:30:00Z");
        Instant oct = Instant.parse("2023-10-05T14:20:00Z");

        SilverObservation obsUsgsSept = sampleObservation("u1", "USGS", sept, 10.0, 4.5, "VALID", List.of());
        SilverObservation obsUsgsOct = sampleObservation("u2", "USGS", oct, 12.0, 5.0, "VALID", List.of());
        SilverObservation obsJmaSept = sampleObservation("j1", "JMA_BULLETIN", sept, 20.0, 4.8, "VALID", List.of());

        SilverWriteRequest request = SilverWriteRequest.of("run-multi-01", List.of(obsUsgsSept, obsUsgsOct, obsJmaSept));
        SilverWriteResult result = writer.write(request);

        assertEquals(3, result.publishedPartitions().size());

        assertTrue(store.exists("source_observation/event_year_utc=2023/event_month_utc=09/source_system=USGS/_SUCCESS"));
        assertTrue(store.exists("source_observation/event_year_utc=2023/event_month_utc=10/source_system=USGS/_SUCCESS"));
        assertTrue(store.exists("source_observation/event_year_utc=2023/event_month_utc=09/source_system=JMA_BULLETIN/_SUCCESS"));
    }

    @Test
    void testWritesRejectRecordsCorrectly(@TempDir Path tempDir) throws Exception {
        FileSilverObjectStore store = new FileSilverObjectStore(tempDir);
        SilverParquetWriter writer = new SilverParquetWriter(store);

        Instant time = Instant.parse("2023-09-16T08:30:00Z");
        SilverObservation obs = sampleObservation("usgs01", "USGS", time, 10.0, 5.0, "VALID", List.of());
        SilverRejectRecord rej = sampleReject("bad01", "USGS", "INVALID_LATITUDE");

        SilverWriteRequest request = SilverWriteRequest.of("run-rej-01", List.of(obs), List.of(rej));
        SilverWriteResult result = writer.write(request);

        assertEquals(1, result.totalObservations());
        assertEquals(1, result.totalRejects());
        assertEquals(1, result.rejectFiles().size());

        String rejectFile = "reject_record/source_system=USGS/run_id=run-rej-01/part-00000-run-rej-01.parquet";
        assertTrue(store.exists(rejectFile));
        assertTrue(store.exists("reject_record/source_system=USGS/run_id=run-rej-01/_SUCCESS"));

        byte[] rejectBytes = store.read(rejectFile);
        SilverParquetSerializer.verifyParquet(rejectBytes, 1, SilverParquetSerializer.REJECT_PARQUET_SCHEMA);
    }

    // Helper failing store to simulate atomic staging failure
    private static final class FailingSilverObjectStore implements SilverObjectStore {
        private final FileSilverObjectStore delegate;
        boolean failOnMove = false;

        FailingSilverObjectStore(Path root) {
            this.delegate = new FileSilverObjectStore(root);
        }

        @Override public void put(String key, byte[] content, String contentType) throws IOException { delegate.put(key, content, contentType); }
        @Override public byte[] read(String key) throws IOException { return delegate.read(key); }
        @Override public boolean exists(String key) throws IOException { return delegate.exists(key); }
        @Override public List<String> list(String prefix) throws IOException { return delegate.list(prefix); }
        @Override public void delete(String key) throws IOException { delegate.delete(key); }
        @Override public void deletePrefix(String prefix) throws IOException { delegate.deletePrefix(prefix); }
        @Override
        public void move(String sourceKey, String targetKey) throws IOException {
            if (failOnMove) {
                throw new IOException("Simulated disk error during promotion move!");
            }
            delegate.move(sourceKey, targetKey);
        }
        @Override public String uriForKey(String key) { return delegate.uriForKey(key); }
    }
}
