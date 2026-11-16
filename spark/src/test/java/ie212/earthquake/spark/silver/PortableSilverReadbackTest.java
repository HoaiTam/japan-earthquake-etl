package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.zip.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PortableSilverReadbackTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir Path temp;

    private ObjectNode fixture() throws Exception {
        return fixture(List.of(), List.of(), List.of());
    }
    private ObjectNode fixture(List<SilverObservation> observations, List<SilverCanonicalMembership> memberships, List<SilverSourceLink> links) throws Exception {
        temp = temp.toRealPath();
        var store = MinioSilverObjectStore.inMemory("japan-earthquake", "silver");
        var context = JSON.createObjectNode().put("quality_passed", true).put("reconciliation_balanced", true);
        context.putArray("affected_months").add("2023-01");
        new SilverBundlePublisher(store).publish(new SilverWriteRequest("portable-test", observations, List.of(), links, memberships, Instant.parse("2026-01-01T00:00:00Z"), false), context);
        Path archivePath = temp.resolve("payloads.zip");
        try (var zip = new ZipOutputStream(Files.newOutputStream(archivePath))) {
            for (String key : store.list("bundles/portable-test")) {
                if (!key.endsWith(".parquet")) continue;
                zip.putNextEntry(new ZipEntry(key)); zip.write(store.read(key)); zip.closeEntry();
            }
        }
        String archiveSha = SilverBundlePublisher.sha(Files.readAllBytes(archivePath));
        byte[] bundleBytes = store.read("bundles/portable-test/manifest.json");
        JsonNode bundle = JSON.readTree(bundleBytes);
        var plan = JSON.createObjectNode().put("version", "dat03-portable-silver-readback-v1").put("owner", "fixture")
                .put("mode", "READ_ONLY_NO_OBJECT_STORE_OR_GOLD_WRITES");
        plan.putArray("affected_months").add("2023-01");
        plan.putArray("archives").addObject().put("path", archivePath.toString()).put("sha256", archiveSha);
        var group = plan.putArray("groups").addObject().put("run_id", "portable-test")
                .put("bundle_sha256", SilverBundlePublisher.sha(bundleBytes)).put("bundle_base64", Base64.getEncoder().encodeToString(bundleBytes));
        var datasets = group.putObject("datasets");
        for (String name : List.of("source_observation", "reject_record", "source_link", "canonical_membership")) {
            byte[] mf = store.read("bundles/portable-test/" + name + "/manifest.json");
            var descriptor = datasets.putObject(name).put("manifest_base64", Base64.getEncoder().encodeToString(mf));
            var sources = descriptor.putArray("sources");
            for (JsonNode file : JSON.readTree(mf).path("files")) sources.addObject()
                    .put("uri", file.path("uri").asText()).put("archive_sha256", archiveSha).put("member", file.path("relative_path").asText());
        }
        assertEquals(4, bundle.path("datasets").size());
        return plan;
    }

    private JsonNode verify(ObjectNode plan) throws Exception {
        Path path = temp.resolve("plan-" + UUID.randomUUID() + ".json");
        byte[] bytes = JSON.writeValueAsBytes(plan); Files.write(path, bytes);
        return PortableSilverReadback.verify(path, SilverBundlePublisher.sha(bytes));
    }

    @Test void emptyFourDatasetsRemainReadOnlyAndNotPublished() throws Exception {
        var result = verify(fixture());
        assertEquals(1, result.path("groups").size()); assertEquals(0, result.path("rows_decoded").asLong());
        for (String field : List.of("gold_published", "portable_import_ready", "official_export_ready", "full_transitive_closure_verified", "s3_keys_verified")) assertFalse(result.path(field).asBoolean());
    }
    @Test void wrongPlanPinFails() throws Exception {
        var plan = fixture(); Path path = temp.resolve("p.json"); Files.write(path, JSON.writeValueAsBytes(plan));
        assertTrue(assertThrows(Exception.class, () -> PortableSilverReadback.verify(path, "0".repeat(64))).getMessage().contains("PLAN_PIN_MISMATCH"));
    }
    @Test void wrongArchivePinFails() throws Exception {
        var plan = fixture(); ((ObjectNode) plan.path("archives").get(0)).put("sha256", "0".repeat(64));
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("ARCHIVE_PIN_MISMATCH"));
    }
    @Test void duplicateMonthsFail() throws Exception {
        var plan = fixture(); plan.withArray("affected_months").add("2023-01");
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("DUPLICATE_MONTH"));
    }
    @Test void mutatedBundleFails() throws Exception {
        var plan = fixture(); ((ObjectNode) plan.path("groups").get(0)).put("bundle_sha256", "0".repeat(64));
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("BUNDLE_PIN_MISMATCH"));
    }
    @Test void traversalMemberFailsWithoutExtraction() throws Exception {
        var plan = fixture(); ((ObjectNode) plan.path("groups").get(0).path("datasets").path("source_observation").path("sources").get(0)).put("member", "../other.parquet");
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("UNSAFE_ZIP_MEMBER"));
    }
    @Test void missingDatasetFails() throws Exception {
        var plan = fixture(); ((ObjectNode) plan.path("groups").get(0).path("datasets")).remove("source_link");
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("FOUR_DATASETS_REQUIRED"));
    }
    @Test void affectedMonthPlanCannotHideSourceScope() throws Exception {
        var plan = fixture(); plan.withArray("affected_months").removeAll().add("2023-02");
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("AFFECTED_MONTHS_MISMATCH"));
    }
    private SilverObservation observation() {
        var time = Instant.parse("2023-01-01T00:00:00Z");
        return new SilverObservation("1.0", "observation-1", "JMA_BULLETIN", "record-1", "revision-1", time, null, null,
                true, time, java.time.LocalDateTime.ofInstant(time, java.time.ZoneId.of("Asia/Tokyo")),
                java.time.LocalDate.of(2023, 1, 1), java.time.LocalDate.of(2023, 1, 1), 2023, 1, 35.0, 139.0,
                10.0, 5.0, "Mj", "EARTHQUAKE", "fixture", null, null, null, null, null, null, null, null,
                true, "VALID", List.of(), "bronze-1", "s3://japan-earthquake/bronze/fixture", "a".repeat(64),
                "line-1", "b".repeat(64), "ingest-1", "fixture-parser", "1", time);
    }
    private SilverCanonicalMembership membership() {
        return new SilverCanonicalMembership("canonical-1", "observation-1", "PRIMARY", null, "model-1", Instant.parse("2023-01-01T00:00:00Z"));
    }
    @Test void realPagesAndCanonicalMembershipAreDecoded() throws Exception {
        var r = verify(fixture(List.of(observation()), List.of(membership()), List.of()));
        assertEquals(2, r.path("rows_decoded").asInt());
        assertEquals(1, r.path("groups").get(0).path("canonical_count").asInt());
    }
    @Test void missingMembershipIsBlocked() throws Exception {
        var plan = fixture(List.of(observation()), List.of(), List.of());
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("CANONICAL_MEMBERSHIP_UNBALANCED"));
    }
    @Test void duplicateCurrentObservationIsBlocked() throws Exception {
        var plan = fixture(List.of(observation(), observation()), List.of(membership()), List.of());
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("DUPLICATE_CURRENT_OBSERVATION"));
    }
    @Test void linkToAbsentObservationIsBlocked() throws Exception {
        var link = new SilverSourceLink("link-1", "observation-1", "missing", 0.0, 0.0, null, null, 1.0,
                "ACCEPTED", List.of(), "model-1", Instant.parse("2023-01-01T00:00:00Z"));
        var plan = fixture(List.of(observation()), List.of(membership()), List.of(link));
        assertTrue(assertThrows(Exception.class, () -> verify(plan)).getMessage().contains("SOURCE_LINK_REFERENCE_MISSING"));
    }
}
