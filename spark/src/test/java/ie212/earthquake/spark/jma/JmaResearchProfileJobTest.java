package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.*;
import ie212.earthquake.spark.silver.SourceKeyGenerator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JmaResearchProfileJobTest {
    @TempDir Path temp;
    private byte[] fixture() throws Exception {
        return Files.readAllBytes(Path.of("../tests/fixtures/jma/fixed-width/success.hyp"));
    }
    private Path zip(byte[] records) throws Exception {
        Path path = temp.resolve("h2023.zip");
        try (var zip = new ZipOutputStream(Files.newOutputStream(path))) {
            zip.putNextEntry(new ZipEntry("h2023")); zip.write(records); zip.closeEntry();
        }
        return path;
    }
    private com.fasterxml.jackson.databind.node.ObjectNode profile(Path path) throws Exception {
        return JmaResearchProfileJob.profile(path, "h2023", SourceKeyGenerator.sha256(Files.readAllBytes(path)));
    }
    @Test void fullStructuralCountWithoutCoverageOrPublicationClaims() throws Exception {
        var result = profile(zip(fixture()));
        assertEquals(2, result.path("counts").path("physical_records").asLong());
        assertEquals(2, result.path("counts").path("parser_accepted").asLong());
        assertEquals(1, result.path("counts").path("pre_mc_research_filter_rows").asLong());
        assertEquals(0, result.path("counts").path("pre_window_mainshock_rows").asLong());
        assertTrue(result.path("counts_reconciled").asBoolean());
        assertEquals("dat03-local-archive-v2", result.path("profile_version").asText());
        assertTrue(result.path("rejected_record_diagnostics").isEmpty());
        for (String flag : new String[]{"coverage_verified", "research_ready", "gold_published", "source_authenticity_verified"}) {
            assertFalse(result.path(flag).asBoolean());
        }
    }
    @Test void countsMainshockRuleWithoutClaimingMcOrCanonicalEligibility() throws Exception {
        String records = new String(fixture(), StandardCharsets.US_ASCII);
        records = records.substring(0, 44) + "08000" + records.substring(49, 52) + "60" + records.substring(54);
        var result = profile(zip(records.getBytes(StandardCharsets.US_ASCII)));
        assertEquals(1, result.path("counts").path("pre_window_mainshock_rows").asLong());
    }
    @Test void parserRejectsAreCountedNotSilentlyDropped() throws Exception {
        byte[] records = fixture(); records[21] = 'x';
        var result = profile(zip(records));
        assertEquals(1, result.path("counts").path("parser_rejected").asLong());
        assertEquals(1, result.path("parser_rejection_reason_counts").path("INVALID_LATITUDE").asLong());
        assertTrue(result.path("counts_reconciled").asBoolean());
        var diagnostic = result.path("rejected_record_diagnostics").get(0);
        assertEquals(1, diagnostic.path("physical_line").asInt());
        assertEquals(SourceKeyGenerator.sha256(java.util.Arrays.copyOf(records, 96)), diagnostic.path("record_sha256").asText());
    }
    @Test void depthNormalizationReportsExactLocatorHashAndValueWithoutRawRow() throws Exception {
        byte[] records = fixture();
        System.arraycopy("  0 4".getBytes(StandardCharsets.US_ASCII), 0, records, 44, 5);
        records[59] = '1';
        var result = profile(zip(records));
        assertEquals(0, result.path("counts").path("parser_rejected").asInt());
        var encoding = result.path("depth_blank_encoding_observations").get(0);
        assertEquals(1, encoding.path("physical_line").asInt());
        assertEquals("  0 4", encoding.path("native_depth_token").asText());
        assertEquals(0.04, encoding.path("depth_km").asDouble());
        assertEquals(SourceKeyGenerator.sha256(java.util.Arrays.copyOf(records, 96)), encoding.path("record_sha256").asText());
        assertFalse(encoding.has("raw_record"));
    }
    @Test void expectedHashAndMemberAreRequired() throws Exception {
        Path path = zip(fixture());
        assertThrows(Exception.class, () -> JmaResearchProfileJob.profile(path, "h2023", "0".repeat(64)));
        assertThrows(Exception.class, () -> JmaResearchProfileJob.profile(path, "../h2023", SourceKeyGenerator.sha256(Files.readAllBytes(path))));
    }
    @Test void malformedRecordAndSymlinkAreRejected() throws Exception {
        Path path = zip("bad\n".getBytes(StandardCharsets.US_ASCII));
        assertThrows(Exception.class, () -> profile(path));
        Path link = temp.resolve("link.zip"); Files.createSymbolicLink(link, path);
        assertThrows(Exception.class, () -> profile(link));
    }
    @Test void emptyArchiveIsNotProofOfSourceCoverage() throws Exception {
        var result = profile(zip(new byte[0]));
        assertEquals(0, result.path("counts").path("physical_records").asLong());
        assertTrue(result.path("first_accepted_event_utc").isNull());
        assertFalse(result.path("coverage_verified").asBoolean());
    }
}
