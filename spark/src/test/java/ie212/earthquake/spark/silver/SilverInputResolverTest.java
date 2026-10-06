package ie212.earthquake.spark.silver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SilverInputResolverTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void resolvesOnlyVerifiedObjectForExpectedRunAndReusesStage(@TempDir Path temp) throws Exception {
        Path bronze = temp.resolve("bronze");
        Path object = bronze.resolve("usgs/response.geojson");
        Files.createDirectories(object.getParent());
        byte[] bytes = "{\"type\":\"FeatureCollection\",\"features\":[]}".getBytes(StandardCharsets.UTF_8);
        Files.write(object, bytes);
        Path manifest = temp.resolve("manifest.json");
        writeManifest(manifest, bytes.length, digest(bytes), "BronzeReady", "run-1");

        SilverInputResolver resolver = new SilverInputResolver(new PathBronzeInputReader(bronze));
        ResolvedBronzeInput first = resolver.resolve(manifest, "run-1", "USGS", temp.resolve("staging"));
        ResolvedBronzeInput second = resolver.resolve(manifest, "run-1", "USGS", temp.resolve("staging"));

        assertEquals("run-1", first.runId());
        assertEquals("USGS", first.sourceSystem());
        assertTrue(Files.isRegularFile(first.stagedObject()));
        assertTrue(second.idempotentReuse());
        assertEquals(first.stagedObject(), second.stagedObject());
    }

    @Test
    void rejectsUnverifiedManifestOrWrongRun(@TempDir Path temp) throws Exception {
        Path bronze = temp.resolve("bronze");
        Files.createDirectories(bronze.resolve("usgs"));
        byte[] bytes = "raw".getBytes(StandardCharsets.UTF_8);
        Files.write(bronze.resolve("usgs/raw.geojson"), bytes);
        Path manifest = temp.resolve("manifest.json");
        writeManifest(manifest, bytes.length, digest(bytes), "Rejected", "run-1");

        SilverInputResolver resolver = new SilverInputResolver(new PathBronzeInputReader(bronze));
        assertThrows(java.io.IOException.class,
                () -> resolver.resolve(manifest, "run-1", "USGS", temp.resolve("staging")));
        writeManifest(manifest, bytes.length, digest(bytes), "BronzeReady", "run-2");
        assertThrows(java.io.IOException.class,
                () -> resolver.resolve(manifest, "run-1", "USGS", temp.resolve("staging")));
    }

    private static void writeManifest(Path path, long length, String sha, String status, String runId) throws Exception {
        ObjectNode root = JSON.createObjectNode();
        root.put("manifest_id", "manifest-1");
        root.put("bronze_status", status);
        root.put("source_system", "USGS");
        root.put("raw_object_uri", "s3://bucket/bronze/usgs/response.geojson");
        root.put("raw_object_key", "usgs/response.geojson");
        root.put("sha256", sha);
        root.put("content_length_bytes", length);
        root.put("run_id", runId);
        ObjectNode validation = root.putObject("validation");
        validation.put("object_write_completed", true);
        validation.put("raw_readback_verified", true);
        validation.put("checksum_verified", true);
        validation.put("source_structure_valid", true);
        validation.put("manifest_consistent", true);
        Files.writeString(path, JSON.writeValueAsString(root));
    }

    private static String digest(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
