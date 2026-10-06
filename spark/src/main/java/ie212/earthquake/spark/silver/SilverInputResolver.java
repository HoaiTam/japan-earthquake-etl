package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.time.Instant;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Resolves one exact Bronze manifest and stages its verified raw object.
 * The resolver never scans a prefix or chooses a "latest" object.
 */
public final class SilverInputResolver {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern SHA256 = Pattern.compile("[0-9a-fA-F]{64}");
    private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._~-]*");

    private final BronzeInputReader reader;

    public SilverInputResolver(BronzeInputReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    public ResolvedBronzeInput resolve(
            Path manifestPath,
            String expectedRunId,
            String expectedSourceSystem,
            Path stagingRoot)
            throws IOException {
        Objects.requireNonNull(manifestPath, "manifestPath");
        Objects.requireNonNull(expectedRunId, "expectedRunId");
        Objects.requireNonNull(expectedSourceSystem, "expectedSourceSystem");
        Objects.requireNonNull(stagingRoot, "stagingRoot");
        JsonNode manifest = JSON.readTree(Files.readAllBytes(manifestPath));
        require(manifest != null && manifest.isObject(), "manifest must be a JSON object");
        require("BronzeReady".equals(text(manifest, "bronze_status")),
                "only BronzeReady manifests may be resolved");
        require(expectedRunId.equals(text(manifest, "run_id")), "manifest run_id is outside this run");
        require(expectedSourceSystem.equals(text(manifest, "source_system")), "manifest source_system mismatch");
        requireBooleanValidation(manifest.path("validation"));

        String manifestId = required(manifest, "manifest_id");
        String rawUri = required(manifest, "raw_object_uri");
        String rawKey = required(manifest, "raw_object_key");
        String sha256 = required(manifest, "sha256");
        require(SHA256.matcher(sha256).matches(), "manifest sha256 is invalid");
        long expectedLength = manifest.path("content_length_bytes").asLong(-1L);
        require(expectedLength >= 0, "manifest content_length_bytes is invalid");
        String catalogRelease = nullableText(manifest, "catalog_release");

        byte[] payload = reader.read(rawKey, rawUri);
        require(payload.length == expectedLength, "Bronze object length does not match manifest");
        require(sha256(payload).equalsIgnoreCase(sha256), "Bronze object checksum does not match manifest");

        Path runRoot = stagingRoot.toAbsolutePath().normalize()
                .resolve(safe(expectedRunId)).resolve(safe(expectedSourceSystem));
        Files.createDirectories(runRoot);
        String fileName = Path.of(rawKey).getFileName().toString();
        require(!fileName.isBlank() && !fileName.equals(".") && !fileName.equals(".."),
                "manifest raw_object_key has no safe file name");
        Path stagedObject = runRoot.resolve(fileName).normalize();
        require(stagedObject.startsWith(runRoot), "staged path escapes the staging root");
        boolean reused = Files.isRegularFile(stagedObject);
        if (reused) {
            byte[] existing = Files.readAllBytes(stagedObject);
            reused = existing.length == payload.length && sha256(existing).equalsIgnoreCase(sha256);
        }
        if (!reused) {
            Path temporary = stagedObject.resolveSibling("." + fileName + ".part");
            Files.write(temporary, payload, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            moveAtomically(temporary, stagedObject);
        }

        ObjectNode staged = JSON.createObjectNode();
        staged.put("resolver_version", "slv-01-v1");
        staged.put("resolved_at_utc", Instant.now().toString());
        staged.put("manifest_id", manifestId);
        staged.put("source_system", expectedSourceSystem);
        staged.put("catalog_release", catalogRelease);
        staged.put("run_id", expectedRunId);
        staged.put("raw_object_uri", rawUri);
        staged.put("raw_object_key", rawKey);
        staged.put("sha256", sha256);
        staged.put("content_length_bytes", expectedLength);
        staged.put("staged_object", stagedObject.toString());
        Path stagedManifest = runRoot.resolve("staging-manifest.json");
        Path temporaryManifest = stagedManifest.resolveSibling(".staging-manifest.json.part");
        Files.writeString(temporaryManifest, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(staged) + "\n");
        moveAtomically(temporaryManifest, stagedManifest);
        return new ResolvedBronzeInput(manifestId, expectedSourceSystem, catalogRelease, expectedRunId,
                rawUri, rawKey, sha256, expectedLength, stagedObject, stagedManifest, reused);
    }

    private static void requireBooleanValidation(JsonNode validation) throws IOException {
        require(validation != null && validation.isObject(), "manifest validation is missing");
        String[] fields = {"object_write_completed", "raw_readback_verified", "checksum_verified", "source_structure_valid", "manifest_consistent"};
        for (String field : fields) {
            require(validation.path(field).isBoolean() && validation.path(field).asBoolean(),
                    "manifest validation failed: " + field);
        }
    }

    private static String required(JsonNode node, String field) throws IOException {
        String value = nullableText(node, field);
        require(value != null, "manifest field is missing: " + field);
        return value;
    }

    private static String nullableText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() || value.asText().isBlank() ? null : value.asText();
    }

    private static String text(JsonNode node, String field) {
        return nullableText(node, field);
    }

    private static void require(boolean condition, String message) throws IOException {
        if (!condition) {
            throw new IOException(message);
        }
    }

    private static String safe(String value) throws IOException {
        require(SAFE.matcher(value).matches(), "run/source value is not safe for staging");
        return value;
    }

    private static String sha256(byte[] payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(payload);
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("JVM does not provide SHA-256", exception);
        }
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
