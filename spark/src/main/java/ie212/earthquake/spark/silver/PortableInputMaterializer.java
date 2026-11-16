package ie212.earthquake.spark.silver;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.zip.*;

/** Materialize one <=12-month verified batch into a NEW local scratch area, never source keys/S3. */
public final class PortableInputMaterializer {
    private static final ObjectMapper JSON = new ObjectMapper();
    public record GroupData(String runId, String bundleSha256, List<String> months, Map<String, List<String>> datasets) { }
    public record Materialized(List<GroupData> groups, JsonNode readback) { }
    private PortableInputMaterializer() { }
    private static void require(boolean ok, String message) throws IOException { if (!ok) throw new IOException(message); }
    public static String fileSha(Path path) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var stream = Files.newInputStream(path)) {
            byte[] block = new byte[1024 * 1024]; int n; while ((n = stream.read(block)) != -1) digest.update(block, 0, n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
    public static Path safe(Path path) throws IOException {
        path = path.toAbsolutePath().normalize();
        for (Path p = path; p != null; p = p.getParent()) require(!Files.isSymbolicLink(p), "NO_SYMLINK_PATHS");
        return path;
    }
    public static Materialized stage(Path planFile, String planSha, Path target) throws Exception {
        // Reuses page/schema/count/relationship verification; no caller-supplied readiness bools.
        JsonNode receipt = PortableSilverReadback.verify(planFile, planSha);
        JsonNode plan = JSON.readTree(Files.readAllBytes(safe(planFile)));
        target = safe(target); Files.createDirectories(target);
        var archives = new HashMap<String, ZipFile>(); var groups = new ArrayList<GroupData>();
        try {
            for (var a : plan.path("archives")) archives.put(a.path("sha256").asText(), new ZipFile(safe(Path.of(a.path("path").asText())).toFile()));
            for (var group : plan.path("groups")) {
                JsonNode bundle = JSON.readTree(Base64.getDecoder().decode(group.path("bundle_base64").asText()));
                var datasets = new TreeMap<String, List<String>>(); var months = new ArrayList<String>();
                bundle.path("context").path("affected_months").forEach(m -> months.add(m.asText()));
                for (String name : List.of("source_observation", "canonical_membership", "source_link", "reject_record")) {
                    JsonNode descriptor = group.path("datasets").path(name);
                    JsonNode manifest = JSON.readTree(Base64.getDecoder().decode(descriptor.path("manifest_base64").asText()));
                    var files = new ArrayList<String>();
                    for (int i = 0; i < manifest.path("files").size(); i++) {
                        JsonNode part = manifest.path("files").get(i), source = descriptor.path("sources").get(i);
                        String digest = part.path("sha256").asText(); require(digest.matches("[a-f0-9]{64}"), "EXACT_PART_SHA_REQUIRED");
                        long size = part.path("byte_size").asLong(); require(size > 0 && size <= 256L * 1024 * 1024, "BOUNDED_PART_REQUIRED");
                        ZipFile zip = archives.get(source.path("archive_sha256").asText()); ZipEntry entry = zip.getEntry(source.path("member").asText());
                        require(entry != null && entry.getSize() == size, "PART_CHANGED_AFTER_READBACK");
                        Path out = safe(target.resolve(digest + ".parquet"));
                        if (!Files.exists(out)) {
                            try (var stream = zip.getInputStream(entry); var dest = Files.newOutputStream(out, StandardOpenOption.CREATE_NEW)) {
                                byte[] block = new byte[1024 * 1024]; long copied = 0; int n;
                                while ((n = stream.read(block)) != -1) { copied += n; require(copied <= size, "PART_SIZE_CHANGED"); dest.write(block, 0, n); }
                                require(copied == size, "PART_TRUNCATED");
                            }
                        }
                        require(Files.size(out) == size && fileSha(out).equals(digest), "PART_SHA_CHANGED_AFTER_READBACK");
                        files.add(out.toString());
                    }
                    datasets.put(name, List.copyOf(files));
                }
                groups.add(new GroupData(group.path("run_id").asText(), group.path("bundle_sha256").asText(), List.copyOf(months), Map.copyOf(datasets)));
            }
            return new Materialized(List.copyOf(groups), receipt);
        } finally { for (var zip : archives.values()) zip.close(); }
    }
}
