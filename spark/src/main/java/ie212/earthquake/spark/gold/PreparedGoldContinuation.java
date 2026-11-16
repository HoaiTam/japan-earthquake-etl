package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.zip.ZipFile;

/** Narrow continuation for the diagnosed dimension EXCEPT column-order bug; not a generic bypass. */
public final class PreparedGoldContinuation {
    public static final String VERSION = "dat03-dimension-column-alignment-v1";
    private static final Set<String> CHANGED = Set.of("ie212/earthquake/spark/gold/GoldIcebergWriter.class",
            "ie212/earthquake/spark/gold/PortableGoldPublicationJob.class");
    private static final Set<String> ADDED = Set.of("ie212/earthquake/spark/gold/PreparedGoldContinuation.class");
    private PreparedGoldContinuation() { }
    private static Map<String,String> classes(Path file) throws Exception {
        var out = new TreeMap<String,String>();
        try (var zip = new ZipFile(file.toFile())) {
            var entries = zip.entries();
            while (entries.hasMoreElements()) {
                var e = entries.nextElement(); String name = e.getName();
                if (!name.startsWith("ie212/earthquake/spark/") || !name.endsWith(".class")) continue;
                try (var stream = zip.getInputStream(e)) {
                    String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(stream.readAllBytes()));
                    if (out.put(name, digest) != null) throw new java.io.IOException("DUPLICATE_RECOVERY_CLASS");
                }
            }
        }
        return out;
    }
    public static void compatibility(Path original, Path continuation) throws Exception {
        var old = classes(original); var next = classes(continuation);
        PortableImportContract.require(!old.isEmpty() && next.keySet().containsAll(old.keySet()), "RECOVERY_ORIGINAL_CLASSES_REQUIRED");
        var changed = new HashSet<String>();
        for (var item : old.entrySet()) if (!Objects.equals(item.getValue(), next.get(item.getKey()))) changed.add(item.getKey());
        var added = new HashSet<>(next.keySet()); added.removeAll(old.keySet());
        PortableImportContract.require(changed.equals(CHANGED) && added.equals(ADDED), "RECOVERY_CHANGED_TRANSFORM_OR_OTHER_COMPONENT");
    }
    public static JsonNode validate(Path root, Path file, String expected, JsonNode request, String requestSha, Path runner) throws Exception {
        file = ie212.earthquake.spark.silver.PortableInputMaterializer.safe(file);
        PortableImportContract.require(file.toString().matches(java.util.regex.Pattern.quote(root.toString()) + "/continuations/[a-f0-9]{32}/continuation.json")
                && ie212.earthquake.spark.silver.PortableInputMaterializer.fileSha(file).equals(expected), "PINNED_NARROW_CONTINUATION_REQUIRED");
        JsonNode c = PortablePreparedCatalog.read(file, 65536);
        PortableImportContract.require(VERSION.equals(c.path("version").asText()) && requestSha.equals(c.path("request_sha256").asText())
                && request.path("run_id").equals(c.path("run_id")) && request.path("namespace").equals(c.path("namespace"))
                && request.path("runner_sha256").equals(c.path("original_runner_sha256"))
                && ie212.earthquake.spark.silver.PortableInputMaterializer.fileSha(root.resolve("runner.jar")).equals(c.path("original_runner_sha256").asText())
                && runner.equals(file.getParent().resolve("runner.jar"))
                && ie212.earthquake.spark.silver.PortableInputMaterializer.fileSha(runner).equals(c.path("continuation_runner_sha256").asText()), "CONTINUATION_CONTEXT_OR_RUNNER_CHANGED");
        compatibility(root.resolve("runner.jar"), runner); return c;
    }
}
