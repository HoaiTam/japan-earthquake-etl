package ie212.earthquake.spark.gold;

import java.nio.file.*;
import java.util.*;
import java.util.zip.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class PreparedGoldContinuationTest {
    @TempDir Path root;
    private static final String P = "ie212/earthquake/spark/gold/";
    private Path jar(String name, Map<String,String> entries) throws Exception {
        Path path = root.resolve(name);
        try (var out = new ZipOutputStream(Files.newOutputStream(path))) {
            for (var e : entries.entrySet()) { out.putNextEntry(new ZipEntry(P + e.getKey() + ".class")); out.write(e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8)); out.closeEntry(); }
        }
        return path;
    }
    private Map<String,String> original() { return Map.of("GoldIcebergWriter", "old", "PortableGoldPublicationJob", "old", "Transformer", "unchanged"); }
    private Map<String,String> next() { return new HashMap<>(Map.of("GoldIcebergWriter", "aligned", "PortableGoldPublicationJob", "explicit-continuation", "Transformer", "unchanged", "PreparedGoldContinuation", "validator")); }
    @Test void exactWriterAndEntryAdapterOnly() throws Exception {
        assertDoesNotThrow(() -> PreparedGoldContinuation.compatibility(jar("old.jar", original()), jar("new.jar", next())));
    }
    @Test void changedTransformerFailsClosed() throws Exception {
        var n = next(); n.put("Transformer", "changed");
        assertThrows(Exception.class, () -> PreparedGoldContinuation.compatibility(jar("old.jar", original()), jar("new.jar", n)));
    }
    @Test void arbitraryAddedClassFailsClosed() throws Exception {
        var n = next(); n.put("Bypass", "extra");
        assertThrows(Exception.class, () -> PreparedGoldContinuation.compatibility(jar("old.jar", original()), jar("new.jar", n)));
    }
    @Test void removedOriginalClassFailsClosed() throws Exception {
        var n = next(); n.remove("Transformer");
        assertThrows(Exception.class, () -> PreparedGoldContinuation.compatibility(jar("old.jar", original()), jar("new.jar", n)));
    }
    @Test void unchangedOldRunnerIsNotRecovery() throws Exception {
        assertThrows(Exception.class, () -> PreparedGoldContinuation.compatibility(jar("old.jar", original()), jar("new.jar", original())));
    }
}
