package ie212.earthquake.spark.gold;

import ie212.earthquake.spark.silver.*;
import java.nio.file.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PortablePreparedCatalogTest {
    @Test void persistedReceiptsCompareExactBytesWithoutJsonIntegerWidthDrift() throws Exception {
        Path root = Files.createTempDirectory("prepared-json-width").toRealPath(); Path file = root.resolve("receipt.json");
        var receipt = PortablePreparedCatalog.JSON.createObjectNode().put("bytes", 123L);
        PortablePreparedCatalog.save(file, receipt);
        assertTrue(PortablePreparedCatalog.persistedJsonEquals(file, receipt));
        assertFalse(PortablePreparedCatalog.persistedJsonEquals(file, receipt.deepCopy().put("bytes", 124L)));
    }
    @Test void exactParquetFooterPinsAreReadWithoutStartingSpark() throws Exception {
        Path root = Files.createTempDirectory("prepared-catalog-fixture").toRealPath(); Path file = root.resolve("part.parquet");
        Files.write(file, new SilverParquetSerializer().serializeObservations(List.of()));
        var descriptor = PortablePreparedCatalog.JSON.createObjectNode().put("row_count", 0);
        descriptor.putArray("files").add(PortablePreparedCatalog.reference(root, file).put("row_count", 0));
        assertEquals(0, PortablePreparedCatalog.verifyDataset(root, descriptor));
        var wrong = descriptor.deepCopy(); wrong.put("row_count", 1); ((com.fasterxml.jackson.databind.node.ObjectNode) wrong.path("files").get(0)).put("row_count", 1);
        assertThrows(Exception.class, () -> PortablePreparedCatalog.verifyDataset(root, wrong));
        byte[] raw = Files.readAllBytes(file); raw[raw.length / 2] ^= 1; Files.write(file, raw);
        assertThrows(Exception.class, () -> PortablePreparedCatalog.verifyDataset(root, descriptor));
    }
    @Test void unsafePathsAndSelfDeclaredReadinessAreNotAccepted() throws Exception {
        Path root = Files.createTempDirectory("prepared-path-fixture").toRealPath();
        for (String path : List.of("../outside.parquet", "/tmp/outside", "a//b", "https://example.com/data"))
            assertThrows(Exception.class, () -> PortablePreparedCatalog.relative(root, path));
        Files.writeString(root.resolve("catalog.json"), "{\"version\":\"dat03-prepared-catalog-v1\",\"status\":\"Published\"}");
        assertThrows(Exception.class, () -> PortablePreparedCatalog.verify(root, PortableInputMaterializer.fileSha(root.resolve("catalog.json"))));
    }
}
