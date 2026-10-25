package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JmaArchiveInventoryTest {
    @Test
    void researchBoundaryIsExplicitAndDoesNotChangeBaselineFortyYears() throws Exception {
        var baselinePath = Path.of("../config/jma/hypocenter_archives_v1.csv");
        var baseline = JmaArchiveInventory.read(baselinePath);
        var boundary = JmaArchiveInventory.read(Path.of("../config/jma/research_boundary_archives_v1.csv"));
        assertEquals("61dc2c73f1bb9cee48fba851cbb941cc62c893201c36db583cdd55ae19f8199a",
                ie212.earthquake.spark.silver.SourceKeyGenerator.sha256(Files.readAllBytes(baselinePath)));
        assertEquals(41, baseline.size());
        assertEquals(2023, baseline.get(baseline.size() - 1).year());
        assertEquals(1, boundary.size());
        assertEquals("dat03-boundary-1.0", boundary.get(0).inventoryVersion());
        assertEquals(2024, boundary.get(0).year());
        assertEquals("h2024", boundary.get(0).memberName());
        assertEquals("2024-01-01T00:00:00+09:00", boundary.get(0).nativeStartJst());
        assertEquals(8197026L, boundary.get(0).observedContentLengthBytes());
    }

    @Test
    void readsPinnedColumnsAndKeeps1997Segments(@TempDir Path temp) throws Exception {
        Path csv = temp.resolve("inventory.csv");
        String header = "inventory_version,year,segment,native_start_jst,native_end_jst,archive_name,member_name,source_url,observed_release_hint,observed_last_modified_utc,observed_content_length_bytes,media_type,record_format,catalog_era\n";
        String row1 = "1.0,1997,jan-sep,1997-01-01T00:00:00+09:00,1997-10-01T00:00:00+09:00,h199701.zip,h199701,https://example.invalid/h199701.zip,lm-a,2025-01-01T00:00:00Z,1,application/zip,jma-hypocenter-96-byte-v1,LEGACY\n";
        String row2 = "1.0,1997,oct-dec,1997-10-01T00:00:00+09:00,1998-01-01T00:00:00+09:00,h199710.zip,h199710,https://example.invalid/h199710.zip,lm-b,2025-01-02T00:00:00Z,2,application/zip,jma-hypocenter-96-byte-v1,UNIFIED\n";
        Files.writeString(csv, header + row1 + row2);

        List<JmaArchiveEntry> entries = JmaArchiveInventory.read(csv);

        assertEquals(2, entries.size());
        assertEquals("jan-sep", entries.get(0).segment());
        assertEquals("UNIFIED", entries.get(1).catalogEra());
    }
}
