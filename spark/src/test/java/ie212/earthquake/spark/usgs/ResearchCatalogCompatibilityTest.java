package ie212.earthquake.spark.usgs;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchCatalogCompatibilityTest {
    @TempDir Path root;
    Path jar(String name,Map<String,String> files) throws Exception {
        Path path=root.toRealPath().resolve(name);
        try(var zip=new ZipOutputStream(Files.newOutputStream(path))){for(var e:files.entrySet()) {
            zip.putNextEntry(new ZipEntry(e.getKey()));zip.write(e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));zip.closeEntry();}}
        return path;
    }
    @Test void onlyReviewedCatalogAndProofCacheCodeCanChange() throws Exception {
        String allowed=ResearchCatalogCompatibility.CHANGED.iterator().next();
        var old=new HashMap<>(Map.of(allowed,"old","fixed/Parser.class","parser"));var current=new HashMap<>(old);current.put(allowed,"new");
        current.put("ie212/earthquake/spark/usgs/ResearchCatalogBatchPlan.class","batch");
        var proof=ResearchCatalogCompatibility.verify(jar("old.jar",old),jar("current.jar",current));
        assertEquals(1,proof.path("matched_original_entries").asInt());assertEquals(1,proof.path("changed_entries").size());assertEquals(1,proof.path("added_entries").size());
    }
    @Test void changedParserPublisherDependencyOrUnreviewedAddedClassFails() throws Exception {
        var old=Map.of("fixed/Parser.class","parser");var original=jar("old.jar",old);
        assertThrows(Exception.class,()->ResearchCatalogCompatibility.verify(original,jar("changed.jar",Map.of("fixed/Parser.class","changed"))));
        assertThrows(Exception.class,()->ResearchCatalogCompatibility.verify(original,jar("added.jar",Map.of("fixed/Parser.class","parser","New.class","unreviewed"))));
        assertThrows(Exception.class,()->ResearchCatalogCompatibility.verify(original,jar("missing.jar",Map.of("Other.class","x"))));
    }
}
