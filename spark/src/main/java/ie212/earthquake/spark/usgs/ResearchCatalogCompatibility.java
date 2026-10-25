package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

/** Narrow continuation allowlist. Parser/canonical/dependency bytecode must remain unchanged;
 * verifier changes are explicit and restricted to the opt-in exact-bucket strategy. */
public final class ResearchCatalogCompatibility {
    public static final String VERSION="dat03-catalog-continuation-v1";
    private static final String BASE="ie212/earthquake/spark/";
    public static final Set<String> CHANGED=Set.of(BASE+"usgs/ResearchCatalogJob.class",BASE+"usgs/ResearchCatalogState.class",
            BASE+"orchestration/FullMonthBronzeReader.class",BASE+"orchestration/FullMonthStateResolver.class",BASE+"orchestration/RealEtlAdapter.class",
            BASE+"gold/GoldPublisher.class",BASE+"gold/GoldVerificationPlan.class");
    public static final Set<String> ADDED=Set.of(BASE+"usgs/ResearchCatalogBatchPlan.class",BASE+"usgs/ResearchCatalogCompatibility.class",
            BASE+"orchestration/FullMonthBronzeReader$JmaStructureCache.class");
    private ResearchCatalogCompatibility() { }
    public static ObjectNode verify(Path original,Path continuation) throws Exception {
        for(Path path:List.of(original,continuation))require(Files.isRegularFile(ResearchCatalogState.safe(path)) && Files.size(path)<=64L*1024*1024,"CATALOG_COMPATIBLE_JARS_REQUIRED");
        var report=JSON.createObjectNode().put("version",VERSION);var changed=report.putObject("changed_entries");var added=report.putObject("added_entries");
        long total=0;int matched=0;
        try(var old=new ZipFile(original.toFile());var current=new ZipFile(continuation.toFile())) {
            var a=new TreeSet<String>();var b=new TreeSet<String>();
            for(var entries=old.entries();entries.hasMoreElements();)require(a.add(entries.nextElement().getName()),"CATALOG_DUPLICATE_JAR_ENTRY");
            for(var entries=current.entries();entries.hasMoreElements();)require(b.add(entries.nextElement().getName()),"CATALOG_DUPLICATE_JAR_ENTRY");
            for(String name:a) {
                var entry=old.getEntry(name);if(entry.isDirectory() || "META-INF/MANIFEST.MF".equals(name))continue;
                require(b.contains(name),"CATALOG_ORIGINAL_CODE_ENTRY_MISSING");
                byte[] before,after;try(var in=old.getInputStream(entry)){before=in.readNBytes(16*1024*1024+1);}
                try(var in=current.getInputStream(current.getEntry(name))){after=in.readNBytes(16*1024*1024+1);}
                total+=before.length+after.length;require(before.length<=16*1024*1024 && after.length<=16*1024*1024 && total<=256L*1024*1024,"CATALOG_JAR_EXPANSION_BOUND");
                if(Arrays.equals(before,after))matched++;else {
                    require(CHANGED.contains(name),"CATALOG_UNREVIEWED_DATA_PLANE_CHANGE");
                    changed.putObject(name).put("original_sha256",sha(before)).put("continuation_sha256",sha(after));
                }
            }
            for(String name:b)if(!a.contains(name) && !current.getEntry(name).isDirectory()) {
                require(ADDED.contains(name),"CATALOG_UNREVIEWED_ADDED_CODE");byte[] bytes;
                try(var in=current.getInputStream(current.getEntry(name))){bytes=in.readNBytes(16*1024*1024+1);}
                require(bytes.length<=16*1024*1024,"CATALOG_JAR_EXPANSION_BOUND");added.put(name,sha(bytes));
            }
        }
        return report.put("matched_original_entries",matched);
    }
}
