package ie212.earthquake.spark.orchestration;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.jma.JmaArchiveEntry;
import ie212.earthquake.spark.usgs.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FullMonthJmaCacheTest {
    @TempDir Path root;
    static final Instant CUTOFF=Instant.parse("2026-10-14T00:00:00Z");
    @Test void cacheSkipsOnlyRedundantZipScanButFreshlyReadsAndHashesRawEveryTime() throws Exception {
        var store=new FileBronzeObjectStore(root);var pin=FullMonthFixture.jma(store,"success");
        var cache=new FullMonthBronzeReader.JmaStructureCache();
        var cached=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY),cache);
        var first=cached.descriptor(pin,CUTOFF);assertEquals(1,cache.fullScans());assertEquals(0,cache.hits());assertNull(first.raw());
        var next=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY),cache).descriptor(pin,CUTOFF);
        assertEquals(1,cache.fullScans());assertEquals(1,cache.hits());assertEquals(first.manifest(),next.manifest());
        var uncached=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY));
        assertEquals(uncached.parse(List.of(uncached.descriptor(pin,CUTOFF)),Set.of("2023-09")),cached.parse(List.of(next),Set.of("2023-09")));
        Path raw=Path.of(java.net.URI.create(first.manifest().path("raw_object_uri").asText()));Files.writeString(raw,"tampered");
        long hits=cache.hits();assertThrows(Exception.class,()->cached.descriptor(pin,CUTOFF));assertEquals(hits,cache.hits());
        assertThrows(Exception.class,()->cached.parse(List.of(first),Set.of("2023-09")));
    }
    @Test void cachedPhysicalCountCannotBeChangedByAPinnedManifestClaim() throws Exception {
        var store=new FileBronzeObjectStore(root);var pin=FullMonthFixture.jma(store,"success");var cache=new FullMonthBronzeReader.JmaStructureCache();
        var reader=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY),cache);var input=reader.descriptor(pin,CUTOFF);
        var manifest=(ObjectNode)input.manifest().deepCopy();manifest.put("record_count_estimate",999);
        byte[] bytes=JSON.writeValueAsBytes(manifest);Files.write(Path.of(java.net.URI.create(pin.path("manifest_uri").asText())),bytes);
        pin.put("manifest_sha256",sha(bytes));assertThrows(Exception.class,()->reader.descriptor(pin,CUTOFF));
        assertEquals(1,cache.fullScans());assertEquals(1,cache.hits());
    }
    @Test void differentMemberCannotReuseAnEarlierStructuralProof() throws Exception {
        var store=new FileBronzeObjectStore(root);var pin=FullMonthFixture.jma(store,"success");var cache=new FullMonthBronzeReader.JmaStructureCache();
        var input=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY),cache).descriptor(pin,CUTOFF);
        var m=(ObjectNode)input.manifest().deepCopy();((ObjectNode)m.path("provenance")).put("member_name","other.dat");
        byte[] bytes=JSON.writeValueAsBytes(m);Files.write(Path.of(java.net.URI.create(pin.path("manifest_uri").asText())),bytes);pin.put("manifest_sha256",sha(bytes));
        var old=FullMonthFixture.ENTRY;var entry=new JmaArchiveEntry(old.inventoryVersion(),old.year(),old.segment(),old.nativeStartJst(),old.nativeEndJst(),
                old.archiveName(),"other.dat",old.sourceUrl(),old.observedReleaseHint(),old.observedLastModifiedUtc(),old.observedContentLengthBytes(),old.mediaType(),old.recordFormat(),old.catalogEra());
        var reader=new FullMonthBronzeReader(store,List.of(entry),cache);assertThrows(Exception.class,()->reader.descriptor(pin,CUTOFF));
        assertThrows(Exception.class,()->reader.descriptor(pin,CUTOFF));assertEquals(3,cache.fullScans());assertEquals(1,cache.entries());
    }
    @Test void separateJvmSessionCacheStartsEmptyAndDefaultReaderBehaviorIsUnchanged() throws Exception {
        var store=new FileBronzeObjectStore(root);var pin=FullMonthFixture.jma(store,"empty");
        var a=new FullMonthBronzeReader.JmaStructureCache();var b=new FullMonthBronzeReader.JmaStructureCache();
        var first=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY),a).read(pin,CUTOFF);
        var second=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY),b).read(pin,CUTOFF);
        var legacy=new FullMonthBronzeReader(store,List.of(FullMonthFixture.ENTRY)).read(pin,CUTOFF);
        assertEquals(1,a.fullScans());assertEquals(1,b.fullScans());assertEquals(0,a.hits());assertEquals(first.manifest(),legacy.manifest());
        assertArrayEquals(first.raw(),second.raw());assertEquals(0,first.manifest().path("record_count_estimate").asInt());
    }
}
