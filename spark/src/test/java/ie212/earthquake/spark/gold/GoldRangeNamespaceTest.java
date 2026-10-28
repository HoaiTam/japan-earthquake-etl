package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GoldRangeNamespaceTest {
    @TempDir Path root;
    @Test void rangeNamespaceUsesAllBlockersAndFreshPublicationReadbacks() throws Exception {
        var c=GoldPublicationTest.commit();String ns="iceberg.dat03_range_"+"a".repeat(32);c.put("namespace",ns);
        for(String t:GoldIcebergWriter.TABLES)((ObjectNode)c.path("tables").path(t)).put("table",ns+"."+t);
        var store=new FileSilverObjectStore(root.toRealPath());
        store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");
        var fake=new GoldPublicationTest.Fake();var publisher=GoldPublisher.researchCatalogCheckpointed(fake,store,()->{});
        var verified=publisher.verify("gold-test",ns,"a".repeat(64));
        assertEquals(GoldPublisher.BUCKETED_OUTSIDE_VERSION,verified.path("outside_scope_strategy").asText());
        assertEquals(GoldPublisher.BUCKETED_BLOCKERS_VERSION,verified.path("fact_blocker_strategy").asText());
        assertTrue(publisher.publish("gold-test",ns,"a".repeat(64)).path("published").asBoolean());
        assertTrue(fake.queries.stream().anyMatch(q->q.startsWith("SELECT snapshot_id FROM")));
    }
    @Test void arbitraryOrSharedNamespacesRemainBlockedBeforeSql() throws Exception {
        var store=new FileSilverObjectStore(root.toRealPath());
        for(String ns:List.of("iceberg.gold","iceberg.dat03_range_bad","iceberg.dat03_range_"+"a".repeat(31),"iceberg.other_"+"a".repeat(32))) {
            var c=GoldPublicationTest.commit();c.put("namespace",ns);
            for(String t:GoldIcebergWriter.TABLES)((ObjectNode)c.path("tables").path(t)).put("table",ns+"."+t);
            store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(c),"application/json");
            var fake=new GoldPublicationTest.Fake();
            assertThrows(Exception.class,()->GoldPublisher.researchCatalogCheckpointed(fake,store,()->{}).verify("gold-test",ns,"a".repeat(64)));
            assertTrue(fake.queries.isEmpty());
        }
    }
}
