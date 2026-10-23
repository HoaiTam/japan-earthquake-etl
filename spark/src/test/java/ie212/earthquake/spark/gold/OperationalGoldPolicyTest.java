package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.CurrentMonthContract;
import ie212.earthquake.spark.silver.FileSilverObjectStore;
import ie212.earthquake.spark.ml.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OperationalGoldPolicyTest {
    @TempDir Path root;
    static ObjectNode policy() {
        var p=CurrentMonthContract.flags();p.remove("published");p.put("contract_version",OperationalGoldPolicy.VERSION);
        p.put("registry_sha256","b".repeat(64));p.put("silver_receipt_sha256","c".repeat(64));
        p.put("event_cutoff_utc","2023-02-01T00:00:00Z");p.put("observation_cutoff_utc","2026-10-22T01:00:00Z");
        p.putObject("month_coverage").putObject("2023-01").put("event_start_utc","2023-01-01T00:00:00Z")
            .put("event_end_utc_exclusive","2023-02-01T00:00:00Z");return p;
    }
    static ObjectNode commit() throws Exception {
        var c=GoldPublicationTest.commit();c.put("namespace","iceberg."+OperationalGoldPolicy.SCHEMA);
        for(var pin:c.path("tables")) ((ObjectNode)pin).put("table","iceberg."+OperationalGoldPolicy.SCHEMA+"."
            +pin.path("table").asText().substring("iceberg.gold.".length()));
        OperationalGoldPolicy.attach(c,policy());return c;
    }
    @Test void policyCannotBecomeResearchOrClaimCompleteMonths() {
        OperationalGoldPolicy.validate("iceberg."+OperationalGoldPolicy.SCHEMA,policy());
        for(String key:List.of("research_ready","ml_upstream_eligible")) {
            var p=policy().put(key,true);assertThrows(Exception.class,()->OperationalGoldPolicy.validate("iceberg."+OperationalGoldPolicy.SCHEMA,p));
        }
        var p=policy().put("coverage_mode","COMPLETE_MONTHS");assertThrows(Exception.class,()->OperationalGoldPolicy.validate("iceberg."+OperationalGoldPolicy.SCHEMA,p));
        assertThrows(Exception.class,()->OperationalGoldPolicy.validate("iceberg.gold",policy()));
        var unknown=policy().put("caller_override",true);assertThrows(Exception.class,()->OperationalGoldPolicy.validate("iceberg."+OperationalGoldPolicy.SCHEMA,unknown));
    }
    @Test void legacyPublisherCannotPromoteAnOperationalCommit() throws Exception {
        var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(commit()),"application/json");
        var sql=new GoldPublicationTest.Fake();
        assertThrows(Exception.class,()->new GoldPublisher(sql,store,()->{}).publish("gold-test","iceberg."+OperationalGoldPolicy.SCHEMA,"a".repeat(64)));
        assertThrows(Exception.class,()->new GoldPublisher(sql,store,()->{}).verify("gold-test","iceberg."+OperationalGoldPolicy.SCHEMA,"a".repeat(64)));
        assertTrue(sql.queries.isEmpty());assertFalse(store.exists("gold-verification/gold-test/publication.json"));
    }
    @Test void operationalPublicationAnnotatesStatusViewsAndReceiptAndRerunsReadOnly() throws Exception {
        var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(commit()),"application/json");
        var sql=new GoldPublicationTest.Fake();var publisher=new GoldPublisher(sql,store,()->{},true);
        var first=publisher.publish("gold-test","iceberg."+OperationalGoldPolicy.SCHEMA,"a".repeat(64));
        assertEquals("PROVISIONAL",first.path("publication_quality").asText());assertFalse(first.path("ml_upstream_eligible").asBoolean(true));
        assertTrue(sql.queries.stream().anyMatch(q->q.startsWith("INSERT") && q.contains("'OBSERVED_AS_OF','PROVISIONAL'") && q.contains("false,false")));
        assertTrue(sql.queries.stream().anyMatch(q->q.startsWith("CREATE OR REPLACE VIEW") && q.contains("AS publication_quality")));
        int before=sql.queries.size();assertEquals(first,publisher.publish("gold-test","iceberg."+OperationalGoldPolicy.SCHEMA,"a".repeat(64)));
        assertFalse(sql.queries.subList(before,sql.queries.size()).stream().anyMatch(q->q.startsWith("CREATE") || q.startsWith("INSERT")));
    }
    @Test void missingSnapshotPolicyOrBadPublicationRowCannotPass() throws Exception {
        var store=new FileSilverObjectStore(root);store.put("gold-commits/gold-test/commit.json",GoldPublicationTest.JSON.writeValueAsBytes(commit()),"application/json");
        for(boolean missingSnapshot:List.of(true,false)) {
            var sql=new GoldPublicationTest.Fake() {
                @Override public GoldPublisher.Result execute(String q) throws Exception {
                    if(missingSnapshot ? q.contains(OperationalGoldPolicy.PROPERTY) : q.contains("AND operational_policy_json="))
                        return new GoldPublisher.Result(List.of(),List.of(GoldPublicationTest.JSON.readTree("[0]")));
                    return super.execute(q);
                }
            };
            assertThrows(Exception.class,()->new GoldPublisher(sql,store,()->{},true).publish("gold-test","iceberg."+OperationalGoldPolicy.SCHEMA,"a".repeat(64)));
            assertFalse(store.exists("gold-verification/gold-test/publication.json"));
            assertFalse(sql.queries.stream().anyMatch(q->q.startsWith("CREATE OR REPLACE VIEW iceberg."+OperationalGoldPolicy.SCHEMA+".earthquake_event_current AS")));
        }
    }
    @Test void researchConsumersRejectReservedPhysicalTableEvenWithForgedEligibility() {
        String ns="iceberg."+OperationalGoldPolicy.SCHEMA;
        GoldPublisher.Sql none=q->{throw new AssertionError("SQL must not run");};
        assertThrows(Exception.class,()->new DatasetManifestWriter(none,()->{}).build(ns,"pub_"+"a".repeat(64),"iceberg.ml",null,"run",Instant.now()));
        var manifest=GoldPublicationTest.JSON.createObjectNode().put("physical_gold_table_name",ns+".event_current").put("gold_snapshot_id",1).put("ml_upstream_eligible",true);
        assertThrows(Exception.class,()->DatasetManifestWriter.pinnedGoldSql(manifest));
        manifest.put("verify_status","PASSED");
        assertThrows(Exception.class,()->new DatasetIdentityPlan(manifest,null,"run",Instant.now()));
        assertTrue(OperationalGoldPolicy.researchSnapshotPredicate().contains("IS NULL"));
    }
}
