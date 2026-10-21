package ie212.earthquake.spark.orchestration;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

/** Local Iceberg writes are real; Trino/MinIO acceptance is a separate runtime gate. */
class RealEtlAdapterTest {
    static SparkSession spark;
    @TempDir static Path warehouse;
    @TempDir Path work;
    FileBronzeObjectStore bronze; FileSilverObjectStore silver;
    GoldIcebergWriter writer; RealEtlAdapter adapter; ObjectNode context; AtomicBoolean held;
    @BeforeAll static void start() {
        spark=SparkSession.builder().master("local[1]").appName("ORC-07-boundaries").config("spark.ui.enabled","false")
                .config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type","hadoop").config("spark.sql.catalog.iceberg.warehouse",warehouse.toUri().toString())
                .config("spark.sql.shuffle.partitions","1").config("spark.sql.session.timeZone","UTC").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stop() { if(spark!=null) spark.stop(); }
    @BeforeEach void setup() throws Exception {
        bronze=new FileBronzeObjectStore(work.resolve("bronze")); silver=new FileSilverObjectStore(work.resolve("silver")); held=new AtomicBoolean(true);
        Runnable lease=()->{if(!held.get()) throw new IllegalStateException("LEASE_NOT_HELD");};
        String namespace="iceberg.gold_qa_"+UUID.randomUUID().toString().replace("-","");
        var request=JSON.createObjectNode(); request.put("resolver_version",VERSION); request.put("dag_id","orc_07_real_etl");
        String op=RealEtlAdapter.operation("orc_07_real_etl","manual__test:1"); request.put("run_id",op);
        request.put("namespace",namespace); request.put("config_version","test-v1"); request.put("code_version","test-v1");
        request.put("processed_at_utc","2026-10-15T00:00:00Z"); request.put("source_cutoff_utc","2026-10-15T00:00:00Z");
        request.putArray("target_months").add("2023-09"); request.putNull("baseline");
        var bases=request.putObject("baseline_snapshots"); GoldIcebergWriter.TABLES.forEach(t->bases.put(t,0));
        var inputs=request.putArray("bronze_inputs");
        inputs.add(FullMonthFixture.usgs(bronze,"adapter-usgs","2023-08-31T23:59:44Z","2023-10-01T00:00:16Z",FullMonthFixture.payload("success")));
        inputs.add(FullMonthFixture.jma(bronze,"success"));
        context=JSON.createObjectNode(); context.put("contract_version",RealEtlAdapter.PROTOCOL); context.put("mode","real");
        context.put("dag_id","orc_07_real_etl"); context.put("run_id","manual__test:1"); context.put("operation_id",op); context.put("trigger","manual");
        context.put("window_start_utc","2023-09-28T00:00:00Z"); context.put("window_end_utc","2023-10-01T00:00:00Z");
        context.putNull("source_summary_sha256"); context.set("resolver_request",request); rehash();
        writer=new GoldIcebergWriter(spark,namespace,silver,lease);
        var publisher=new GoldPublisher(sql->{throw new IOException("TRINO_UNAVAILABLE");},silver,lease);
        adapter=new RealEtlAdapter(spark,bronze,silver,List.of(FullMonthFixture.ENTRY),writer,publisher,lease);
    }
    void rehash() throws Exception { context.remove("context_sha256"); context.put("context_sha256",digest(context)); }
    ObjectNode through(String last) throws Exception {
        ObjectNode upstream=null;
        for(String phase:RealEtlAdapter.PHASES) {
            upstream=adapter.execute(phase,context,upstream,work); if(phase.equals(last)) break;
        }
        return upstream;
    }
    String prefix() { return "real-etl/"+context.path("operation_id").asText(); }
    @Test void actualLocalGoldCommitAndFullReplayKeepAllSixSnapshotIdsAndNeverPublish() throws Exception {
        var first=through("gold"); var snapshots=writer.baselines();
        assertEquals(6,first.path("snapshots").size()); assertTrue(snapshots.values().stream().allMatch(id->id>0));
        var rerun=through("gold"); assertEquals(first,rerun); assertEquals(snapshots,writer.baselines());
        assertThrows(IOException.class,()->adapter.execute("verify",context,first,work));
        assertFalse(silver.exists(prefix()+"/publish-receipt.json"));
        assertFalse(silver.exists("gold-verification/"+context.path("operation_id").asText()+"/publication.json"));
        String key="gold-commits/"+context.path("operation_id").asText()+"/commit.json";
        var altered=(ObjectNode)JSON.readTree(silver.read(key)); altered.put("identity_sha256","b".repeat(64));
        silver.put(key,JSON.writeValueAsBytes(altered),"application/json");
        assertEquals("GOLD_JOURNAL_CHANGED",assertThrows(IOException.class,()->adapter.execute("verify",context,first,work)).getMessage());
    }
    @Test void phasesCannotSkipPredecessorOrUseChangedUpstream() throws Exception {
        assertThrows(Exception.class,()->adapter.execute("gold",context,null,work));
        var ready=through("readiness"); var changed=ready.deepCopy(); changed.put("artifact_sha256","0".repeat(64));
        assertThrows(IOException.class,()->adapter.execute("bronze",context,changed,work));
        assertTrue(writer.baselines().values().stream().allMatch(id->id==0));
    }
    @Test void alteredArtifactBytesAreRejectedBeforeNextPhase() throws Exception {
        var ready=through("readiness"); silver.put(prefix()+"/readiness.json","{}".getBytes(),"application/json");
        assertThrows(IOException.class,()->adapter.execute("bronze",context,ready,work));
        assertFalse(silver.exists(prefix()+"/bronze-receipt.json"));
    }
    @Test void mockContextOrChangedChecksumCannotEnterRealPipeline() throws Exception {
        context.put("mode","mock"); rehash(); assertThrows(IOException.class,()->through("readiness"));
        context.put("mode","real"); rehash(); context.put("window_end_utc","2023-10-02T00:00:00Z");
        assertThrows(IOException.class,()->through("readiness"));
        assertFalse(silver.exists(prefix()+"/context.json"));
    }
    @Test void missingCoverageStopsBeforeAnyGoldWrite() throws Exception {
        ((ArrayNode)context.path("resolver_request").path("bronze_inputs")).remove(0); rehash();
        var bronze=through("bronze"); assertThrows(IOException.class,()->adapter.execute("silver",context,bronze,work));
        assertTrue(writer.baselines().values().stream().allMatch(id->id==0));
        assertFalse(silver.exists(prefix()+"/gold-receipt.json"));
    }
    @Test void immutableContextAndExactWholeRunLeaseAreRequired() throws Exception {
        through("readiness"); held.set(false); assertThrows(IllegalStateException.class,()->through("readiness"));
        held.set(true); ((ObjectNode)context.path("resolver_request")).put("config_version","changed"); rehash();
        assertThrows(IOException.class,()->through("readiness"));
    }
    @Test void corruptedFullMonthProofStopsGoldWithoutOverwritingAnyPartition() throws Exception {
        var monthly=through("silver");
        silver.put("full-month/"+context.path("operation_id").asText()+"/registry.json","{}".getBytes(),"application/json");
        assertThrows(Exception.class,()->adapter.execute("gold",context,monthly,work));
        assertTrue(writer.baselines().values().stream().allMatch(id->id==0));
    }
    @Test void cumulativeCoverageVerificationReads225MonthsButStillRejectsChangedProof() throws Exception {
        var all=JSON.createArrayNode();
        for(var m=java.time.YearMonth.of(2000,1);m.isBefore(java.time.YearMonth.of(2018,10));m=m.plusMonths(1)) all.add(m.toString());
        var pin=JSON.createObjectNode().put("source_system","JMA_BULLETIN").put("manifest_uri","file:///fixture/history.json");
        var input=new FullMonthBronzeReader.Input(pin,JSON.createObjectNode(),new byte[0],
            java.time.Instant.parse("1999-01-01T00:00:00Z"),java.time.Instant.parse("2019-01-01T00:00:00Z"));
        var inputs=List.of(input); var registry=JSON.createObjectNode(); registry.set("complete_months",all);
        registry.set("coverage",FullMonthStateResolver.coverage(inputs,registryMonths(all)));
        assertDoesNotThrow(()->RealEtlAdapter.verifyRegistryCoverage(inputs,registry));
        assertThrows(IOException.class,()->months(all)); // affected write bound remains 24
        ((ObjectNode)registry.path("coverage").get(1)).put("coverage","INCOMPLETE");
        assertEquals("FULL_MONTH_COVERAGE_CHANGED",assertThrows(IOException.class,
            ()->RealEtlAdapter.verifyRegistryCoverage(inputs,registry)).getMessage());
    }
}
