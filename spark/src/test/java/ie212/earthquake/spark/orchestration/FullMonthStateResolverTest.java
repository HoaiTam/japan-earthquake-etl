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
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class FullMonthStateResolverTest {
    static SparkSession spark;
    @TempDir static Path catalogRoot;
    @TempDir Path temporary;
    FileBronzeObjectStore bronze; FileSilverObjectStore silver; FullMonthStateResolver resolver;
    Map<String,Long> current;
    @BeforeAll static void startSpark() {
        spark=SparkSession.builder().master("local[1]").appName("ORC-06-fixtures").config("spark.ui.enabled","false")
                .config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.type","hadoop").config("spark.sql.catalog.iceberg.warehouse",catalogRoot.toUri().toString())
                .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","1").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stopSpark() { if(spark!=null) spark.stop(); }
    @BeforeEach void setUp() {
        bronze=new FileBronzeObjectStore(temporary.resolve("bronze")); silver=new FileSilverObjectStore(temporary.resolve("silver"));
        current=new TreeMap<>(); GoldIcebergWriter.TABLES.forEach(t->current.put(t,0L));
        resolver=new FullMonthStateResolver(bronze,silver,List.of(FullMonthFixture.ENTRY),()->new TreeMap<>(current),()->{});
    }
    ObjectNode request(String run,String month) {
        var r=JSON.createObjectNode(); r.put("resolver_version",VERSION); r.put("run_id",run); r.put("dag_id","orc_06_qa");
        r.put("namespace","iceberg.gold_qa_orc06"); r.put("config_version","test-v1"); r.put("code_version","test-v1");
        r.put("processed_at_utc","2026-10-14T00:00:00Z"); r.put("source_cutoff_utc","2026-10-14T00:00:00Z");
        r.putArray("target_months").add(month); r.set("baseline_snapshots",JSON.valueToTree(current));
        r.putNull("baseline"); r.putArray("bronze_inputs"); return r;
    }
    ObjectNode bootstrap(String run,String usgsFixture,String jmaFixture) throws Exception {
        var r=request(run,"2023-09"); r.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,run+"-usgs",
                "2023-08-31T23:59:44Z","2023-10-01T00:00:16Z",FullMonthFixture.payload(usgsFixture)));
        r.withArray("bronze_inputs").add(FullMonthFixture.jma(bronze,jmaFixture)); return r;
    }
    void baseline(ObjectNode request,JsonNode receipt) {
        var b=request.putObject("baseline"); b.put("run_id",receipt.path("run_id").asText());
        b.put("registry_sha256",receipt.path("registry_sha256").asText());
    }
    @Test void bootstrapHasFourReadbackDatasetsDeterministicRerunAndGoldReaderCompatibility() throws Exception {
        var req=bootstrap("bootstrap","success","success"); var first=resolver.resolve(spark,req);
        assertEquals("FullMonthReady",first.path("status").asText()); assertFalse(first.path("gold_published").asBoolean(true));
        assertEquals("COMPLETE_MONTHS",first.path("scope_completeness").asText());
        assertEquals(first,resolver.resolve(spark,req));
        var bundle=new SilverBundlePublisher(silver).verify("bootstrap"); assertEquals(4,bundle.path("datasets").size());
        var goldRequest=first.path("gold_request"); var context=goldRequest.path("run_context");
        var run=new GoldRunContext("bootstrap",Instant.parse(context.path("window_start_utc").asText()),Instant.parse(context.path("window_end_utc").asText()),
                LocalDate.of(2026,10,14),true,"test-v1",List.of(first.path("silver_bundle_uri").asText()));
        var gold=GoldInputReader.read(spark,silver,"bootstrap",first.path("silver_bundle_sha256").asText(),run,Instant.parse("2026-10-14T00:00:00Z"),temporary.resolve("readback"));
        assertEquals(first.path("reconciliation").path("canonical").asLong(),gold.canonicalEventCount());
        assertEquals(first.path("reconciliation").path("current").asLong(),gold.bridgeRowCount());
    }
    @Test void scopedReplayKeepsHistoryCoverageWithoutParsingUnchangedJmaMonthsAndRejectsDowngrade() throws Exception {
        var raw=new java.io.ByteArrayOutputStream();
        try(var zip=new java.util.zip.ZipOutputStream(raw)) {
            zip.putNextEntry(new java.util.zip.ZipEntry("hypo.dat"));
            String template=Files.readAllLines(FullMonthFixture.repo().resolve("tests/fixtures/jma/fixed-width/success.hyp")).get(0);
            for(String date:List.of("2023091509000000","2023101509000000","2023111509000000"))
                zip.write((template.substring(0,1)+date+template.substring(17)+"\n").getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
            zip.closeEntry();
        }
        var first=request("scoped-parent","2023-09");
        first.withArray("bronze_inputs").add(FullMonthFixture.jmaBytes(bronze,raw.toByteArray(),"annual-input","release-v1"));
        first.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"september-empty","2023-08-31T23:59:44Z","2023-10-01T00:00:16Z",FullMonthFixture.payload("empty")));
        var parent=resolver.resolve(spark,first);
        var next=request("scoped-october","2023-10").put("resolver_version",SCOPED_VERSION); baseline(next,parent);
        next.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"october-empty","2023-09-30T23:59:44Z","2023-11-01T00:00:16Z",FullMonthFixture.payload("empty")));
        var result=resolver.resolve(spark,next);
        assertEquals(1,result.path("reconciliation").path("input_unique_revisions").asInt());
        assertEquals(JSON.readTree("[\"2023-10\"]"),result.path("affected_months"));
        var context=result.path("gold_request").path("run_context");
        var goldContext=new GoldRunContext("scoped-october",Instant.parse(context.path("window_start_utc").asText()),
                Instant.parse(context.path("window_end_utc").asText()),LocalDate.of(2026,10,14),true,"test-v1",List.of(result.path("silver_bundle_uri").asText()));
        var gold=GoldInputReader.read(spark,silver,"scoped-october",result.path("silver_bundle_sha256").asText(),goldContext,
                Instant.parse("2026-10-14T00:00:00Z"),temporary.resolve("scoped-gold-readback"));
        assertEquals(result.path("reconciliation").path("canonical").asLong(),gold.canonicalEventCount());
        assertEquals(result,resolver.resolve(spark,next));
        var registry=JSON.readTree(silver.read(FullMonthStateResolver.root("scoped-october")+"/registry.json"));
        assertEquals(JSON.readTree("[\"2023-09\",\"2023-10\"]"),registry.path("complete_months"));
        var downgrade=request("downgrade","2023-10"); baseline(downgrade,result);
        assertEquals("RESOLVER_DOWNGRADE_FORBIDDEN",assertThrows(IOException.class,()->resolver.resolve(spark,downgrade)).getMessage());
        assertFalse(silver.exists(FullMonthStateResolver.root("downgrade")+"/identity.sha256"));
    }
    @Test void descriptorValidatesBytesNowAndAgainOnUseWithoutRetainingRaw() throws Exception {
        var pin=FullMonthFixture.jma(bronze,"success"); var reader=new FullMonthBronzeReader(bronze,List.of(FullMonthFixture.ENTRY));
        var descriptor=reader.descriptor(pin,Instant.parse("2026-10-14T00:00:00Z")); assertNull(descriptor.raw());
        assertFalse(reader.parse(List.of(descriptor),Set.of("2023-09")).isEmpty());
        Path path=Path.of(java.net.URI.create(descriptor.manifest().path("raw_object_uri").asText())); Files.writeString(path,"corrupt");
        assertThrows(IOException.class,()->reader.parse(List.of(descriptor),Set.of("2023-09")));
        assertThrows(IOException.class,()->reader.descriptor(pin,Instant.parse("2026-10-14T00:00:00Z")));
    }
    @Test void scopedReplayClosesCrossMonthGraphBeforeCertifyingAndMovedUsgsStillExpandsOldMonth() throws Exception {
        var r=request("v2-cross","2023-09").put("resolver_version",SCOPED_VERSION);
        var payload=FullMonthFixture.payload("success"); var props=(ObjectNode)payload.path("features").get(0).path("properties");
        props.put("time",Instant.parse("2023-09-30T23:59:59Z").toEpochMilli());
        r.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"v2-cross-usgs","2023-08-31T23:59:44Z","2023-11-01T00:00:16Z",payload));
        r.withArray("bronze_inputs").add(FullMonthFixture.jmaBytes(bronze,FullMonthFixture.jmaRecordArchive("2023100109000100"),"v2-cross-jma","release-v1"));
        var first=resolver.resolve(spark,r);
        assertEquals(JSON.readTree("[\"2023-09\",\"2023-10\"]"),first.path("affected_months"));
        assertEquals(1,first.path("reconciliation").path("canonical").asInt());
        var delta=request("v2-move","2023-11").put("resolver_version",SCOPED_VERSION); baseline(delta,first);
        props.put("time",Instant.parse("2023-11-10T00:00:00Z").toEpochMilli());
        props.put("updated",Instant.parse("2023-11-11T00:00:00Z").toEpochMilli());
        delta.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"v2-moved-usgs","2023-10-31T23:59:44Z","2023-12-01T00:00:16Z",payload));
        var moved=resolver.resolve(spark,delta);
        assertEquals(JSON.readTree("[\"2023-09\",\"2023-10\",\"2023-11\"]"),moved.path("affected_months"));
        assertTrue(moved.path("reconciliation").path("balanced").asBoolean());
    }
    @Test void scopedNewReleaseRebuildsRegisteredLineageEvenWhenAllRawRecordBytesAreUnchanged() throws Exception {
        var initial=bootstrap("v2-release-parent","empty","success").put("resolver_version",SCOPED_VERSION);
        var before=resolver.resolve(spark,initial); var next=request("v2-release-next","2023-10").put("resolver_version",SCOPED_VERSION);
        baseline(next,before);
        var raw=Files.readAllBytes(FullMonthFixture.repo().resolve("tests/fixtures/jma/archives/success.zip"));
        next.withArray("bronze_inputs").add(FullMonthFixture.jmaBytes(bronze,raw,"v2-same-bytes-new-release","release-v2"));
        next.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"v2-release-october-empty","2023-09-30T23:59:44Z","2023-11-01T00:00:16Z",FullMonthFixture.payload("empty")));
        var result=resolver.resolve(spark,next);
        assertEquals(JSON.readTree("[\"2023-09\",\"2023-10\"]"),result.path("affected_months"));
        assertEquals(before.path("reconciliation").path("current"),result.path("reconciliation").path("current"));
        assertEquals(before,resolver.verify("v2-release-parent",before.path("registry_sha256").asText()));
    }
    @Test void deltaKeepsEventsOutsideOverlapAndRemovesOldCurrentWhenEventMovesMonths() throws Exception {
        var payload=FullMonthFixture.payload("success"); var feature=payload.path("features").get(0).deepCopy();
        ((ObjectNode)feature).put("id","keep-outside-overlap"); ((ObjectNode)feature.path("properties")).put("time",Instant.parse("2023-09-20T00:00:00Z").toEpochMilli());
        payload.withArray("features").add(feature); ((ObjectNode)payload.path("metadata")).put("count",2);
        var first=request("initial","2023-09"); first.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"initial-usgs",
                "2023-08-31T23:59:44Z","2023-10-01T00:00:16Z",payload)); first.withArray("bronze_inputs").add(FullMonthFixture.jma(bronze,"success"));
        var before=resolver.resolve(spark,first);
        var delta=request("moved","2023-10"); baseline(delta,before);
        var revised=FullMonthFixture.payload("success"); var props=(ObjectNode)revised.path("features").get(0).path("properties");
        props.put("time",Instant.parse("2023-10-01T12:00:00Z").toEpochMilli()); props.put("updated",Instant.parse("2023-10-02T00:00:00Z").toEpochMilli());
        delta.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"delta-usgs","2023-09-30T23:59:44Z","2023-11-01T00:00:16Z",revised));
        var after=resolver.resolve(spark,delta);
        assertEquals(JSON.readTree("[\"2023-09\",\"2023-10\"]"),after.path("affected_months"));
        assertEquals(before.path("reconciliation").path("current").asInt(),after.path("reconciliation").path("current").asInt());
        assertEquals(1,after.path("reconciliation").path("history").asInt());
        assertEquals(before,resolver.verify("initial",before.path("registry_sha256").asText()));
    }
    @Test void explicitEmptyMonthHasVerifiedZeroDatasetsAndNonemptyCoverageProof() throws Exception {
        var r=bootstrap("empty","empty","empty"); var result=resolver.resolve(spark,r);
        assertEquals(0,result.path("reconciliation").path("canonical").asInt()); assertEquals(2,result.path("coverage").size());
        assertEquals(0,new SilverBundlePublisher(silver).verify("empty").path("datasets").path("source_observation").path("record_count").asInt());
        assertEquals(JSON.readTree("[\"2023-09\"]"),result.path("reconciliation").path("explicit_empty_months"));
    }
    @Test void pilotAndMissingSourceCoverageFailBeforeAnyWrite() throws Exception {
        var r=request("partial","2023-09"); r.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"partial-usgs",
                "2023-09-01T00:00:00Z","2023-09-04T00:00:00Z",FullMonthFixture.payload("success")));
        r.withArray("bronze_inputs").add(FullMonthFixture.jma(bronze,"success"));
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,r)).getMessage().contains("COVERAGE"));
        assertTrue(silver.list("").isEmpty());
    }
    @Test void staleBaselineIdentityConflictAndCorruptParentFailClosed() throws Exception {
        var r=bootstrap("reserved","success","success"); var receipt=resolver.resolve(spark,r);
        var conflict=r.deepCopy(); conflict.put("code_version","different");
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,conflict)).getMessage().contains("IDENTITY_CONFLICT"));
        current.put("event_current",42L);
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,r)).getMessage().contains("STALE")); current.put("event_current",0L);
        var next=request("corrupt-parent","2023-09"); baseline(next,receipt);
        silver.put(FullMonthStateResolver.root("reserved")+"/registry.json","tampered".getBytes(),"application/json");
        assertThrows(IOException.class,()->resolver.resolve(spark,next));
        assertFalse(silver.exists(FullMonthStateResolver.root("corrupt-parent")+"/_SUCCESS"));
    }
    @Test void rawOrManifestChangedUnderSamePinIsRejected() throws Exception {
        var r=bootstrap("tamper","success","success"); var pin=r.path("bronze_inputs").get(0);
        var manifestPath=Path.of(java.net.URI.create(pin.path("manifest_uri").asText())); Files.writeString(manifestPath,"changed");
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,r)).getMessage().contains("PIN"));
        assertTrue(silver.list("").isEmpty());
    }
    @Test void snapshotChangesDuringWorkCannotProduceSuccessMarker() throws Exception {
        var r=bootstrap("racing","empty","empty"); var calls=new AtomicInteger();
        var racing=new FullMonthStateResolver(bronze,silver,List.of(FullMonthFixture.ENTRY),()->{
            var map=new TreeMap<>(current); if(calls.incrementAndGet()>1) map.put("event_current",99L); return map;
        },()->{});
        assertThrows(IOException.class,()->racing.resolve(spark,r));
        assertFalse(silver.exists(FullMonthStateResolver.root("racing")+"/_SUCCESS"));
    }
    @Test void malformedRequestUnknownFlagsDuplicatesAndWrongSnapshotTypesAreRejected() throws Exception {
        var r=bootstrap("invalid","empty","empty");
        for(String change:List.of("version","complete","type","duplicate-month","open-month")) {
            var copy=r.deepCopy();
            switch(change) {
                case "version" -> copy.put("resolver_version","unreviewed");
                case "complete" -> copy.put("scope_completeness","COMPLETE_MONTHS");
                case "type" -> ((ObjectNode)copy.path("baseline_snapshots")).put("event_current",true);
                case "duplicate-month" -> copy.withArray("target_months").add("2023-09");
                case "open-month" -> { copy.putArray("target_months").add("2026-10"); }
            }
            assertThrows(Exception.class,()->resolver.resolve(spark,copy),change);
        }
        assertTrue(silver.list("").isEmpty());
    }

    @Test void changedArchiveSameReleaseIsRejectedAndNewReleaseRequiresEveryChangedMonth() throws Exception {
        var r=bootstrap("old-release","success","success"); var first=resolver.resolve(spark,r);
        var changed=FullMonthFixture.jmaRecordArchive("2023100109000100");
        for(String release:List.of("release-v1","release-v2")) {
            var next=request("new-"+release,"2023-09"); baseline(next,first);
            next.withArray("bronze_inputs").add(FullMonthFixture.jmaBytes(bronze,changed,"jma-new-"+release,release));
            var error=assertThrows(IOException.class,()->resolver.resolve(spark,next));
            assertTrue(error.getMessage().contains(release.equals("release-v1")?"RELEASE_IDENTITY":"COVERAGE"),error.getMessage());
            assertFalse(silver.exists(FullMonthStateResolver.root("new-"+release)+"/_SUCCESS"));
        }
    }

    @Test void crossMonthSourceLinkCannotBeCutAndBothMonthsCanBeExplicitlyRebuilt() throws Exception {
        var r=request("cross-boundary","2023-09");
        var payload=FullMonthFixture.payload("success"); var props=(ObjectNode)payload.path("features").get(0).path("properties");
        props.put("time",Instant.parse("2023-09-30T23:59:59Z").toEpochMilli());
        props.put("updated",Instant.parse("2023-10-02T00:00:00Z").toEpochMilli());
        r.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"cross-usgs","2023-08-31T23:59:44Z","2023-10-01T00:00:16Z",payload));
        r.withArray("bronze_inputs").add(FullMonthFixture.jmaBytes(bronze,FullMonthFixture.jmaRecordArchive("2023100109000100"),"jma-cross","release-v1"));
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,r)).getMessage().contains("EXPANSION"));
        assertTrue(silver.list("").isEmpty());
        r.withArray("target_months").add("2023-10");
        r.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"cross-oct","2023-09-30T23:59:44Z","2023-11-01T00:00:16Z",payload));
        var result=resolver.resolve(spark,r);
        assertEquals(1,result.path("reconciliation").path("canonical").asInt());
        assertEquals(2,result.path("reconciliation").path("memberships").asInt());
        assertEquals(JSON.readTree("[\"2023-09\",\"2023-10\"]"),result.path("affected_months"));
    }

    @Test void untouchedMonthIsNotRepublishedAndRegistryRetainsCoverageAndInputPins() throws Exception {
        var initial=bootstrap("unchanged-baseline","success","success"); var first=resolver.resolve(spark,initial);
        byte[] original=silver.read("bundles/unchanged-baseline/manifest.json");
        var next=request("only-october","2023-10"); baseline(next,first);
        next.withArray("bronze_inputs").add(FullMonthFixture.usgs(bronze,"empty-oct","2023-09-30T23:59:44Z","2023-11-01T00:00:16Z",FullMonthFixture.payload("empty")));
        var after=resolver.resolve(spark,next);
        assertEquals(JSON.readTree("[\"2023-10\"]"),after.path("affected_months"));
        assertArrayEquals(original,silver.read("bundles/unchanged-baseline/manifest.json"));
        var registry=JSON.readTree(silver.read(FullMonthStateResolver.root("only-october")+"/registry.json"));
        assertEquals(JSON.readTree("[\"2023-09\",\"2023-10\"]"),registry.path("complete_months")); assertEquals(4,registry.path("coverage").size());
    }

    @Test void filteredSaturatedFalseValidationAndWrongTypesFailEvenWithRepinnedManifest() throws Exception {
        var r=bootstrap("bad-source","success","success"); var p=(ObjectNode)r.path("bronze_inputs").get(0);
        Path path=Path.of(java.net.URI.create(p.path("manifest_uri").asText())); var original=(ObjectNode)JSON.readTree(Files.readAllBytes(path));
        for(String defect:List.of("filtered","saturated","flag","count","future-interval")) {
            var changed=original.deepCopy();
            switch(defect) {
                case "filtered" -> ((ObjectNode)changed.path("request").path("query")).put("minmagnitude","4.0");
                case "saturated" -> ((ObjectNode)changed.path("request").path("query")).put("limit","1");
                case "flag" -> ((ObjectNode)changed.path("validation")).put("checksum_verified","true");
                case "count" -> changed.put("record_count_estimate",99);
                case "future-interval" -> changed.put("retrieved_at_utc","2023-09-15T00:00:00Z");
            }
            byte[] bytes=JSON.writeValueAsBytes(changed); Files.write(path,bytes); p.put("manifest_sha256",sha(bytes));
            assertThrows(IOException.class,()->resolver.resolve(spark,r),defect);
        }
        assertTrue(silver.list("").isEmpty());
    }

    @Test void missingSourceGapDuplicatePinAndExistingGoldWithoutRegistryAreRejected() throws Exception {
        var r=bootstrap("source-gap","empty","empty");
        var duplicate=r.deepCopy(); duplicate.withArray("bronze_inputs").add(duplicate.path("bronze_inputs").get(0));
        assertThrows(IOException.class,()->resolver.resolve(spark,duplicate));
        var missing=r.deepCopy(); missing.putArray("bronze_inputs").add(r.path("bronze_inputs").get(0));
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,missing)).getMessage().contains("COVERAGE"));
        current.put("event_current",100L); r.set("baseline_snapshots",JSON.valueToTree(current));
        assertTrue(assertThrows(IOException.class,()->resolver.resolve(spark,r)).getMessage().contains("REGISTRY"));
        assertTrue(silver.list("").isEmpty());
    }

    @Test void partialReceiptFailureCanResumeOnlySameIdentityWithoutChangingVerifiedBundle() throws Exception {
        var r=bootstrap("resumable","empty","empty"); var failures=new AtomicInteger();
        SilverObjectStore flaky=new SilverObjectStore() {
            public void put(String key,byte[] bytes,String type) throws IOException {
                if(key.equals("full-month/resumable/receipt.json") && failures.getAndIncrement()==0) throw new IOException("INJECTED_WRITE_FAILURE");
                silver.put(key,bytes,type);
            }
            public byte[] read(String key) throws IOException { return silver.read(key); }
            public boolean exists(String key) throws IOException { return silver.exists(key); }
            public List<String> list(String prefix) throws IOException { return silver.list(prefix); }
            public void delete(String key) { fail("No deletes"); }
            public void deletePrefix(String prefix) { fail("No deletes"); }
            public void move(String from,String to) { fail("No moves"); }
            public String uriForKey(String key) { return silver.uriForKey(key); }
        };
        var interrupted=new FullMonthStateResolver(bronze,flaky,List.of(FullMonthFixture.ENTRY),()->current,()->{});
        assertThrows(IOException.class,()->interrupted.resolve(spark,r)); assertFalse(silver.exists("full-month/resumable/_SUCCESS"));
        byte[] bundle=silver.read("bundles/resumable/manifest.json");
        var done=interrupted.resolve(spark,r); assertEquals("FullMonthReady",done.path("status").asText());
        assertArrayEquals(bundle,silver.read("bundles/resumable/manifest.json"));
    }

    @Test void closureAlternatesAcrossOldAndNewRevisionGraphsToAFixedPoint() {
        var affected=new TreeSet<>(Set.of("2023-09"));
        var graph=Map.of("a","2023-09","b","2023-10","c","2023-11");
        var oldLink=new SilverSourceLink("old","b","c",0,0,null,null,1,"ACCEPTED",List.of(),"link_v1.0",Instant.EPOCH);
        var newLink=new SilverSourceLink("new","a","b",0,0,null,null,1,"ACCEPTED",List.of(),"link_v1.0",Instant.EPOCH);
        FullMonthStateResolver.closeBothGraphs(affected,graph,List.of(oldLink),graph,List.of(newLink));
        assertEquals(Set.of("2023-09","2023-10","2023-11"),affected);
    }

    @Test void pinnedRegistryAndActualLocalIcebergCommitResumeWithNonzeroSnapshotsWithoutGoldWrites() throws Exception {
        var initial=bootstrap("committed-parent","success","success"); initial.put("namespace","iceberg.gold_qa_orc06_nonzero");
        var first=resolver.resolve(spark,initial); var writer=new GoldIcebergWriter(spark,"iceberg.gold_qa_orc06_nonzero",silver,()->{});
        var context=new GoldRunContext("committed-parent",start("2023-09"),end("2023-09"),LocalDate.of(2026,10,14),true,"test-v1",
                List.of(first.path("silver_bundle_uri").asText()));
        var transformed=GoldInputReader.read(spark,silver,"committed-parent",first.path("silver_bundle_sha256").asText(),context,
                Instant.parse("2026-10-14T00:00:00Z"),temporary.resolve("gold-baseline-readback"));
        var commit=writer.commit(transformed,new GoldCommitScope(context,first.path("silver_bundle_sha256").asText(),"test-v1",List.of("2023-09"),writer.baselines()));
        current=writer.baselines(); var before=new TreeMap<>(current); assertTrue(current.values().stream().anyMatch(id->id>0));
        var next=request("after-gold-commit","2023-09"); next.put("namespace","iceberg.gold_qa_orc06_nonzero"); baseline(next,first);
        var pin=(ObjectNode)next.path("baseline"); pin.put("commit_key","gold-commits/committed-parent/commit.json");
        pin.put("commit_sha256",sha(silver.read("gold-commits/committed-parent/commit.json")));
        var actualProbe=new FullMonthStateResolver(bronze,silver,List.of(FullMonthFixture.ENTRY),writer::baselines,()->{});
        var rebuilt=actualProbe.resolve(spark,next);
        assertEquals(before,writer.baselines()); assertEquals(first.path("reconciliation").path("current"),rebuilt.path("reconciliation").path("current"));
        var wrong=next.deepCopy(); wrong.put("run_id","wrong-commit-proof"); ((ObjectNode)wrong.path("baseline")).put("commit_sha256","0".repeat(64));
        assertThrows(IOException.class,()->actualProbe.resolve(spark,wrong)); assertFalse(silver.exists("full-month/wrong-commit-proof/_SUCCESS"));
    }

    @Test void s3KeyResolutionUsesFullConfiguredBronzePrefixNotAnInvalidProbeKey() throws Exception {
        java.util.function.Function<String,String> strict=key->{
            if(!key.startsWith("bronze/")) throw new IllegalArgumentException("PREFIX_REQUIRED");
            return "s3://japan-earthquake/"+key;
        };
        assertEquals("bronze/usgs/run_id=test/manifest.json",key("s3://japan-earthquake/bronze/usgs/run_id=test/manifest.json",strict));
        assertThrows(IOException.class,()->key("s3://wrong-bucket/bronze/usgs/manifest.json",strict));
        assertThrows(IOException.class,()->key("s3://japan-earthquake/bronze/../private/manifest.json",strict));
        assertThrows(IOException.class,()->key("s3://japan-earthquake/bronze/manifest.json?token=not-allowed",strict));
    }
}
