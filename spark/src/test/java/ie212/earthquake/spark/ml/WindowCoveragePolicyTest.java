package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.*;

class WindowCoveragePolicyTest {
    static SparkSession spark;
    static final MainshockSelectionConfig SELECTION=MainshockSelectionConfig.reproductionBaseline();
    static final Instant START=SELECTION.periodStartUtc(),END=SELECTION.periodEndUtcExclusive();
    @BeforeAll static void start() {
        spark=SparkSession.builder().master("local[1]").appName("dat03-strict-split-fixture").config("spark.ui.enabled","false")
                .config("spark.driver.host","127.0.0.1").config("spark.driver.bindAddress","127.0.0.1")
                .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","1").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }
    @AfterAll static void stop() { if(spark!=null) spark.stop(); }
    static Row window(String id,Instant t,Instant from,Instant to) {
        return RowFactory.create(id,Timestamp.from(t),Timestamp.from(from),Timestamp.from(to));
    }
    static Dataset<Row> boundaries() {
        var schema=new StructType().add("mainshock_event_id","string").add("mainshock_time_utc","timestamp")
                .add("window_start_utc","timestamp").add("window_end_utc","timestamp");
        return spark.createDataFrame(List.of(
                window("pre",START.plusSeconds(3600),START.minusNanos(1000),START.plusSeconds(7200)),
                window("post",END.minusSeconds(3600),END.minusSeconds(7200),END),
                window("both",START.plusSeconds(3600),START.minusSeconds(1),END),
                window("complete",START.plusSeconds(3600),START,END.minusNanos(1000))),schema);
    }
    @Test void exactStartAndLastMicrosecondPassButInclusiveEndFailsAndEveryExclusionIsExplained() {
        var decision=WindowCoveragePolicy.apply(boundaries(),SELECTION,WindowCoveragePolicy.STRICT);
        assertEquals(List.of("complete"),decision.included().select("mainshock_event_id").as(org.apache.spark.sql.Encoders.STRING()).collectAsList());
        var report=decision.report(); assertEquals(4,report.path("eligible_mainshock_count").asLong());
        assertEquals(3,report.path("excluded_mainshock_count").asLong());
        assertEquals("PRE_AND_POST_OUTSIDE_SPLIT",report.path("excluded_mainshocks").get(0).path("reason_code").asText());
        assertEquals("POST_OUTSIDE_SPLIT",report.path("excluded_mainshocks").get(1).path("reason_code").asText());
        assertEquals("PRE_OUTSIDE_SPLIT",report.path("excluded_mainshocks").get(2).path("reason_code").asText());
        assertFalse(report.path("windows_clipped").asBoolean(true)); assertFalse(report.path("cross_split_data_used").asBoolean(true));
        assertEquals(report,WindowCoveragePolicy.apply(boundaries().repartition(2),SELECTION,WindowCoveragePolicy.STRICT).report());
    }
    @Test void malformedOrRehashedExclusionReportsCannotPassSemanticVerification() {
        var report=WindowCoveragePolicy.apply(boundaries(),SELECTION,WindowCoveragePolicy.STRICT).report();
        for(String key:List.of("eligible_mainshock_count","included_mainshock_count","excluded_mainshock_count","windows_clipped","reason_counts")) {
            var changed=report.deepCopy(); changed.put(key,"forged");
            assertThrows(IllegalArgumentException.class,()->WindowCoveragePolicy.validateReport(changed,1));
        }
        var changed=report.deepCopy(); changed.withArray("excluded_mainshocks").add(changed.path("excluded_mainshocks").get(0));
        assertThrows(IllegalArgumentException.class,()->WindowCoveragePolicy.validateReport(changed,1));
        assertThrows(IllegalArgumentException.class,()->WindowCoveragePolicy.validateReport(report,2));
    }
    @Test void absentPolicyKeepsLegacyAndUnknownPolicyNeverFallsBack() throws Exception {
        var manifest=WindowArtifactInput.JSON.createObjectNode().put("filter_config_json","{}");
        assertEquals(WindowCoveragePolicy.LEGACY,WindowCoveragePolicy.fromManifest(manifest));
        assertEquals(4,WindowCoveragePolicy.apply(boundaries(),SELECTION,WindowCoveragePolicy.LEGACY).included().count());
        for(String value:List.of("null","true","\"clip\"","\"dat03-strict-split-v2\"")) {
            manifest.put("filter_config_json","{\"window_coverage_policy\":"+value+"}");
            assertThrows(IllegalArgumentException.class,()->WindowCoveragePolicy.fromManifest(manifest));
        }
    }
    @Test void boundaryExcludedMainshockRemainsAnOrdinaryCandidateOfACompleteWindow() {
        var schema=new StructType().add("canonical_event_id","string").add("event_time_utc","timestamp")
                .add("latitude","double").add("longitude","double").add("depth_km","double").add("magnitude","double").add("is_eligible","boolean");
        Instant time=Instant.parse("2005-06-01T00:00:00Z");
        var selection=new MainshockSelectionConfig("fixture",50,200,5.5,"REPRODUCTION",time,time.plusSeconds(86400*30),10000,false);
        var model=new WindowModelConfig(WindowModelType.UHRHAMMER,"fixture-window",0,24,24.,24.,250.,null,null,null,null);
        var events=spark.createDataFrame(List.of(
                RowFactory.create("excluded-pre",Timestamp.from(time.plusSeconds(3600)),35.,140.,80.,6.,true),
                RowFactory.create("complete",Timestamp.from(time.plusSeconds(86400)),35.,140.,80.,6.,true)),schema);
        var result=new MainshockWindowEngine().execute(events,selection,model,2,"ds_fixture",time,WindowCoveragePolicy.STRICT);
        result.assertInvariants(); assertEquals(1,result.mainshockCount());
        assertEquals(1,result.candidateSnapshot().filter(col("candidate_event_id").equalTo("excluded-pre")).count());
        assertEquals(0,result.mainshockSnapshot().filter(col("mainshock_event_id").equalTo("excluded-pre")).count());
        assertEquals(1,result.windowCoverageReport().path("excluded_mainshock_count").asInt());
        assertTrue(result.toJsonNode().has("window_coverage"));
    }
    @Test void allExcludedAndEmptyAreReportedWithoutInventingASelfWindow() {
        var only=boundaries().filter(not(col("mainshock_event_id").equalTo("complete")));
        var report=WindowCoveragePolicy.apply(only,SELECTION,WindowCoveragePolicy.STRICT);
        assertEquals(0,report.included().count()); assertEquals(3,report.report().path("excluded_mainshock_count").asInt());
        var empty=WindowCoveragePolicy.apply(only.limit(0),SELECTION,WindowCoveragePolicy.STRICT);
        WindowCoveragePolicy.validateReport(empty.report(),0);
    }
}
