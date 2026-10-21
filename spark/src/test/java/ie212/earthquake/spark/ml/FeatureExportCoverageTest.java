package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.*;
import org.junit.jupiter.api.Test;

class FeatureExportCoverageTest {
    static ObjectNode manifest() {
        var config = GoldAuditAcceptanceJob.datasetConfig("REPRODUCTION", Instant.parse("2026-10-21T00:00:00Z"), "fixture-v1");
        var pub = FeatureExportBundle.JSON.createObjectNode().put("verify_status", "PASSED").put("gold_snapshot_id", 123L)
            .put("gold_table_name", "lake.gold.earthquake_event_current").put("physical_gold_table_name", "lake.gold.event_current")
            .put("gold_publication_id", "pub_" + "b".repeat(64)).put("publication_bundle_sha256", "b".repeat(64));
        return (ObjectNode)new DatasetIdentityPlan(pub, config, "fixture", Instant.parse("2026-10-21T00:00:00Z")).manifest();
    }
    static ObjectNode full() {
        var state = FeatureExportBundle.JSON.createObjectNode(); var months = state.putArray("complete_months"); var coverage = state.putArray("coverage");
        for (YearMonth m = YearMonth.of(2000,1); m.isBefore(YearMonth.of(2018,10)); m = m.plusMonths(1)) {
            months.add(m.toString()); coverage.addObject().put("month", m.toString()).put("source_system", "JMA_BULLETIN")
                .put("coverage", "COMPLETE").putArray("manifest_uris").add("s3://fixture/bronze/manifest.json");
        }
        return state;
    }
    @Test void fullReproductionRequires225ReceiptBoundMonths() throws Exception {
        assertEquals(225, FeatureExportCoverage.validate(full(), manifest()).path("required_month_count").asInt());
    }
    @Test void smallRealPilotCannotBecomeOfficialByChangingResearchReadyFlag() {
        var partial = full(); partial.withArray("coverage").remove(0); partial.put("research_ready",true);
        assertTrue(assertThrows(java.io.IOException.class, () -> FeatureExportCoverage.validate(partial,manifest())).getMessage().startsWith("DS_EXPORT_COVERAGE_INCOMPLETE"));
    }
    @Test void duplicateOrUnregisteredCoverageFailsClosed() {
        var duplicate = full(); duplicate.withArray("coverage").add(duplicate.path("coverage").get(0));
        assertThrows(java.io.IOException.class, () -> FeatureExportCoverage.validate(duplicate,manifest()));
        var missing = full(); missing.withArray("complete_months").remove(0);
        assertThrows(java.io.IOException.class, () -> FeatureExportCoverage.validate(missing,manifest()));
    }
    @Test void incompleteSourceAndMissingManifestReferencesAreNotProof() {
        var bad = full(); ((ObjectNode)bad.path("coverage").get(0)).put("coverage","PARTIAL");
        assertThrows(java.io.IOException.class, () -> FeatureExportCoverage.validate(bad,manifest()));
        var empty = full(); ((ObjectNode)empty.path("coverage").get(0)).putArray("manifest_uris");
        assertThrows(java.io.IOException.class, () -> FeatureExportCoverage.validate(empty,manifest()));
    }
    @Test void changedResearchPeriodIsRejectedInsteadOfShrinkingScopeToPilot() {
        var bad = manifest().put("period_end_utc_exclusive","2000-02-01T00:00:00Z");
        assertThrows(IllegalArgumentException.class, () -> FeatureExportCoverage.validate(full(),bad));
    }
    @Test void malformedRequestFailsBeforeLeaseAndSpark() {
        assertThrows(java.io.IOException.class, () -> FeatureExportJob.main(new String[0]));
    }
    @Test void cumulativeRegistryIsNotTheAffectedWriteWindow() throws Exception {
        var months = full().path("complete_months");
        assertEquals(225,ie212.earthquake.spark.orchestration.FullMonthContract.registryMonths(months).size());
        assertThrows(java.io.IOException.class,()->ie212.earthquake.spark.orchestration.FullMonthContract.months(months));
        var overflow = FeatureExportBundle.JSON.createArrayNode();
        for (int i=0;i<481;i++) overflow.add(YearMonth.of(1984,1).plusMonths(i).toString());
        assertThrows(java.io.IOException.class,()->ie212.earthquake.spark.orchestration.FullMonthContract.registryMonths(overflow));
    }
    @Test void lifecycleSqlAcceptsSha256ColumnsAndStillRejectsUnsafeFieldNames() throws Exception {
        var json=FeatureExportBundle.JSON; var queries=new java.util.ArrayList<String>();
        ie212.earthquake.spark.gold.GoldPublisher.Sql sql=query->{
            queries.add(query);
            return new ie212.earthquake.spark.gold.GoldPublisher.Result(
                java.util.List.of(json.createObjectNode().put("name","dataset_status"),json.createObjectNode().put("name","feature_schema_sha256")),
                java.util.List.of(json.createArrayNode().add("VALIDATED").add("a".repeat(64))));
        };
        FeatureExportJob.update(sql,"iceberg.ml","ds_"+"b".repeat(64),"BUILDING",
            java.util.Map.of("dataset_status",json.getNodeFactory().textNode("VALIDATED"),"feature_schema_sha256",json.getNodeFactory().textNode("a".repeat(64))),()->{});
        assertTrue(queries.get(0).contains("feature_schema_sha256='"+"a".repeat(64)+"'"));
        queries.clear();
        assertThrows(java.io.IOException.class,()->FeatureExportJob.update(sql,"iceberg.ml","ds_"+"b".repeat(64),"BUILDING",
            java.util.Map.of("bad;DROP",json.getNodeFactory().textNode("invalid")),()->{}));
        assertTrue(queries.isEmpty());
    }
}
