package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DatasetIdentityPlanTest {
    private static final Map<String,String> VERSIONS = Map.of("dataset_config_version","1", "audit_rule_version","1",
        "mc_method_version","1", "window_model_version","1", "feature_version","1", "scaling_config_version","1", "code_version","abc123");
    private DatasetIdentityPlan plan(long snapshot, String filters, String run, String date) throws Exception {
        return new DatasetIdentityPlan(new DatasetIdentityPlan.SnapshotFixture(snapshot,"fixture-pub","Published"),
            "REPRODUCTION", Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("2018-10-01T00:00:00Z"),
            Instant.parse("2024-01-01T00:00:00Z"), new ObjectMapper().readTree(filters), VERSIONS, run, Instant.parse(date));
    }
    @Test void stableAcrossNestedKeyOrderRetryAndTimestamp() throws Exception {
        var a=plan(7,"{\"natural\":true,\"range\":{\"min\":1,\"max\":2}}","run1","2024-01-01T00:00:00Z");
        var b=plan(7,"{\"range\":{\"max\":2,\"min\":1},\"natural\":true}","run2","2024-01-02T00:00:00Z");
        assertEquals(a.datasetId(),b.datasetId()); a.assertRerun(b.datasetId(),b.canonicalPayload());
        assertEquals("BUILDING",a.buildingManifestFixture().get("dataset_status").asText());
        assertFalse(a.canMaterializePublishedDataset());
    }
    @Test void snapshotAndConfigChangeIdentityAndDetectConflict() throws Exception {
        var a=plan(7,"{\"natural\":true}","r","2024-01-01T00:00:00Z");
        assertNotEquals(a.datasetId(),plan(8,"{\"natural\":true}","r","2024-01-01T00:00:00Z").datasetId());
        assertNotEquals(a.datasetId(),plan(7,"{\"natural\":false}","r","2024-01-01T00:00:00Z").datasetId());
        assertThrows(IllegalArgumentException.class,()->a.assertRerun(a.datasetId(),"other"));
    }
    @Test void unpublishedAndNonPositiveSnapshotRejected() {
        assertThrows(IllegalArgumentException.class,()->new DatasetIdentityPlan.SnapshotFixture(1,"p","Committed"));
        assertThrows(IllegalArgumentException.class,()->new DatasetIdentityPlan.SnapshotFixture(0,"p","Published"));
    }
    @Test void exactHalfOpenPeriods() throws Exception {
        var snapshot=new DatasetIdentityPlan.SnapshotFixture(7,"p","Published");
        var filters=new ObjectMapper().readTree("{\"natural\":true}");
        var boundary=Instant.parse("2018-10-01T00:00:00Z");
        var extension=new DatasetIdentityPlan(snapshot,"EXTENSION",boundary,Instant.parse("2024-01-01T00:00:00Z"),boundary,filters,VERSIONS,"r",boundary);
        assertEquals(boundary.toString(),extension.buildingManifestFixture().get("period_start_utc").asText());
        assertThrows(IllegalArgumentException.class,()->new DatasetIdentityPlan(snapshot,"EXTENSION",boundary.minusSeconds(1),Instant.parse("2024-01-01T00:00:00Z"),boundary,filters,VERSIONS,"r",boundary));
    }
    @Test void invalidVersionsAndUnsafeFilterRejected() throws Exception {
        assertThrows(IllegalArgumentException.class,()->plan(7,"{}","r","2024-01-01T00:00:00Z"));
        assertThrows(IllegalArgumentException.class,()->plan(7,"{\"path\":\"/home/person/data\"}","r","2024-01-01T00:00:00Z"));
        var time=Instant.parse("2018-10-01T00:00:00Z");
        assertThrows(IllegalArgumentException.class,()->new DatasetIdentityPlan(new DatasetIdentityPlan.SnapshotFixture(1,"p","Published"),"REPRODUCTION",Instant.parse("2000-01-01T00:00:00Z"),time,time,new ObjectMapper().readTree("{\"natural\":true}"),Map.of(),"r",time));
    }
}
