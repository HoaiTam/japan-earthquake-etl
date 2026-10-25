package ie212.earthquake.spark.gold;

import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

class ResearchGoldCapacityJobTest {
    ObjectNode request() {
        return GoldPublicationTest.JSON.createObjectNode().put("version","dat03-spark-gold-capacity-v1")
                .put("namespace","iceberg.dat03_e2e_"+"1".repeat(32)).put("table","event_current").put("snapshot_id",9079127511755137392L)
                .put("source_count",105282).put("copies",43).put("shuffle_partitions",1).put("required_rows",4489823)
                .put("runner_sha256","2".repeat(64)).put("writer_class_sha256","3".repeat(64));
    }
    @Test void exactFullScaleWorkloadRetainsTypedBigintPinAndReviewedProfiles() throws Exception {
        var r=request();ResearchGoldCapacityJob.validate(r);
        r.put("table","event_source_bridge").put("shuffle_partitions",16);ResearchGoldCapacityJob.validate(r);
    }
    @Test void partialBooleanSharedNamespaceAndExtraClaimsCannotStartSpark() {
        for(int i=0;i<7;i++) {
            var r=request();
            switch(i) {
                case 0->r.put("copies",32);case 1->r.put("source_count",true);case 2->r.put("namespace","iceberg.gold");
                case 3->r.put("table","dataset_manifest");case 4->r.put("shuffle_partitions",8);case 5->r.put("snapshot_id",0);case 6->r.put("research_ready",true);
            }
            assertThrows(Exception.class,()->ResearchGoldCapacityJob.validate(r));
        }
    }
}
