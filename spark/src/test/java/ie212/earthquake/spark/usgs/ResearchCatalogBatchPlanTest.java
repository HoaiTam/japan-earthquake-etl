package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static org.junit.jupiter.api.Assertions.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.*;
import org.junit.jupiter.api.Test;

class ResearchCatalogBatchPlanTest {
    @org.junit.jupiter.api.Test void v2ReservesTimeForLargerPinnedGoldReadbacksWithoutIncreasingParallelExecution() {
        org.junit.jupiter.api.Assertions.assertFalse(ResearchCatalogBatchPlan.mayStart(3599));
        org.junit.jupiter.api.Assertions.assertTrue(ResearchCatalogBatchPlan.mayStart(3600));
        org.junit.jupiter.api.Assertions.assertEquals(16,ResearchCatalogBatchPlan.SHUFFLE_PARTITIONS);
        org.junit.jupiter.api.Assertions.assertEquals(75000,ResearchCatalogBatchPlan.PROFILE_ROWS);
    }
    ObjectNode plan(){var p=JSON.createObjectNode().put("version","dat03-rollout-plan-v1");var rows=p.putArray("batches");
        for(int i=0;i<288;i++)rows.addObject().put("month",ResearchCatalogState.MONTHS.get(i)).put("split",i<225?"REPRODUCTION":"EXTENSION").put("profile_accepted_rows",10000);
        return p;}
    @Test void exactPackingCoversEveryMonthOnceNeverCrossesSplitAndDoesNotTruncateRows() throws Exception {
        var p=plan();var all=new ArrayList<String>();
        while(all.size()<288){var next=ResearchCatalogBatchPlan.next(p,all.size(),288-all.size());assertTrue(next.size()<=7);assertFalse(next.isEmpty());all.addAll(next);}
        assertEquals(ResearchCatalogState.MONTHS,all);assertEquals(288,new HashSet<>(all).size());
        assertEquals(List.of("2018-09"),ResearchCatalogBatchPlan.next(p,224,100));
    }
    @Test void denseMonthsAndMaxMonthsProduceWholeMonthBatchesNotSamples() throws Exception {
        var p=plan();((ObjectNode)p.path("batches").get(0)).put("profile_accepted_rows",57044);
        assertEquals(List.of("2000-01","2000-02"),ResearchCatalogBatchPlan.next(p,0,288));
        assertEquals(List.of("2000-01"),ResearchCatalogBatchPlan.next(p,0,1));
        assertEquals(List.of(),ResearchCatalogBatchPlan.next(p,288,0));
    }
    @Test void unknownLargeOrUntypedProfileNeedsReviewInsteadOfRelaxingThe100kGuard() throws Exception {
        for(Object count:List.of(100001,75001,-1,true,1.0,"1")) {var p=plan();((ObjectNode)p.path("batches").get(0)).set("profile_accepted_rows",JSON.valueToTree(count));
            assertThrows(Exception.class,()->ResearchCatalogBatchPlan.next(p,0,288));}
        var wrong=plan();((ObjectNode)wrong.path("batches").get(0)).put("month","1999-12");assertThrows(Exception.class,()->ResearchCatalogBatchPlan.next(wrong,0,288));
    }
    @Test void batchContextBindsNewCodeButKeepsOriginalRequestIdentityCutoffAndSourcePolicy() throws Exception {
        var r=ResearchCatalogStateTest.request();var inputs=JSON.createArrayNode();inputs.addObject().put("manifest_uri","exact");
        var context=ResearchCatalogState.context(r,List.of("2000-01","2000-02"),inputs,null,ResearchCatalogStateTest.pins(0),"c".repeat(64));
        assertEquals(JSON.readTree("[\"2000-01\",\"2000-02\"]"),context.path("resolver_request").path("target_months"));
        assertEquals("2000-03-01T00:00:00Z",context.path("window_end_utc").asText());
        assertEquals("jar-"+"c".repeat(16),context.path("resolver_request").path("code_version").asText());
        assertEquals(r.path("source_cutoff_utc"),context.path("resolver_request").path("source_cutoff_utc"));
        assertThrows(Exception.class,()->ResearchCatalogState.context(r,List.of("2018-09","2018-10"),inputs,null,ResearchCatalogStateTest.pins(0),"c".repeat(64)));
        assertThrows(Exception.class,()->ResearchCatalogState.context(r,List.of("2000-01","2000-03"),inputs,null,ResearchCatalogStateTest.pins(0),"c".repeat(64)));
    }
}
