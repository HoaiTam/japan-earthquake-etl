package ie212.earthquake.spark.usgs;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class PipelineQaSafetyTest {
    @Test void acceptanceCannotTargetProductionNamespacesBeforeLeaseOrSparkAccess() {
        for(String name:new String[]{"iceberg.gold","iceberg.ml","iceberg.qa03_old","iceberg.qa04_"+"a".repeat(32),"iceberg.qa02_old","iceberg.qa02_"+"a".repeat(33),"iceberg.qa02_"+"g".repeat(32)}) {
            var request=JSON.createObjectNode().put("namespace",name);
            assertEquals("ISOLATED_PIPELINE_QA_REQUIRED",assertThrows(java.io.IOException.class,
                ()->new PipelineQaSupport(request,Path.of("unused"))).getMessage());
        }
    }
    @Test void fixedJobsRejectMissingRequestsWithoutCreatingAnyState() {
        assertThrows(java.io.IOException.class,()->RerunRevisionAcceptanceJob.main(new String[0]));
        assertThrows(java.io.IOException.class,()->HistoricalBackfillAcceptanceJob.main(new String[0]));
    }
    @Test void controlledMagnitudeRevisionUsesExactlyTheDocumentedColumns() throws Exception {
        String row=java.nio.file.Files.readAllLines(FullMonthFixture.repo().resolve("tests/fixtures/jma/fixed-width/success.hyp")).get(0);
        String revised=row.substring(0,52)+"53"+row.substring(54);
        assertEquals(row.length(),revised.length());assertEquals("52",row.substring(52,54));
        assertEquals(row.substring(0,52),revised.substring(0,52));assertEquals(row.substring(54),revised.substring(54));
        assertEquals(ie212.earthquake.spark.silver.JmaFixedWidthParser.originTimeOfVerifiedRecord(row.getBytes()),
            ie212.earthquake.spark.silver.JmaFixedWidthParser.originTimeOfVerifiedRecord(revised.getBytes()));
    }
    @Test void historicalStopIsAnActualControlledExceptionNotAProcessCrash() {
        assertEquals("QA_STOP_AFTER_DURABLE_SILVER",assertThrows(java.io.IOException.class,
            HistoricalBackfillAcceptanceJob::failAfterSilver).getMessage());
    }
}
