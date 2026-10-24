package ie212.earthquake.spark.jma;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.usgs.FullMonthFixture;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;

class JmaResearchReplayProfileJobTest {
    static Path fixture(String name) { return FullMonthFixture.repo().resolve("tests/fixtures/jma/archives/"+name+".zip"); }
    @Test void probesOnlyRequestedMonthsWithoutClaimingCoverageOrPublication() throws Exception {
        var file=fixture("success"); var report=JmaResearchReplayProfileJob.profile(file,"hypo.dat",sha(Files.readAllBytes(file)),2023,Set.of("2023-09","2023-10"));
        assertEquals("PASSED",report.path("combined_scope_status").asText());
        assertEquals(2,report.path("single_month_operations").size());
        assertTrue(report.path("single_month_operations").get(0).path("unique_revisions").asLong()>0);
        assertEquals(0,report.path("single_month_operations").get(1).path("unique_revisions").asLong());
        for(String key:List.of("research_ready","coverage_verified","gold_published","full_resolver_verified")) assertFalse(report.path(key).asBoolean(true));
        assertFalse(report.has("bronze_status"));
    }
    @Test void pinNativeYearAndMemberCannotBeGuessed() throws Exception {
        var file=fixture("success"); String hash=sha(Files.readAllBytes(file));
        assertThrows(IOException.class,()->JmaResearchReplayProfileJob.profile(file,"hypo.dat","0".repeat(64),2023,Set.of("2023-09")));
        assertThrows(IOException.class,()->JmaResearchReplayProfileJob.profile(file,"wrong",hash,2023,Set.of("2023-09")));
        assertThrows(IOException.class,()->JmaResearchReplayProfileJob.profile(file,"hypo.dat",hash,2000,Set.of("2000-09")));
        assertThrows(IOException.class,()->JmaResearchReplayProfileJob.profile(file,"hypo.dat",hash,2023,Set.of()));
    }
    @Test void validEmptyArchiveStaysZeroNotMissingCoverageProof() throws Exception {
        var file=fixture("empty"); var report=JmaResearchReplayProfileJob.profile(file,"hypo.dat",sha(Files.readAllBytes(file)),2023,Set.of("2023-09"));
        assertEquals(0,report.path("physical_records").asLong()); assertEquals(0,report.path("combined_unique_revisions").asLong());
        assertFalse(report.path("coverage_verified").asBoolean(true));
    }
}
