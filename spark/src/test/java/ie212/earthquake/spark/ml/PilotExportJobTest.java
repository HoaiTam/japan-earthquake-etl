package ie212.earthquake.spark.ml;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.node.*;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

class PilotExportJobTest {
    @org.junit.jupiter.api.io.TempDir Path temporary;
    static SparkSession spark;
    static WindowArtifactInput input;
    static final String MID = "evt_" + "a".repeat(32), SHA = "b".repeat(64);
    static final com.fasterxml.jackson.databind.ObjectMapper JSON = WindowArtifactInput.JSON;
    @BeforeAll static void start() {
        spark = LocalFeatureJob.session();
        var schema = new StructType().add("canonical_event_id", "string", false).add("event_time_utc", "timestamp", false)
            .add("latitude", "double").add("longitude", "double").add("depth_km", "double").add("magnitude", "double").add("is_eligible", "boolean");
        Instant centre = Instant.parse("2000-07-01T00:00:00Z");
        var data = spark.createDataFrame(List.of(RowFactory.create(MID, Timestamp.from(centre), 35., 140., 80., 6., true),
            RowFactory.create("evt_" + "c".repeat(32), Timestamp.from(centre.plusSeconds(3600)), 35.01, 140.01, 81., 3., true)), schema);
        var windows = new MainshockWindowEngine().execute(data, MainshockSelectionConfig.reproductionBaseline(), WindowModelConfig.expandedV1(), 1., "ds_fixture", centre);
        var manifest = JSON.createObjectNode().put("dataset_id", "ds_fixture").put("period_start_utc", "2000-01-01T00:00:00Z")
            .put("period_end_utc_exclusive", "2018-10-01T00:00:00Z");
        var dataset = JSON.createObjectNode(); dataset.set("manifest", manifest); dataset.set("audit", JSON.createObjectNode());
        dataset.set("window", JSON.createObjectNode().put("mainshock_content_sha256", "m").put("candidate_content_sha256", "c"));
        dataset.set("window_manifest", JSON.createObjectNode());
        var nativeReport = JSON.createObjectNode(); var source = nativeReport.putArray("sources").addObject();
        source.putObject("pin").put("source_system", "JMA_BULLETIN");
        source.put("sample_start_utc", "2000-06-24T00:00:00Z").put("sample_end_utc_exclusive", "2000-07-08T00:00:00Z");
        nativeReport.set("publication", JSON.createObjectNode());
        var receipt = JSON.createObjectNode().put("native_report_sha256", SHA); receipt.putArray("files");
        input = new WindowArtifactInput(nativeReport, dataset, receipt, windows.mainshockSnapshot().localCheckpoint(true), windows.candidateSnapshot().localCheckpoint(true));
    }
    @AfterAll static void stop() { spark.stop(); }
    static ObjectNode selection() {
        var config = JSON.createObjectNode().put("selection_version", "fixture-v1").put("native_report_sha256", SHA)
            .put("source_dataset_id", "ds_fixture").put("selection_reason", "fixed before fit"); config.putArray("mainshock_ids").add(MID); return config;
    }
    static ObjectNode scaling() {
        return JSON.createObjectNode().put("feature_version", "relative-4d-v1").put("scaling_config_version", "fixture-v1").put("mode", "GLOBAL")
            .put("space_scale_km", 50.).put("depth_scale_km", 30.).put("time_scale_hours", 720.).put("max_candidates_per_window", 10000)
            .put("coordinate_tolerance_km", 1e-8).put("time_tolerance_hours", 1e-12);
    }
    @Test void exportsAllCandidatesAndIsByteIdenticalAcrossIndependentRootsAndRerun() throws Exception {
        var first = PilotExportJob.export(input, selection(), scaling(), temporary.resolve("one"), SHA);
        var second = PilotExportJob.export(input, selection(), scaling(), temporary.resolve("two"), SHA);
        assertEquals(first.path("pilot_id"), second.path("pilot_id")); assertEquals(first.path("checksums"), second.path("checksums"));
        assertEquals(2, first.path("feature_count").asInt()); assertEquals("PARTIAL", first.path("coverage_status").asText());
        assertTrue(PilotExportJob.export(input, selection(), scaling(), temporary.resolve("one"), SHA).path("reused").asBoolean());
        var changed = PilotExportJob.export(input, selection(), scaling().put("time_scale_hours", 721.), temporary.resolve("one"), SHA);
        assertNotEquals(first.path("pilot_id"), changed.path("pilot_id"));
        Path dir = temporary.resolve("one/pilot_id=" + first.path("pilot_id").asText());
        assertEquals(5, LocalFeatureJob.hashes(dir).size()); assertFalse(Files.exists(dir.resolve("_SUCCESS.json")));
        var manifest = JSON.readTree(Files.readString(dir.resolve("pilot_manifest.json")));
        assertEquals("DEVELOPMENT_ONLY", manifest.path("allowed_usage").asText()); assertFalse(manifest.path("research_ready").asBoolean(true));
        Files.writeString(dir.resolve("coverage_report.json"), "tampered");
        assertThrows(IllegalStateException.class, () -> PilotExportJob.export(input, selection(), scaling(), temporary.resolve("one"), SHA));
    }
    @Test void rejectsBadSelectionMissingSelfAndNeverWritesPartialBundle() {
        var selection = selection(); selection.withArray("mainshock_ids").add(MID);
        assertThrows(IllegalArgumentException.class, () -> PilotExportJob.selection(selection, SHA, input.dataset()));
        assertThrows(IllegalArgumentException.class, () -> PilotExportJob.selection(selection(), "0".repeat(64), input.dataset()));
        var bad = new WindowArtifactInput(input.nativeReport(), input.dataset(), input.receipt(), input.mainshocks(), input.candidates().filter(not(col("is_mainshock"))));
        Path out = temporary.resolve("invalid");
        assertThrows(IllegalStateException.class, () -> PilotExportJob.export(bad, selection(), scaling(), out, SHA));
        assertFalse(Files.exists(out));
    }
    @Test void coverageDoesNotClipTheScientificWindowToTheFourteenDaySlice() {
        var report = PilotExportJob.coverage(input.mainshocks(), input.nativeReport(), input.dataset().path("manifest"));
        assertEquals("PARTIAL", report.path("status").asText()); assertFalse(report.path("windows").get(0).path("missing_ranges").isEmpty());
        var complete = input.nativeReport().deepCopy(); var source = (ObjectNode) complete.path("sources").get(0);
        source.put("sample_start_utc", "2000-01-01T00:00:00Z").put("sample_end_utc_exclusive", "2001-01-01T00:00:00Z");
        assertEquals("COMPLETE", PilotExportJob.coverage(input.mainshocks(), complete, input.dataset().path("manifest")).path("status").asText());
        assertNotEquals(PilotExportJob.identity(JSON.createObjectNode().put("input", "old")), PilotExportJob.identity(JSON.createObjectNode().put("input", "new")));
    }
    @Test void confinesPathsRejectsSymlinksAndRequiresUntamperedBytesBeforeAnySql() throws Exception {
        Path file = temporary.resolve("a"); Files.writeString(file, "a"); assertEquals(file, WindowArtifactInput.safe(temporary, "a"));
        assertThrows(Exception.class, () -> WindowArtifactInput.safe(temporary, "../other"));
        Files.createSymbolicLink(temporary.resolve("link"), file);
        assertThrows(Exception.class, () -> WindowArtifactInput.safe(temporary, "link"));
        var receipt = JSON.createObjectNode().put("input_version", "mld04-window-input-v1").put("split", "REPRODUCTION").put("native_report_sha256", SHA);
        receipt.putArray("files").addObject().put("path", "a").put("bytes", 1).put("sha256", "0".repeat(64));
        LocalFeatureJob.json(temporary.resolve("source-input.json"), receipt);
        assertThrows(java.io.IOException.class, () -> WindowArtifactInput.load(spark, temporary, SHA, sql -> { fail("SQL must not run before byte validation"); return null; }));
    }
}
