package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PortableImportContractTest {
    static ObjectNode fixture() {
        var json = PortableImportContract.JSON; String hash = "a".repeat(64);
        var r = json.createObjectNode().put("version", PortableImportContract.VERSION).put("mode", "PREPARE_ONLY")
                .put("run_id", "dat03-import-" + "a".repeat(32)).put("code_sha256", hash).put("processing_date", "2026-11-15")
                .put("processed_at_utc", "2026-11-15T01:00:00Z").put("source_cutoff_utc", "2026-10-25T06:13:04.747Z");
        var context = r.putObject("run_context").put("run_id", r.path("run_id").asText()).put("processing_date", "2026-11-15")
                .put("window_start_utc", "2000-01-01T00:00:00Z").put("window_end_utc", "2024-01-01T00:00:00Z")
                .put("is_backfill", true).put("config_version", PortableImportContract.VERSION);
        context.putArray("input_manifest_uris").add("mock://verified/source");
        r.putObject("bronze_plan").put("relative_path", "inputs/bronze-plan.json").put("sha256", hash);
        r.putObject("baseline_acceptance").put("relative_path", "inputs/baseline-acceptance.json").put("sha256", hash);
        var b = r.putObject("baseline").put("namespace", "iceberg.dat03_catalog_" + "b".repeat(32)).put("gold_run_id", "baseline-run")
                .put("bundle_sha256", hash).put("acceptance_file_sha256", hash).put("completed_months", 163);
        var tables = b.putObject("tables");
        for (String table : GoldIcebergWriter.TABLES) tables.putObject(table).put("snapshot_id", 9007199254740993L).put("count", 1);
        var batches = r.putArray("worker_batches"); int n = 0;
        for (String owner : List.of("ThanhTris", "rosy179")) {
            var months = owner.equals("ThanhTris") ? PortableImportContract.months("2013-08", "2018-10") : PortableImportContract.months("2018-10", "2024-01");
            for (int i = 0; i < months.size(); i += 12) {
                var batch = batches.addObject().put("owner", owner).put("plan_sha256", hash).put("original_plan_sha256", hash)
                        .put("plan_file", String.format("inputs/worker-%02d.json", ++n)).put("observation_count", 1).put("canonical_count", 1).put("source_link_count", 0);
                batch.set("months", json.valueToTree(months.subList(i, Math.min(i + 12, months.size())))); batch.putArray("run_ids").add("run-" + n);
            }
        }
        return r;
    }
    static ObjectNode pins() {
        var root = PortableImportContract.JSON.createObjectNode(); var array = root.putArray("pins");
        for (int y = 2000; y <= 2024; y++) array.addObject().put("source_system", "JMA_BULLETIN").put("year", y).put("segment", "full-year")
                .put("manifest_uri", "s3://lake/bronze/jma/year=" + y + "/manifest.json").put("manifest_sha256", "a".repeat(64));
        for (String value : PortableImportContract.months("2023-01", "2024-01")) {
            YearMonth m = YearMonth.parse(value); var start = m.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant(); var end = m.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
            array.addObject().put("source_system", "USGS").put("manifest_uri", "s3://lake/bronze/usgs/month=" + value + "/manifest.json")
                    .put("manifest_sha256", "b".repeat(64)).put("window_start_utc", start.toString()).put("window_end_utc", end.plusSeconds(16).toString());
        }
        return root;
    }
    @Test void exactFullScopePreservesInt64Pins() { assertDoesNotThrow(() -> PortableImportContract.validate(fixture())); }
    @Test void gapOverlapAndReorderedMonthsAreRejected() {
        for (String mutation : List.of("gap", "overlap", "order")) {
            var r = fixture(); var months = (com.fasterxml.jackson.databind.node.ArrayNode) r.path("worker_batches").get(0).path("months");
            if (mutation.equals("gap")) months.remove(1); else if (mutation.equals("overlap")) months.add(months.get(0)); else {var first = months.get(0); months.set(0, months.get(1)); months.set(1, first);}
            assertThrows(Exception.class, () -> PortableImportContract.validate(r));
        }
    }
    @Test void duplicateSourceRunIsNotSilentlyReadTwice() {
        var r = fixture(); ((ObjectNode) r.path("worker_batches").get(1)).withArray("run_ids").set(0, r.path("worker_batches").get(0).path("run_ids").get(0));
        assertThrows(Exception.class, () -> PortableImportContract.validate(r));
    }
    @Test void callerReadinessFlagsAndGoldWritesAreForbidden() {
        var r = fixture().put("research_ready", true); assertThrows(Exception.class, () -> PortableImportContract.validate(r));
        var copy = fixture().put("mode", "COMMIT_GOLD"); assertThrows(Exception.class, () -> PortableImportContract.validate(copy));
    }
    @Test void fractionalOrOverflowedSnapshotPinsAreRejected() {
        for (String value : List.of("1.5", "9223372036854775808")) {
            var r = fixture(); ((ObjectNode) r.path("baseline").path("tables").path("event_current")).set("snapshot_id", assertDoesNotThrow(() -> PortableImportContract.JSON.readTree(value)));
            assertThrows(Exception.class, () -> PortableImportContract.validate(r));
        }
    }
    @Test void batchCountTypesAndLimitsAreStrict() {
        var r = fixture(); ((ObjectNode) r.path("worker_batches").get(0)).put("observation_count", 0.5);
        assertThrows(Exception.class, () -> PortableImportContract.validate(r));
    }
    @Test void newCoverageIncludesTheDecemberUtcJstAndUsgsSeedBoundary() throws Exception {
        var report = PortableImportContract.coverage(pins().path("pins"), "c".repeat(64));
        assertEquals(288, report.path("months").size());
        var december = report.path("months").get(287); assertEquals(2, december.path("jma_physical_manifest_uris").size());
        assertTrue(december.path("jma_physical_manifest_uris").get(1).asText().contains("2024"));
        var seed = report.path("months").get(275); assertEquals("2022-12", seed.path("month").asText());
        assertEquals(1, seed.path("usgs_physical_manifest_uris").size()); assertFalse(report.path("official_export_ready").asBoolean());
    }
    @Test void absentNextYearOrAmbiguousUsgsCoverageCannotPass() {
        var p = pins(); ((com.fasterxml.jackson.databind.node.ArrayNode) p.path("pins")).remove(24);
        assertThrows(Exception.class, () -> PortableImportContract.coverage(p.path("pins"), "c".repeat(64)));
        var q = pins(); ((ObjectNode) q.path("pins").get(25)).put("window_end_utc", "2023-01-15T00:00:00Z");
        assertThrows(Exception.class, () -> PortableImportContract.coverage(q.path("pins"), "c".repeat(64)));
    }
}
