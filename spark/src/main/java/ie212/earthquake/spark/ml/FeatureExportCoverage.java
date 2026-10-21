package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.FullMonthStateResolver;
import ie212.earthquake.spark.silver.*;
import java.time.*;
import java.util.*;

/** Coverage is read from receipt-bound ETL state, never a request research_ready boolean. */
public final class FeatureExportCoverage {
    private FeatureExportCoverage() { }
    public static ObjectNode resolve(SilverObjectStore store, JsonNode publication, JsonNode manifest) throws Exception {
        String run = publication.path("gold_run_id").asText(); String root = FullMonthStateResolver.root(run);
        byte[] commitBytes = store.read("gold-commits/" + run + "/commit.json");
        var commit = JSON.readTree(commitBytes);
        require(SilverBundlePublisher.sha(commitBytes).equals(publication.path("bundle_sha256").asText())
            && ("pub_" + SilverBundlePublisher.sha(commitBytes)).equals(manifest.path("gold_publication_id").asText())
            && commit.path("tables").path("event_current").path("snapshot_id").equals(manifest.path("gold_snapshot_id")), "DS_EXPORT_GOLD_PIN_CONFLICT");
        require(store.exists(root + "/registry.json") && store.exists(root + "/receipt.json") && store.exists(root + "/_SUCCESS"), "DS_EXPORT_COVERAGE_REQUIRED");
        byte[] registry = store.read(root + "/registry.json"), receiptBytes = store.read(root + "/receipt.json");
        var receipt = JSON.readTree(receiptBytes); var state = JSON.readTree(registry);
        require(Arrays.equals(store.read(root + "/_SUCCESS"), SilverBundlePublisher.sha(receiptBytes).getBytes(java.nio.charset.StandardCharsets.US_ASCII))
            && receipt.path("registry_sha256").asText().equals(SilverBundlePublisher.sha(registry))
            && "FullMonthReady".equals(receipt.path("status").asText()) && run.equals(receipt.path("run_id").asText())
            && run.equals(state.path("run_id").asText()) && commit.path("namespace").equals(state.path("namespace"))
            && receipt.path("silver_bundle_sha256").equals(state.path("silver_bundle_sha256"))
            && commit.path("scope").path("silver_bundle_sha256").equals(state.path("silver_bundle_sha256"))
            && !Instant.parse(state.path("source_cutoff_utc").asText()).isAfter(Instant.parse(manifest.path("observation_cutoff_utc").asText())), "DS_EXPORT_COVERAGE_PIN_CONFLICT");
        new SilverBundlePublisher(store).verify(run);
        require(SilverBundlePublisher.sha(store.read(SilverBundlePublisher.root(run) + "/manifest.json"))
            .equals(state.path("silver_bundle_sha256").asText()), "DS_EXPORT_COVERAGE_PIN_CONFLICT");
        var coverage = validate(state, manifest); coverage.put("registry_sha256", SilverBundlePublisher.sha(registry)); return coverage;
    }
    public static ObjectNode validate(JsonNode state, JsonNode manifest) throws Exception {
        // Same split policy as the audit engine, before considering any coverage claim.
        GoldAuditConfig.fromManifest(manifest, "mld05-coverage", "audit-report.json");
        var registered = new TreeSet<String>();
        for (var month : state.path("complete_months")) require(month.isTextual() && month.asText().matches("[12][0-9]{3}-(0[1-9]|1[0-2])")
            && registered.add(month.asText()), "DS_EXPORT_COVERAGE_INVALID");
        require(registered.size() <= 480, "DS_EXPORT_COVERAGE_INVALID");
        var jma = new TreeSet<String>();
        for (var item : state.path("coverage")) if ("JMA_BULLETIN".equals(item.path("source_system").asText())) {
            String month = item.path("month").asText();
            require("COMPLETE".equals(item.path("coverage").asText()) && registered.contains(month) && jma.add(month)
                && item.path("manifest_uris").isArray() && !item.path("manifest_uris").isEmpty(), "DS_EXPORT_COVERAGE_INVALID");
        }
        var missing = JSON.createArrayNode();
        YearMonth first = YearMonth.from(Instant.parse(manifest.path("period_start_utc").asText()).atZone(ZoneOffset.UTC));
        YearMonth end = YearMonth.from(Instant.parse(manifest.path("period_end_utc_exclusive").asText()).atZone(ZoneOffset.UTC));
        int required = 0;
        for (YearMonth m = first; m.isBefore(end); m = m.plusMonths(1)) { required++; if (!jma.contains(m.toString())) missing.add(m.toString()); }
        require(missing.isEmpty(), "DS_EXPORT_COVERAGE_INCOMPLETE:" + missing.size() + "_MONTHS");
        return JSON.createObjectNode().put("coverage_version", "mld05-receipt-bound-months-v1").put("status", "COMPLETE")
            .put("required_month_count", required).put("source_system", "JMA_BULLETIN");
    }
}
