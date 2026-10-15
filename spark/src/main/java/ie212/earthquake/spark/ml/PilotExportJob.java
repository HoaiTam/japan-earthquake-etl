package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.gold.TrinoSqlClient;
import java.net.URI;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.*;

/** DAT-02 development-only exporter; no INSERT/CREATE/UPDATE or official lifecycle marker. */
public final class PilotExportJob {
    static final ObjectMapper JSON = WindowArtifactInput.JSON;
    public static List<String> selection(JsonNode config, String nativeSha, JsonNode dataset) {
        Set<String> keys = new HashSet<>(); config.fieldNames().forEachRemaining(keys::add);
        if (!keys.equals(Set.of("selection_version", "native_report_sha256", "source_dataset_id", "mainshock_ids", "selection_reason"))
                || !config.path("selection_version").asText().matches("[a-z][a-z0-9_-]{0,95}")
                || !nativeSha.equals(config.path("native_report_sha256").asText())
                || !dataset.path("manifest").path("dataset_id").equals(config.path("source_dataset_id"))
                || config.path("selection_reason").asText().isBlank() || !config.path("mainshock_ids").isArray())
            throw new IllegalArgumentException("PILOT_EXACT_SELECTION_REQUIRED");
        var ids = new TreeSet<String>();
        for (var id : config.path("mainshock_ids")) if (!id.isTextual() || !id.asText().matches("evt_[a-f0-9]{32}") || !ids.add(id.asText()))
            throw new IllegalArgumentException("PILOT_INVALID_MAINSHOCK_SELECTION");
        if (ids.isEmpty() || ids.size() > 3) throw new IllegalArgumentException("PILOT_SELECT_ONE_TO_THREE_MAINSHOCKS");
        return List.copyOf(ids);
    }
    public static ObjectNode coverage(Dataset<Row> main, JsonNode nativeReport, JsonNode manifest) {
        var windows = JSON.createArrayNode(); boolean allComplete = true;
        Instant periodStart = Instant.parse(manifest.path("period_start_utc").asText());
        Instant periodEnd = Instant.parse(manifest.path("period_end_utc_exclusive").asText());
        var ranges = new ArrayList<Instant[]>();
        for (var source : nativeReport.path("sources")) {
            if (!"JMA_BULLETIN".equals(source.path("pin").path("source_system").asText()) || !source.has("sample_start_utc")) continue;
            Instant start = Instant.parse(source.path("sample_start_utc").asText());
            Instant end = Instant.parse(source.path("sample_end_utc_exclusive").asText());
            if (!start.isBefore(end)) throw new IllegalArgumentException("PILOT_INVALID_SOURCE_RANGE");
            start = start.isBefore(periodStart) ? periodStart : start; end = end.isAfter(periodEnd) ? periodEnd : end;
            if (start.isBefore(end)) ranges.add(new Instant[]{start, end});
        }
        ranges.sort(Comparator.comparing(r -> r[0]));
        if (ranges.isEmpty()) throw new IllegalArgumentException("PILOT_EXACT_SOURCE_COVERAGE_REQUIRED");
        for (Row row : main.orderBy("mainshock_event_id").collectAsList()) {
            Instant centre = row.<Timestamp>getAs("event_time_utc").toInstant();
            Instant start = centre.minusNanos(Math.round(row.<Double>getAs("pre_window_hours") * 3_600_000_000.0) * 1000);
            // Candidate membership includes the upper microsecond: encode the corresponding half-open range.
            Instant end = centre.plusNanos(Math.round(row.<Double>getAs("post_window_hours") * 3_600_000_000.0) * 1000).plusNanos(1000);
            var window = windows.addObject().put("mainshock_event_id", row.<String>getAs("mainshock_event_id"))
                .put("required_start_utc", start.toString()).put("required_end_utc_exclusive", end.toString())
                .put("candidate_count", row.<Long>getAs("candidate_count"));
            var available = window.putArray("available_ranges"); var missing = window.putArray("missing_ranges");
            Instant cursor = start;
            for (Instant[] range : ranges) {
                Instant a = range[0].isBefore(start) ? start : range[0]; Instant b = range[1].isAfter(end) ? end : range[1];
                if (!a.isBefore(b)) continue;
                available.addObject().put("start_utc", a.toString()).put("end_utc_exclusive", b.toString());
                if (a.isAfter(cursor)) missing.addObject().put("start_utc", cursor.toString()).put("end_utc_exclusive", a.toString());
                if (b.isAfter(cursor)) cursor = b;
            }
            if (cursor.isBefore(end)) missing.addObject().put("start_utc", cursor.toString()).put("end_utc_exclusive", end.toString());
            boolean complete = missing.isEmpty(); window.put("status", complete ? "COMPLETE" : "PARTIAL"); allComplete &= complete;
        }
        var report = JSON.createObjectNode().put("coverage_version", "dat02-temporal-scope-v1").put("status", allComplete ? "COMPLETE" : "PARTIAL")
            .put("research_ready", false).put("meaning", "Temporal input-scope coverage only; not catalog completeness or research approval");
        report.set("windows", windows); return report;
    }
    public static String identity(ObjectNode payload) { return "pilot_" + DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(payload).toString()); }
    public static ObjectNode export(WindowArtifactInput input, JsonNode selection, JsonNode scaling, Path root, String codeSha) throws Exception {
        var ids = selection(selection, input.receipt().path("native_report_sha256").asText(), input.dataset());
        var main = input.mainshocks().filter(col("mainshock_event_id").isin(ids.toArray()));
        var candidates = input.candidates().filter(col("mainshock_event_id").isin(ids.toArray()));
        if (main.count() != ids.size()) throw new IllegalArgumentException("PILOT_SELECTED_MAINSHOCK_MISSING");
        var result = new SpacetimeFeatureEngine().execute(main, candidates, FeatureScalingConfig.fromJson(scaling)); result.assertValid();
        if (result.inputCount() < ids.size() || result.inputCount() > 30000) throw new IllegalArgumentException("PILOT_RESOURCE_LIMIT_NO_TRUNCATION");
        var dataset = input.dataset(); var sourceManifest = dataset.path("manifest");
        String configText = DatasetIdentityPlan.canonical(scaling).toString(), selectionText = DatasetIdentityPlan.canonical(selection).toString();
        var payload = JSON.createObjectNode().put("identity_version", "dat02-identity-v1")
            .put("serialization_version", "dat02-parquet-snappy-micros-stable-footer-v1")
            .put("spark_version", result.features().sparkSession().version()).put("java_version", System.getProperty("java.version"))
            .put("native_report_sha256", input.receipt().path("native_report_sha256").asText())
            .put("source_dataset_id", sourceManifest.path("dataset_id").asText()).put("code_sha256", codeSha)
            .put("scaling_config_sha256", DatasetIdentityPlan.sha(configText)).put("selection_config_sha256", DatasetIdentityPlan.sha(selectionText))
            .put("audit_sha256", DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(dataset.path("audit")).toString()))
            .put("mainshock_input_sha256", dataset.path("window").path("mainshock_content_sha256").asText())
            .put("candidate_input_sha256", dataset.path("window").path("candidate_content_sha256").asText());
        String id = identity(payload);
        var features = result.features().withColumn("pilot_id", lit(id)); main = main.withColumn("pilot_id", lit(id));
        var coverage = coverage(main, input.nativeReport(), sourceManifest);
        Files.createDirectories(root); Path temporary = Files.createTempDirectory(root, ".pilot-prepare-");
        LocalFeatureJob.parquet(features, temporary.resolve("features-part-00000.parquet"), "dataset_id", "mainshock_event_id", "candidate_event_id");
        LocalFeatureJob.parquet(main, temporary.resolve("mainshocks.parquet"), "dataset_id", "mainshock_event_id");
        LocalFeatureJob.json(temporary.resolve("coverage_report.json"), coverage);
        var manifest = JSON.createObjectNode().put("manifest_version", "dat-02-pilot-v1").put("pilot_id", id)
            .put("artifact_kind", "DEVELOPMENT_PILOT").put("allowed_usage", "DEVELOPMENT_ONLY").put("research_ready", false)
            .put("source_dataset_id", sourceManifest.path("dataset_id").asText()).put("source_dataset_status", "BUILDING")
            .put("feature_version", scaling.path("feature_version").asText()).put("scaling_config_version", scaling.path("scaling_config_version").asText())
            .put("scaling_config_json", configText).put("selection_config_json", selectionText)
            .put("feature_count", result.validCount()).put("mainshock_count", ids.size()).put("invalid_count", result.invalidCount())
            .put("coverage_status", coverage.path("status").asText()).put("coverage_sha256", WindowArtifactInput.sha(temporary.resolve("coverage_report.json")))
            .put("feature_content_sha256", MainshockWindowJob.fingerprint(features)).put("mainshock_content_sha256", MainshockWindowJob.fingerprint(main));
        manifest.set("identity_payload", payload); manifest.set("selected_mainshock_ids", JSON.valueToTree(ids));
        manifest.set("source_manifest", sourceManifest); manifest.set("gold_publication", input.nativeReport().path("publication"));
        manifest.set("source_files", input.receipt().path("files")); manifest.set("audit", dataset.path("audit"));
        manifest.set("window_manifest", dataset.path("window_manifest")); manifest.set("vector_columns", JSON.valueToTree(SpacetimeFeatureEngine.VECTOR));
        manifest.set("feature_schema", LocalFeatureJob.schema(features)); manifest.set("mainshock_schema", LocalFeatureJob.schema(main));
        manifest.put("schema_sha256", DatasetIdentityPlan.sha(DatasetIdentityPlan.canonical(manifest.path("feature_schema")).toString()));
        manifest.set("runtime", JSON.createObjectNode().put("spark", features.sparkSession().version()).put("java", System.getProperty("java.version")));
        manifest.putArray("limitations").add("Development-only, bounded JMA QA source; not representative or research-ready")
            .add("Full candidates retained including self-only window; no fitting or tuning performed")
            .add("MLD-05 owns official validation/export; EXP-01/EXP-02 own harness, algorithms and local-to-Colab gate; MLI-03 rejects official pilot import");
        LocalFeatureJob.json(temporary.resolve("pilot_manifest.json"), manifest);
        StringBuilder checksums = new StringBuilder();
        for (var entry : LocalFeatureJob.hashes(temporary).entrySet()) checksums.append(entry.getValue()).append("  ").append(entry.getKey()).append('\n');
        Files.writeString(temporary.resolve("checksums.sha256"), checksums, StandardOpenOption.CREATE_NEW);
        boolean reused = LocalFeatureJob.install(temporary, root.resolve("pilot_id=" + id));
        var receipt = JSON.createObjectNode().put("status", "DEVELOPMENT_PILOT_READY").put("pilot_id", id).put("reused", reused)
            .put("feature_count", result.validCount()).put("mainshock_count", ids.size()).put("coverage_status", coverage.path("status").asText())
            .put("research_ready", false).put("source_status_unchanged", "BUILDING");
        receipt.set("checksums", JSON.valueToTree(LocalFeatureJob.hashes(root.resolve("pilot_id=" + id)))); return receipt;
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 6) throw new IllegalArgumentException("usage: PilotExportJob INPUT NATIVE_SHA SCALING_JSON OUTPUT_ROOT TRINO_URL SELECTION_JSON");
        String code = LocalFeatureJob.codeSha(); var spark = LocalFeatureJob.session();
        try {
            var input = WindowArtifactInput.load(spark, Path.of(args[0]), args[1], new TrinoSqlClient(URI.create(args[4])));
            var receipt = export(input, JSON.readTree(Files.readAllBytes(Path.of(args[5]))), JSON.readTree(Files.readAllBytes(Path.of(args[2]))), Path.of(args[3]).toAbsolutePath(), code);
            System.out.println(DatasetIdentityPlan.canonical(receipt));
        } finally { spark.stop(); }
    }
}
