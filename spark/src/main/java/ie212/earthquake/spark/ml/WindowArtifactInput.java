package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.*;
import ie212.earthquake.spark.gold.*;
import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.*;

/** Read-only accepted MLD-03 artifact resolver. All lake statements are SELECT, never manifest.build(). */
public record WindowArtifactInput(JsonNode nativeReport, JsonNode dataset, JsonNode receipt,
        Dataset<Row> mainshocks, Dataset<Row> candidates) {
    static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY);
    public static String sha(Path path) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        try (var in = Files.newInputStream(path)) { byte[] buffer = new byte[262144]; int n;
            while ((n = in.read(buffer)) != -1) digest.update(buffer, 0, n); }
        return HexFormat.of().formatHex(digest.digest());
    }
    public static Path safe(Path root, String relative) throws IOException {
        Path path = root.resolve(relative).normalize();
        if (relative.isBlank() || Path.of(relative).isAbsolute() || !path.startsWith(root) || Files.isSymbolicLink(path)
                || !path.toRealPath().startsWith(root.toRealPath())) throw new IOException("ML_WINDOW_UNSAFE_INPUT_PATH");
        return path;
    }
    public static WindowArtifactInput load(SparkSession spark, Path input, String expectedNativeSha, GoldPublisher.Sql sql) throws Exception {
        input = input.toAbsolutePath().normalize();
        if (!expectedNativeSha.matches("[a-f0-9]{64}") || Files.isSymbolicLink(input)) throw new IOException("ML_WINDOW_EXACT_INPUT_REQUIRED");
        Path receiptPath = safe(input, "source-input.json");
        if (Files.size(receiptPath) > 262144) throw new IOException("ML_WINDOW_RECEIPT_LIMIT");
        var receipt = JSON.readTree(Files.readAllBytes(receiptPath));
        if (!"mld04-window-input-v1".equals(receipt.path("input_version").asText())
                || !expectedNativeSha.equals(receipt.path("native_report_sha256").asText())
                || !Set.of("REPRODUCTION", "EXTENSION").contains(receipt.path("split").asText()))
            throw new IOException("ML_WINDOW_RECEIPT_CONFLICT");
        Map<String, JsonNode> indexed = new TreeMap<>();
        for (var file : receipt.path("files")) {
            String name = file.path("path").asText(); Path path = safe(input, name);
            if (indexed.put(name, file) != null || Files.size(path) > 16_777_216
                    || !file.path("bytes").isIntegralNumber() || Files.size(path) != file.path("bytes").asLong()
                    || !sha(path).equals(file.path("sha256").asText())) throw new IOException("ML_WINDOW_INPUT_BYTES_CHANGED");
        }
        if (indexed.isEmpty() || indexed.size() > 1027) throw new IOException("ML_WINDOW_INPUT_FILE_LIMIT");
        Path report = safe(input, "native-report.json");
        if (Files.size(report) > 2_097_152 || !sha(report).equals(expectedNativeSha)) throw new IOException("ML_WINDOW_NATIVE_RECEIPT_CHANGED");
        var nativeReport = JSON.readTree(Files.readAllBytes(report));
        if (!"VERIFIED_PUBLISHED_JMA_WINDOWS".equals(nativeReport.path("status").asText())
                || nativeReport.path("research_ready").asBoolean(true) || !nativeReport.path("rerun_unchanged").asBoolean(false))
            throw new IOException("ML_WINDOW_ACCEPTED_NATIVE_RECEIPT_REQUIRED");
        JsonNode selected = null;
        for (var dataset : nativeReport.path("datasets")) if (receipt.path("split").equals(dataset.path("manifest").path("dataset_split"))) {
            if (selected != null) throw new IOException("ML_WINDOW_DUPLICATE_SPLIT"); selected = dataset;
        }
        if (selected == null) throw new IOException("ML_WINDOW_SPLIT_MISSING");
        String prefix = receipt.path("split").asText().toLowerCase(Locale.ROOT);
        for (var entry : Map.of("window-runtime-report.json", selected.path("window"), "window-manifest.json", selected.path("window_manifest")).entrySet()) {
            if (!indexed.containsKey(prefix + "/" + entry.getKey()) || !JSON.readTree(Files.readAllBytes(safe(input, prefix + "/" + entry.getKey()))).equals(entry.getValue()))
                throw new IOException("ML_WINDOW_HANDOFF_CHANGED");
        }
        Set<String> expectedParts = new TreeSet<>();
        for (var part : selected.path("window").path("parquet_files")) {
            String name = prefix + "/" + part.path("path").asText();
            if (!name.matches("(?:reproduction|extension)/windows/(?:mainshock_candidate_snapshot|sequence_candidate_snapshot)\\.parquet/part-[A-Za-z0-9_.-]+\\.parquet")
                    || !expectedParts.add(name) || !indexed.containsKey(name)
                    || !part.path("sha256").equals(indexed.get(name).path("sha256"))) throw new IOException("ML_WINDOW_UNINDEXED_PART");
        }
        Set<String> actualParts = new TreeSet<>();
        try (var files = Files.walk(input)) {
            for (Path path : files.toList()) {
                if (Files.isSymbolicLink(path)) throw new IOException("ML_WINDOW_SYMLINK_REJECTED");
                if (path.toString().endsWith(".parquet") && Files.isRegularFile(path)) actualParts.add(input.relativize(path).toString());
            }
        }
        if (expectedParts.isEmpty() || !expectedParts.equals(actualParts)) throw new IOException("ML_WINDOW_PART_SET_CHANGED");
        var main = spark.read().parquet(safe(input, prefix + "/windows/mainshock_candidate_snapshot.parquet").toString());
        var candidates = spark.read().parquet(safe(input, prefix + "/windows/sequence_candidate_snapshot.parquet").toString());
        var window = selected.path("window"); var manifest = selected.path("manifest");
        if (main.count() != window.path("mainshock_count").asLong(-1) || main.count() < 1 || main.count() > 128
                || candidates.count() != window.path("total_candidate_row_count").asLong(-1) || candidates.count() > 100000
                || !MainshockWindowJob.fingerprint(main).equals(window.path("mainshock_content_sha256").asText())
                || !MainshockWindowJob.fingerprint(candidates).equals(window.path("candidate_content_sha256").asText()))
            throw new IOException("ML_WINDOW_PARQUET_CONTENT_CHANGED");
        verifyPublication(nativeReport, manifest, sql);
        verifyGoldRows(candidates, manifest, sql);
        // Exact manifest/audit/Mc linkage, without mutating the official BUILDING row.
        var audit = selected.path("audit"); var handoff = selected.path("window_manifest");
        if (!"BUILDING".equals(manifest.path("dataset_status").asText()) || !"BUILDING".equals(handoff.path("dataset_status").asText())
                || !audit.path("candidate_build_allowed").asBoolean(false)
                || !DatasetIdentityPlan.sha(audit.path("mc_config_json").asText()).equals(audit.path("mc_config_sha256").asText())
                || !audit.path("mc_config_sha256").equals(window.path("mc_config_sha256"))
                || !audit.path("mc_config_sha256").equals(handoff.path("mc_config_sha256"))) throw new IOException("ML_WINDOW_AUDIT_PIN_CONFLICT");
        for (var node : List.of(audit, window, handoff)) {
            for (String field : List.of("dataset_id", "gold_snapshot_id", "gold_publication_id"))
                if (!node.path(field).equals(manifest.path(field))) throw new IOException("ML_WINDOW_DATASET_LINEAGE_CONFLICT");
        }
        return new WindowArtifactInput(nativeReport, selected, receipt, main, candidates);
    }
    static void verifyPublication(JsonNode nativeReport, JsonNode manifest, GoldPublisher.Sql sql) throws Exception {
        var expected = nativeReport.path("publication"); var pins = expected.path("snapshot_bundle");
        String physical = manifest.path("physical_gold_table_name").asText(); DatasetManifestWriter.pinnedGoldSql(manifest);
        String ns = physical.substring(0, physical.lastIndexOf('.'));
        String pub = manifest.path("gold_publication_id").asText();
        if (!pub.matches("pub_[a-f0-9]{64}") || !expected.path("published").asBoolean(false)
                || !"PASSED".equals(expected.path("verify_status").asText())) throw new IOException("DS_GOLD_NOT_PUBLISHED");
        var rows = DatasetManifestWriter.records(sql.execute("SELECT * FROM " + ns + ".publication_status WHERE publication_id=" + GoldVerificationPlan.quote(pub)));
        if (rows.size() != 1 || !"PASSED".equals(rows.get(0).path("verify_status").asText())
                || !pub.substring(4).equals(rows.get(0).path("bundle_sha256").asText())
                || !manifest.path("gold_snapshot_id").equals(rows.get(0).path("snapshot_id"))
                || !physical.equals(rows.get(0).path("physical_gold_table_name").asText())
                || !DatasetIdentityPlan.canonical(JSON.readTree(rows.get(0).path("snapshot_bundle_json").asText())).toString()
                    .equals(DatasetIdentityPlan.canonical(pins).toString())) throw new IOException("DS_PUBLICATION_CONFLICT");
        Set<String> tables = new HashSet<>(); pins.fieldNames().forEachRemaining(tables::add);
        if (!tables.equals(new HashSet<>(GoldIcebergWriter.TABLES))) throw new IOException("DS_INCOMPLETE_PUBLICATION_BUNDLE");
        for (String name : GoldIcebergWriter.TABLES) {
            var pin = pins.path(name);
            if (!pin.path("snapshot_id").isIntegralNumber() || pin.path("snapshot_id").asLong() <= 0
                    || !pin.path("table").asText().equals(ns + "." + name)) throw new IOException("DS_PUBLICATION_PIN_CONFLICT");
            var available = sql.execute("SELECT count(*) FROM " + ns + ".\"" + name + "$snapshots\" WHERE snapshot_id=" + pin.path("snapshot_id").asLong());
            if (available.rows().size() != 1 || available.rows().get(0).get(0).asLong(-1) != 1) throw new IOException("DS_PINNED_SNAPSHOT_UNAVAILABLE");
        }
        if (!manifest.path("gold_snapshot_id").equals(pins.path("event_current").path("snapshot_id"))) throw new IOException("DS_PUBLICATION_PIN_CONFLICT");
        String ml = nativeReport.path("ml_namespace").asText();
        if (!ml.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*")) throw new IOException("DS_INVALID_NAMESPACE");
        var current = DatasetManifestWriter.records(sql.execute("SELECT * FROM " + ml + ".dataset_manifest WHERE dataset_id=" + GoldVerificationPlan.quote(manifest.path("dataset_id").asText())));
        if (current.size() != 1) throw new IOException("DS_EXACT_BUILDING_MANIFEST_REQUIRED");
        for (var fields = manifest.fields(); fields.hasNext();) {
            var field = fields.next();
            if (!field.getValue().equals(current.get(0).path(field.getKey()))) throw new IOException("DS_MANIFEST_IDENTITY_CONFLICT");
        }
    }
    static void verifyGoldRows(Dataset<Row> candidates, JsonNode manifest, GoldPublisher.Sql sql) throws Exception {
        var rows = candidates.select("candidate_event_id", "candidate_time_utc", "candidate_latitude", "candidate_longitude", "candidate_depth_km", "candidate_magnitude").distinct().toLocalIterator();
        while (rows.hasNext()) {
            Map<String, Row> batch = new LinkedHashMap<>();
            while (rows.hasNext() && batch.size() < 128) {
                Row row = rows.next(); if (batch.put(row.getString(0), row) != null) throw new IOException("ML_WINDOW_EVENT_CONTENT_CONFLICT");
            }
            String ids = String.join(",", batch.keySet().stream().map(GoldVerificationPlan::quote).toList());
            String table = manifest.path("physical_gold_table_name").asText();
            long snapshot = manifest.path("gold_snapshot_id").asLong();
            var gold = DatasetManifestWriter.records(sql.execute("SELECT canonical_event_id,event_time_utc,latitude,longitude,depth_km,magnitude,"
                + "canonical_source_system,catalog_era,is_natural_earthquake,is_in_study_area FROM " + table + " FOR VERSION AS OF " + snapshot
                + " WHERE canonical_event_id IN (" + ids + ")"));
            if (gold.size() != batch.size()) throw new IOException("ML_WINDOW_UNKNOWN_GOLD_EVENT");
            for (var g : gold) {
                Row candidate = batch.remove(g.path("canonical_event_id").asText());
                if (candidate == null || !"JMA_BULLETIN".equals(g.path("canonical_source_system").asText())
                        || !"UNIFIED".equals(g.path("catalog_era").asText()) || !g.path("is_natural_earthquake").asBoolean(false)
                        || !g.path("is_in_study_area").asBoolean(false)
                        || !candidate.<Timestamp>getAs("candidate_time_utc").toInstant().equals(Instant.parse(g.path("event_time_utc").asText())))
                    throw new IOException("ML_WINDOW_GOLD_LINEAGE_CONFLICT");
                int index = 2;
                for (String field : List.of("latitude", "longitude", "depth_km", "magnitude"))
                    if (!g.path(field).isNumber() || Double.compare(candidate.getDouble(index++), g.path(field).doubleValue()) != 0)
                        throw new IOException("ML_WINDOW_GOLD_VALUE_CONFLICT");
            }
            if (!batch.isEmpty()) throw new IOException("ML_WINDOW_UNKNOWN_GOLD_EVENT");
        }
    }
}
