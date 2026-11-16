package ie212.earthquake.spark.gold;

import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.bronze.PortableBronzeProvenance;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

/** PREPARE ONLY: exact baseline snapshots + portable Silver -> checkpointed local Gold inputs. */
public final class PortableCatalogImportJob {
    private static final ObjectMapper JSON = PortablePreparedCatalog.JSON;
    private PortableCatalogImportJob() { }
    private static void require(boolean ok, String reason) throws Exception { PortableImportContract.require(ok, reason); }
    private static String sha(Path p) throws Exception { return PortableInputMaterializer.fileSha(p); }
    private static JsonNode pinned(Path root, JsonNode ref) throws Exception {
        Path path = PortablePreparedCatalog.relative(root, ref.path("relative_path").asText());
        require(sha(path).equals(ref.path("sha256").asText()), "IMPORT_INPUT_PIN_CHANGED"); return PortablePreparedCatalog.read(path, 8 * 1024 * 1024);
    }
    private static Dataset<Row> physicalMap(SparkSession spark, JsonNode receipt, boolean originals) {
        List<Row> rows = new ArrayList<>();
        for (var m : receipt.path("mappings")) rows.add(RowFactory.create(
                m.path(originals ? "original_manifest_id" : "physical_manifest_id").asText(),
                m.path(originals ? "original_raw_object_uri" : "physical_raw_object_uri").asText(),
                m.path("sha256").asText(), m.path("physical_manifest_id").asText(), m.path("physical_raw_object_uri").asText()));
        var schema = new StructType().add("_original_id", "string", false).add("_original_uri", "string", false)
                .add("_original_sha", "string", false).add("_physical_id", "string", false).add("_physical_uri", "string", false);
        return spark.createDataFrame(rows, schema).distinct();
    }
    public static Dataset<Row> relocate(Dataset<Row> observations, Dataset<Row> mapping) throws Exception {
        Dataset<Row> joined = observations.join(broadcast(mapping), observations.col("bronze_manifest_id").equalTo(mapping.col("_original_id"))
                .and(observations.col("raw_object_uri").equalTo(mapping.col("_original_uri")))
                .and(observations.col("raw_sha256").equalTo(mapping.col("_original_sha"))), "left");
        require(joined.filter(col("_physical_id").isNull()).limit(1).count() == 0, "OBSERVATION_PHYSICAL_LINEAGE_NOT_RESOLVED");
        return joined.withColumn("bronze_manifest_id", col("_physical_id")).withColumn("raw_object_uri", col("_physical_uri"))
                .drop("_original_id", "_original_uri", "_original_sha", "_physical_id", "_physical_uri");
    }
    private static ObjectNode checkpoint(Path prepared, String id, List<String> months, Map<String, Dataset<Row>> data,
            String requestSha, String previousSha, JsonNode sourceReceipt) throws Exception {
        Path file = prepared.resolve("checkpoints/" + id + ".json");
        if (Files.exists(file)) {
            JsonNode old = PortablePreparedCatalog.read(file, 1024 * 1024);
            require(requestSha.equals(old.path("request_sha256").asText()) && old.path("months").equals(JSON.valueToTree(months))
                    && Objects.equals(previousSha, old.path("previous_checkpoint_sha256").isNull() ? null : old.path("previous_checkpoint_sha256").asText()),
                    "IMPORT_CHECKPOINT_IDENTITY_CHANGED");
            for (var fields = old.path("tables").fields(); fields.hasNext();) PortablePreparedCatalog.verifyDataset(prepared, fields.next().getValue());
            return (ObjectNode) old;
        }
        var record = JSON.createObjectNode().put("version", "dat03-portable-batch-checkpoint-v1").put("batch_id", id).put("request_sha256", requestSha);
        record.set("months", JSON.valueToTree(months)); if (previousSha == null) record.putNull("previous_checkpoint_sha256"); else record.put("previous_checkpoint_sha256", previousSha);
        record.set("source_receipt", sourceReceipt); var tables = record.putObject("tables");
        for (var entry : data.entrySet()) tables.set(entry.getKey(), PortablePreparedCatalog.describeDataset(prepared,
                prepared.resolve("batches/" + id + "/" + entry.getKey()), entry.getValue()));
        PortablePreparedCatalog.save(file, record); return record;
    }
    private static Map<String, Dataset<Row>> goldTables(GoldTransformationResult gold, Dataset<Row> links) {
        var data = new LinkedHashMap<String, Dataset<Row>>();
        data.put("event_current", gold.eventCurrent().withColumn("event_time_jst", col("event_time_jst").cast("timestamp_ntz")));
        data.put("event_source_bridge", gold.eventSourceBridge().join(gold.eventCurrent()
                .select(col("canonical_event_id"), date_format(col("event_time_utc"), "yyyy-MM").alias("event_month_utc")), "canonical_event_id"));
        data.put("dim_date", gold.dimDate()); data.put("dim_region", gold.dimRegion()); data.put("dim_magnitude_band", gold.dimMagnitudeBand());
        data.put("dim_depth_band", gold.dimDepthBand()); data.put("source_link", links); return data;
    }
    private static Dataset<Row> all(SparkSession spark, Path prepared, List<JsonNode> checkpoints, String name) throws Exception {
        var files = new ArrayList<String>();
        for (var cp : checkpoints) for (var f : cp.path("tables").path(name).path("files"))
            files.add(PortablePreparedCatalog.relative(prepared, f.path("relative_path").asText()).toString());
        return spark.read().parquet(files.toArray(String[]::new));
    }
    private static long total(List<JsonNode> checkpoints, String name) { return checkpoints.stream().mapToLong(c -> c.path("tables").path(name).path("row_count").asLong()).sum(); }
    private static JsonNode combined(List<JsonNode> checkpoints, String name) {
        var result = JSON.createObjectNode().put("row_count", total(checkpoints, name)); var files = result.putArray("files");
        for (var cp : checkpoints) { result.put("schema_json", cp.path("tables").path(name).path("schema_json").asText()); for (var f : cp.path("tables").path(name).path("files")) files.add(f); }
        return result;
    }
    private static List<String> monthList(JsonNode value) { var out = new ArrayList<String>(); value.forEach(m -> out.add(m.asText())); return out; }
    public static void main(String[] args) throws Exception {
        require(args.length == 4, "IMPORT_REQUEST_SHA_SESSION_BUDGET_REQUIRED");
        Path requestFile = PortableInputMaterializer.safe(Path.of(args[0])), root = requestFile.getParent();
        require(root.toString().matches("/opt/pipeline/staging/dat03-portable-import/[a-f0-9]{32}") && args[2].matches("[a-f0-9]{32}"), "EXACT_IMPORT_ROOT_REQUIRED");
        int seconds = Integer.parseInt(args[3]); require(seconds >= 600 && seconds <= 6000, "BOUNDED_IMPORT_SESSION_REQUIRED");
        long deadline = System.nanoTime() + seconds * 1_000_000_000L;
        require(sha(requestFile).equals(args[1]), "IMPORT_REQUEST_PIN_MISMATCH");
        JsonNode request = PortablePreparedCatalog.read(requestFile, 1024 * 1024); PortableImportContract.validate(request);
        Path jar = Path.of(PortableCatalogImportJob.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        require(Files.isRegularFile(jar) && sha(jar).equals(request.path("code_sha256").asText()), "IMPORT_CODE_PIN_MISMATCH");
        Path guard = Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")), "owner.json");
        require(!Files.exists(guard), "SOURCE_WRITE_LEASE_NOT_IDLE");
        Path prepared = root.resolve("prepared"); Files.createDirectories(prepared.resolve("checkpoints")); Files.createDirectories(prepared.resolve("sessions"));
        Path copiedRequest = prepared.resolve("request.json");
        if (!Files.exists(copiedRequest)) Files.write(copiedRequest, Files.readAllBytes(requestFile), StandardOpenOption.CREATE_NEW);
        require(sha(copiedRequest).equals(args[1]), "PREPARED_REQUEST_CHANGED");
        if (Files.exists(prepared.resolve("catalog.json"))) {
            String existing = sha(prepared.resolve("catalog.json")); boolean acceptedBefore = false;
            try (var oldSessions = Files.list(prepared.resolve("sessions"))) {
                for (var old : oldSessions.sorted().toList()) {
                    var value = PortablePreparedCatalog.read(old, 65536);
                    if ("PREPARED".equals(value.path("status").asText())) {
                        require(value.path("request_sha256").asText().equals(args[1]) && value.path("catalog_sha256").asText().equals(existing), "PREPARED_ACCEPTANCE_PIN_CHANGED");
                        acceptedBefore = true;
                    }
                }
            }
            require(acceptedBefore, "UNACCEPTED_COMPLETION_REQUIRES_REVIEW"); PortablePreparedCatalog.verify(prepared, existing);
            var report = JSON.createObjectNode().put("status", "PREPARED").put("request_sha256", args[1]).put("catalog_sha256", existing)
                    .put("reused", true).put("gold_published", false).put("official_export_ready", false);
            PortablePreparedCatalog.save(prepared.resolve("sessions/" + args[2] + ".json"), report); return;
        }
        JsonNode bronzePlan = pinned(root, request.path("bronze_plan")); JsonNode provenance;
        try (var store = MinioBronzeObjectStore.fromEnvironment()) {
            provenance = PortableBronzeProvenance.verify(bronzePlan, request.path("bronze_plan").path("sha256").asText(), store);
            for (var pin : bronzePlan.path("physical_bronze_inputs")) {
                String uri = pin.path("manifest_uri").asText(); byte[] raw = store.read(java.net.URI.create(uri).getPath().substring(1));
                require(SilverBundlePublisher.sha(raw).equals(pin.path("manifest_sha256").asText()), "SOURCE_MANIFEST_CHANGED_AFTER_READBACK");
                require(!Instant.parse(JSON.readTree(raw).path("retrieved_at_utc").asText()).isAfter(Instant.parse(request.path("source_cutoff_utc").asText())), "SOURCE_AFTER_FROZEN_CUTOFF");
            }
        }
        Path provenanceFile = prepared.resolve("provenance.json");
        if (!Files.exists(provenanceFile)) PortablePreparedCatalog.save(provenanceFile, provenance);
        require(PortablePreparedCatalog.persistedJsonEquals(provenanceFile, provenance), "PROVENANCE_RERUN_CHANGED");
        var coverage = PortableImportContract.coverage(bronzePlan.path("physical_bronze_inputs"), sha(provenanceFile));
        Path coverageFile = prepared.resolve("coverage.json"); if (!Files.exists(coverageFile)) PortablePreparedCatalog.save(coverageFile, coverage);
        require(PortablePreparedCatalog.persistedJsonEquals(coverageFile, coverage), "COVERAGE_RERUN_CHANGED");
        var builder = GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-portable-preparation")
                .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "16").config("spark.sql.adaptive.enabled", "false")
                .config("spark.local.dir", root.resolve("spill").toString());
        PortableGoldCapacityJob.readCompatibilityOptions().forEach(builder::config);
        var spark = builder.getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
        var checkpoints = new ArrayList<JsonNode>(); String previous = null; int ordinal = 0;
        try (var store = MinioSilverObjectStore.fromEnvironment()) {
            JsonNode baseline = request.path("baseline"); String baseRun = baseline.path("gold_run_id").asText();
            byte[] commitRaw = store.read("gold-commits/" + baseRun + "/commit.json"); JsonNode commit = JSON.readTree(commitRaw);
            JsonNode publication = JSON.readTree(store.read("gold-verification/" + baseRun + "/publication.json"));
            require(SilverBundlePublisher.sha(commitRaw).equals(baseline.path("bundle_sha256").asText())
                    && commit.path("tables").equals(baseline.path("tables")) && commit.path("namespace").equals(baseline.path("namespace"))
                    && publication.path("published").asBoolean(false) && "PASSED".equals(publication.path("verify_status").asText())
                    && publication.path("bundle_sha256").equals(baseline.path("bundle_sha256")), "PINNED_BASELINE_PUBLICATION_CHANGED");
            JsonNode accepted = pinned(root, request.path("baseline_acceptance"));
            require(accepted.path("completed_months").asInt(-1) == 163 && "ACCEPTED_CATALOG_CHECKPOINT".equals(accepted.path("status").asText())
                    && accepted.path("lease_release").path("status").asText().equals("RELEASED")
                    && baseline.path("acceptance_file_sha256").asText().equals(request.path("baseline_acceptance").path("sha256").asText()), "BASELINE_ACCEPTANCE_NOT_VERIFIED");
            String stateRoot = ie212.earthquake.spark.orchestration.FullMonthStateResolver.root(baseRun);
            byte[] registryBytes = store.read(stateRoot + "/registry.json"), stateReceiptBytes = store.read(stateRoot + "/receipt.json");
            JsonNode state = JSON.readTree(registryBytes), stateReceipt = JSON.readTree(stateReceiptBytes);
            require(Arrays.equals(store.read(stateRoot + "/_SUCCESS"), SilverBundlePublisher.sha(stateReceiptBytes).getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                    && stateReceipt.path("registry_sha256").asText().equals(SilverBundlePublisher.sha(registryBytes))
                    && "FullMonthReady".equals(stateReceipt.path("status").asText()) && state.path("run_id").asText().equals(baseRun)
                    && state.path("namespace").equals(baseline.path("namespace")) && state.path("complete_months").equals(JSON.valueToTree(PortableImportContract.BASELINE_MONTHS))
                    && state.path("source_cutoff_utc").equals(request.path("source_cutoff_utc"))
                    && state.path("silver_bundle_sha256").equals(commit.path("scope").path("silver_bundle_sha256")), "BASELINE_RECEIPT_BOUND_STATE_CHANGED");
            new SilverBundlePublisher(store).verify(baseRun);
            Map<String, Dataset<Row>> source = new LinkedHashMap<>();
            for (String table : GoldIcebergWriter.TABLES) {
                JsonNode pin = baseline.path("tables").path(table);
                require(accepted.path("current_snapshots").path(table).equals(pin.path("snapshot_id"))
                        && accepted.path("table_counts").path(table).equals(pin.path("count")), "BASELINE_ACCEPTANCE_SNAPSHOT_PIN_CHANGED");
                Dataset<Row> frame = spark.read().format("iceberg").option("snapshot-id", pin.path("snapshot_id").asText()).load(baseline.path("namespace").asText() + "." + table);
                require(frame.count() == pin.path("count").asLong(), "BASELINE_SNAPSHOT_COUNT_CHANGED");
                var iceberg = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, baseline.path("namespace").asText() + "." + table);
                var snapshot = iceberg.snapshot(pin.path("snapshot_id").asLong());
                require(snapshot != null && PortableGoldCapacityJob.writerFingerprint(frame).equals(snapshot.summary().get("gold.content_sha256")),
                        "BASELINE_SNAPSHOT_CONTENT_FINGERPRINT_CHANGED");
                source.put(table, frame);
            }
            Dataset<Row> physical = physicalMap(spark, provenance, false);
            Dataset<Row> baselineBridge = source.get("event_source_bridge");
            require(baselineBridge.join(broadcast(physical), baselineBridge.col("bronze_manifest_id").equalTo(physical.col("_physical_id"))
                    .and(baselineBridge.col("raw_object_uri").equalTo(physical.col("_physical_uri"))), "left_anti").limit(1).count() == 0, "BASELINE_PHYSICAL_LINEAGE_UNRESOLVED");
            require(baselineBridge.filter(col("source_link_id").isNotNull()).limit(1).count() == 0, "BASELINE_LINK_EVIDENCE_REQUIRED");
            Instant processed = Instant.parse(request.path("processed_at_utc").asText());
            for (int offset = 0; offset < 163; offset += 12) {
                var months = PortableImportContract.BASELINE_MONTHS.subList(offset, Math.min(163, offset + 12));
                String id = String.format("%02d", ++ordinal); Path cpPath = prepared.resolve("checkpoints/" + id + ".json");
                if (!Files.exists(cpPath) && (deadline - System.nanoTime()) / 1e9 < 300) break;
                System.out.println("event=portable_import_batch batch=" + ordinal + "/" + (14 + request.path("worker_batches").size()) + " months=" + months.get(0) + ".." + months.get(months.size() - 1));
                var data = new LinkedHashMap<>(source);
                data.put("event_current", source.get("event_current").filter(GoldIcebergWriter.monthPredicate(months))
                        .withColumn("gold_run_id", lit(request.path("run_id").asText())).withColumn("record_updated_at_utc", lit(Timestamp.from(processed))));
                data.put("event_source_bridge", source.get("event_source_bridge").filter(col("event_month_utc").isin(months.toArray())));
                data.put("source_link", spark.createDataFrame(List.<Row>of(), SilverSchemas.SOURCE_LINK_SCHEMA));
                var cp = checkpoint(prepared, id, months, data, args[1], previous, baseline); checkpoints.add(cp); previous = sha(cpPath);
            }
            if (checkpoints.size() == 14) for (var batch : request.path("worker_batches")) {
                String id = String.format("%02d", ++ordinal); var months = monthList(batch.path("months")); Path cpPath = prepared.resolve("checkpoints/" + id + ".json");
                if (!Files.exists(cpPath) && (deadline - System.nanoTime()) / 1e9 < 300) break;
                System.out.println("event=portable_import_batch batch=" + ordinal + "/" + (14 + request.path("worker_batches").size()) + " months=" + months.get(0) + ".." + months.get(months.size() - 1));
                if (Files.exists(cpPath)) {
                    var cp = checkpoint(prepared, id, months, Map.of(), args[1], previous, JSON.nullNode()); checkpoints.add(cp); previous = sha(cpPath); continue;
                }
                Path planFile = PortablePreparedCatalog.relative(root, batch.path("plan_file").asText());
                require(PortablePreparedCatalog.read(planFile, 8 * 1024 * 1024).path("affected_months").equals(batch.path("months")), "WORKER_PLAN_MONTH_SCOPE_CHANGED");
                var materialized = PortableInputMaterializer.stage(planFile, batch.path("plan_sha256").asText(), root.resolve("materialized"));
                Dataset<Row> observations = null, memberships = null, links = null;
                var actualRuns = new ArrayList<String>();
                for (var g : materialized.groups()) {
                    actualRuns.add(g.runId());
                    Dataset<Row> o = spark.read().parquet(g.datasets().get("source_observation").toArray(String[]::new));
                    Dataset<Row> m = spark.read().parquet(g.datasets().get("canonical_membership").toArray(String[]::new));
                    Dataset<Row> l = spark.read().parquet(g.datasets().get("source_link").toArray(String[]::new));
                    observations = observations == null ? o : observations.unionByName(o); memberships = memberships == null ? m : memberships.unionByName(m);
                    links = links == null ? l : links.unionByName(l);
                }
                require(JSON.valueToTree(actualRuns).equals(batch.path("run_ids")), "WORKER_RUN_SET_CHANGED");
                require(observations.filter(not(date_format(col("event_time_utc"), "yyyy-MM").isin(months.toArray()))
                        .or(col("event_time_utc").isNull())).limit(1).count() == 0, "WORKER_ROWS_OUTSIDE_EXACT_MONTH_SCOPE");
                observations = relocate(observations, physicalMap(spark, provenance, true)).persist(org.apache.spark.storage.StorageLevel.DISK_ONLY());
                memberships = memberships.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY()); links = links.withColumn("decided_at_utc", lit(Timestamp.from(processed)));
                try {
                    require(observations.count() == batch.path("observation_count").asLong(), "WORKER_OBSERVATION_COUNT_CHANGED");
                    var ctx = new GoldRunContext(request.path("run_id").asText(), Instant.parse(months.get(0) + "-01T00:00:00Z"),
                            YearMonth.parse(months.get(months.size() - 1)).plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
                            LocalDate.parse(request.path("processing_date").asText()), true, PortableImportContract.VERSION,
                            actualRuns.stream().map(r -> "s3://japan-earthquake/silver/bundles/" + r + "/manifest.json").toList());
                    var gold = new GoldEventTransformer().transform(observations, memberships, links, spark.createDataFrame(List.<Row>of(), GoldEventTransformer.REGION_SCHEMA), ctx, processed);
                    require(gold.canonicalEventCount() == batch.path("canonical_count").asLong()
                            && gold.bridgeRowCount() == batch.path("observation_count").asLong()
                            && links.count() == batch.path("source_link_count").asLong(), "WORKER_GOLD_RECONCILIATION_FAILED");
                    var cp = checkpoint(prepared, id, months, goldTables(gold, links), args[1], previous, materialized.readback());
                    checkpoints.add(cp); previous = sha(cpPath);
                } finally { observations.unpersist(); memberships.unpersist(); }
            }
            var report = JSON.createObjectNode().put("version", PortableImportContract.VERSION).put("request_sha256", args[1])
                    .put("completed_batches", checkpoints.size()).put("required_batches", 14 + request.path("worker_batches").size())
                    .put("gold_published", false).put("official_export_ready", false).put("reused", false);
            if (checkpoints.size() == 14 + request.path("worker_batches").size() && (deadline - System.nanoTime()) / 1e9 >= 300) {
                System.out.println("event=portable_import_global_checks phase=global_boundary_revision_source_link");
                try (var checked = PortableCatalogReconciler.reconcile(all(spark, prepared, checkpoints, "event_current"), all(spark, prepared, checkpoints, "event_source_bridge"),
                        all(spark, prepared, checkpoints, "source_link"), total(checkpoints, "event_current"), total(checkpoints, "event_source_bridge"), total(checkpoints, "source_link"))) {
                    Path rec = prepared.resolve("reconciliation.json"); PortablePreparedCatalog.save(rec, checked.report());
                    var manifest = JSON.createObjectNode().put("version", PortableImportContract.PREPARED_VERSION).put("status", "PREPARED");
                    manifest.set("complete_months", JSON.valueToTree(PortableImportContract.ALL_MONTHS));
                    manifest.set("request", PortablePreparedCatalog.reference(prepared, copiedRequest)); manifest.set("coverage", PortablePreparedCatalog.reference(prepared, coverageFile));
                    manifest.set("reconciliation", PortablePreparedCatalog.reference(prepared, rec)); manifest.set("provenance", PortablePreparedCatalog.reference(prepared, provenanceFile));
                    var chain = manifest.putArray("checkpoints");
                    for (int i = 1; i <= checkpoints.size(); i++) chain.add(PortablePreparedCatalog.reference(prepared, prepared.resolve(String.format("checkpoints/%02d.json", i))));
                    manifest.set("source_links", checked.report().path("counts").path("exact_link_duplicates").asLong() > 0
                            ? PortablePreparedCatalog.describeDataset(prepared, prepared.resolve("global/source_link"), checked.links())
                            : combined(checkpoints, "source_link"));
                    var tables = manifest.putObject("tables");
                    for (String name : GoldIcebergWriter.TABLES) {
                        if (name.startsWith("dim_")) {
                            var data = all(spark, prepared, checkpoints, name).distinct(); String key = name.equals("dim_date") ? "date_key" : name.equals("dim_region") ? "region_key" : "band_code";
                            require(data.groupBy(key).count().filter(col("count").gt(1)).limit(1).count() == 0, "GLOBAL_DIMENSION_CONFLICT");
                            tables.set(name, PortablePreparedCatalog.describeDataset(prepared, prepared.resolve("global/" + name), data));
                        } else {
                            String countKey = name.equals("event_current") ? "exact_event_duplicates" : "exact_bridge_duplicates";
                            if (checked.report().path("counts").path(countKey).asLong() > 0)
                                tables.set(name, PortablePreparedCatalog.describeDataset(prepared, prepared.resolve("global/" + name), name.equals("event_current") ? checked.events() : checked.bridge()));
                            else tables.set(name, combined(checkpoints, name));
                        }
                    }
                    manifest.put("gold_published", false).put("official_export_ready", false).put("research_ready", false).put("ml_upstream_eligible", false);
                    PortablePreparedCatalog.save(prepared.resolve("catalog.json"), manifest); String catalogSha = sha(prepared.resolve("catalog.json"));
                    PortablePreparedCatalog.verify(prepared, catalogSha); report.put("status", "PREPARED").put("catalog_sha256", catalogSha);
                    report.set("reconciliation", checked.report());
                }
            } else report.put("status", "CHECKPOINTED");
            PortablePreparedCatalog.save(prepared.resolve("sessions/" + args[2] + ".json"), report);
        } finally { spark.stop(); }
    }
}
