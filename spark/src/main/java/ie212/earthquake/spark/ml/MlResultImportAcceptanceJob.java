package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.GoldPublisher;
import ie212.earthquake.spark.gold.TrinoSqlClient;
import java.net.URI;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.spark.sql.SparkSession;

/** Opt-in component acceptance. Synthetic inputs, real REST/S3 commits and Trino reads.
 * No Gold writes, table drops, pilot promotion or scientific approval. */
public final class MlResultImportAcceptanceJob {
    private static final ObjectMapper JSON = new ObjectMapper();
    private MlResultImportAcceptanceJob() { }

    public static void main(String[] args) throws Exception {
        require(args.length == 1, "MLI03_QA_REQUEST_REQUIRED");
        Path requestPath = Path.of(args[0]);
        JsonNode request = JSON.readTree(Files.readAllBytes(requestPath));
        String ns = request.path("namespace").asText();
        requireQaNamespace(ns);
        Path root = requestPath.toRealPath().getParent();
        Path fixtures = Path.of("/opt/pipeline/ml-result-fixtures");
        String catalog = System.getenv("ICEBERG_CATALOG_NAME");
        require(ns.startsWith(catalog + "."), "MLI03_QA_CATALOG_MISMATCH");
        Runnable lease = () -> {
            try {
                JsonNode held = JSON.readTree(Files.readAllBytes(Path.of(System.getenv("SOURCE_GUARD_ROOT"), "owner.json")));
                require(request.path("lease_owner").equals(held), "MLI03_QA_LEASE_NOT_HELD");
            } catch (java.io.IOException error) { throw new IllegalStateException("MLI03_QA_LEASE_NOT_HELD", error); }
        };
        lease.run();
        SparkSession spark = GoldCommitJob.configured(System.getenv()).appName("mli-03-runtime-acceptance").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            GoldPublisher.Sql sql = new TrinoSqlClient(URI.create("http://" + System.getenv("TRINO_HOST") + ":"
                + System.getenv("TRINO_INTERNAL_PORT")));
            ObjectNode report = accept(spark, sql, root, fixtures, ns, lease);
            report.put("java_version", System.getProperty("java.version")).put("spark_version", spark.version());
            Files.writeString(root.resolve("acceptance-report.json"), report.toPrettyString() + "\n", StandardOpenOption.CREATE_NEW);
            System.out.println(report);
        } finally { spark.stop(); }
    }

    static void requireQaNamespace(String ns) {
        require(ns != null && ns.matches("[a-z][a-z0-9_]*\\.ml_qa_[0-9a-f]{32}"), "MLI03_QA_NAMESPACE_REQUIRED");
    }

    static ObjectNode accept(SparkSession spark, GoldPublisher.Sql sql, Path root, Path fixtures,
                              String ns, Runnable lease) throws Exception {
        requireQaNamespace(ns);
        // Fresh namespaces only: never resume/overwrite an unrelated acceptance run.
        require(!spark.catalog().databaseExists(ns), "MLI03_QA_NAMESPACE_ALREADY_EXISTS");
        String gold = ns.substring(0, ns.indexOf('.')) + ".gold.event_current";
        String goldBefore = goldState(spark, gold);
        lease.run();
        spark.sql("CREATE NAMESPACE " + ns);
        spark.sql("CREATE TABLE " + ns + ".candidate_event USING iceberg AS SELECT 'ds_fixture' dataset_id, "
            + "'main' mainshock_event_id, 'main' candidate_event_id, 'MAINSHOCK' role UNION ALL "
            + "SELECT 'ds_fixture', 'main', 'post', 'POST'");
        spark.sql("CREATE TABLE " + ns + ".mainshock_event USING iceberg AS SELECT 'ds_fixture' dataset_id, 'main' mainshock_event_id");
        spark.sql("CREATE TABLE " + ns + ".dataset_registry USING iceberg AS SELECT 'ds_fixture' dataset_id, 'EXPORTED' dataset_status");
        ObjectNode pins = JSON.createObjectNode();
        for (String table : List.of("candidate_event", "mainshock_event", "dataset_registry")) {
            long snapshot = Spark3Util.loadIcebergTable(spark, ns + "." + table).currentSnapshot().snapshotId();
            long count = scalar(sql, "SELECT count(*) FROM " + ns + "." + table + " FOR VERSION AS OF " + snapshot);
            require(count == (table.equals("candidate_event") ? 2 : 1), "MLI03_QA_INPUT_READBACK_FAILED");
            pins.put(table, snapshot);
        }
        Path registry = root.resolve("registry"); Files.createDirectory(registry);
        Path export = registry.resolve("export.json");
        ObjectNode exportDoc = JSON.createObjectNode().put("dataset_id", "ds_fixture").put("synthetic_component_qa", true);
        exportDoc.set("snapshots", pins);
        write(export, exportDoc);
        ObjectNode manifest = JSON.createObjectNode().put("dataset_id", "ds_fixture").put("dataset_status", "EXPORTED")
            .put("export_manifest_ref", "export.json").put("export_manifest_sha256", digest(export))
            .put("registry_snapshot_id", pins.path("dataset_registry").asText())
            .put("candidate_snapshot_id", pins.path("candidate_event").asText())
            .put("mainshock_snapshot_id", pins.path("mainshock_event").asText());
        var candidates = manifest.putArray("candidates");
        // Resolve lineage from the same pinned Iceberg rows, not external request claims.
        for (var row : spark.read().format("iceberg").option("snapshot-id", pins.path("candidate_event").asText())
                .load(ns + ".candidate_event").collectAsList())
            candidates.addObject().put("mainshock_event_id", (String) row.getAs("mainshock_event_id"))
                .put("candidate_event_id", (String) row.getAs("candidate_event_id")).put("role", (String) row.getAs("role"));
        write(registry.resolve("dataset.json"), manifest);
        ObjectNode request = JSON.createObjectNode().put("dataset_id", "ds_fixture").put("experiment_run_id", "exp_fixture")
            .put("algorithm_name", "WINDOW").put("model_config_version", "1")
            .put("dataset_manifest_ref", "dataset.json").put("dataset_manifest_sha256", digest(registry.resolve("dataset.json")));
        var context = TrustedDatasetResolver.resolve(request, registry, null);
        var checks = JSON.createObjectNode();

        // Every invalid input is rejected with the real writer wired, before any result table exists.
        Map<String, String> invalid = new LinkedHashMap<>();
        invalid.put("checksum", "IMP_CHECKSUM_MISMATCH"); invalid.put("schema", "IMP_SCHEMA_MISMATCH");
        invalid.put("duplicate", "IMP_DUPLICATE_GRAIN"); invalid.put("unknown", "IMP_UNKNOWN_EVENT_ID");
        for (var item : invalid.entrySet()) {
            Path bundle = copyBundle(fixtures.resolve(item.getKey()), root.resolve(item.getKey()));
            addRuntimeMetadata(bundle);
            Path journal = root.resolve("reject-" + item.getKey());
            expect(item.getValue(), () -> MlResultImportJob.importBundle(bundle, journal, context,
                new SparkMlIcebergWriter(spark, ns, lease, journal), verifier(sql, ns, lease), ns));
            require(!spark.catalog().tableExists(ns + ".experiment_run"), "MLI03_QA_REJECT_WROTE_TABLES");
            require(JSON.readTree(Files.readAllBytes(journal.resolve("experiment-run-exp_fixture.json")))
                .path("experiment_status").asText().equals("REJECTED"), "MLI03_QA_REJECTION_EVIDENCE_REQUIRED");
            checks.put(item.getKey(), item.getValue());
        }
        Path nan = copyBundle(fixtures.resolve("hdb-success"), root.resolve("nan")); addRuntimeMetadata(nan);
        var decoded = MlResultBundleReader.read(nan);
        ((ObjectNode) decoded.memberships().get(0)).put("membership_probability", Double.NaN);
        writeMembership(nan.resolve("memberships.parquet"), decoded.memberships()); rehash(nan, "memberships.parquet");
        ObjectNode hdbRequest = request.deepCopy().put("algorithm_name", "HDBSCAN_GLOBAL");
        var hdbContext = TrustedDatasetResolver.resolve(hdbRequest, registry, null);
        expect("IMP_SCHEMA_MISMATCH", () -> MlResultImportJob.importBundle(nan, root.resolve("reject-nan"), hdbContext,
            new SparkMlIcebergWriter(spark, ns, lease, root.resolve("reject-nan")), verifier(sql, ns, lease), ns));
        require(!spark.catalog().tableExists(ns + ".experiment_run"), "MLI03_QA_NAN_WROTE_TABLES");
        checks.put("nan", "IMP_SCHEMA_MISMATCH");
        rejectNonExported(registry, manifest, request, checks);
        Path valid = copyBundle(fixtures.resolve("success"), root.resolve("valid"));
        Path renamedPilot = copyBundle(valid, root.resolve("renamed-pilot"));
        Files.writeString(renamedPilot.resolve("_SUCCESS.json"), "{\"status\":\"DEVELOPMENT_PILOT\"}\n");
        expect("IMP_INCOMPLETE_BUNDLE", () -> MlResultImportJob.importBundle(renamedPilot, root.resolve("reject-pilot"), context,
            new SparkMlIcebergWriter(spark, ns, lease, root.resolve("reject-pilot")), verifier(sql, ns, lease), ns));
        require(!spark.catalog().tableExists(ns + ".experiment_run"), "MLI03_QA_PILOT_WROTE_TABLES");
        checks.put("renamed_pilot_marker", "IMP_INCOMPLETE_BUNDLE");
        Path journal = root.resolve("journal");
        var first = MlResultImportJob.importBundle(valid, journal, context,
            new SparkMlIcebergWriter(spark, ns, lease, journal), verifier(sql, ns, lease), ns);
        require(first.path("experiment_status").asText().equals("CANDIDATE"), "MLI03_QA_NOT_CANDIDATE");
        require(scalar(sql, "SELECT count(*) FROM " + ns + ".experiment_run FOR VERSION AS OF "
            + first.path("experiment_snapshot_id").asLong() + " WHERE experiment_run_id='exp_fixture' "
            + "AND experiment_status='CANDIDATE' AND membership_snapshot_id=" + first.path("membership_snapshot_id").asLong()
            + " AND summary_snapshot_id=" + first.path("summary_snapshot_id").asLong()) == 1,
            "MLI03_QA_TRINO_PUBLICATION_PIN_MISMATCH");
        checks.put("trino_publication_pins", true);
        ObjectNode before = tableStates(spark, ns);
        var rerun = MlResultImportJob.importBundle(valid, root.resolve("new-journal"), context,
            new SparkMlIcebergWriter(spark, ns, lease, root.resolve("new-journal")), verifier(sql, ns, lease), ns);
        require(before.equals(tableStates(spark, ns)), "MLI03_QA_RERUN_CHANGED_TABLES");
        require(first.path("membership_snapshot_id").equals(rerun.path("membership_snapshot_id")), "MLI03_QA_RERUN_CHANGED_PIN");
        checks.put("rerun_unchanged", true);
        Path changed = copyBundle(valid, root.resolve("changed"));
        Path config = changed.resolve("experiment_config.json");
        ObjectNode value = (ObjectNode) JSON.readTree(Files.readAllBytes(config)); value.put("random_seed", 99);
        Files.writeString(config, value.toPrettyString() + "\n"); rehash(changed, "experiment_config.json");
        expect("IMP_EXPERIMENT_RUN_REUSED", () -> MlResultImportJob.importBundle(changed, root.resolve("changed-journal"), context,
            new SparkMlIcebergWriter(spark, ns, lease, root.resolve("changed-journal")), verifier(sql, ns, lease), ns));
        require(before.equals(tableStates(spark, ns)), "MLI03_QA_CHANGED_FINGERPRINT_WROTE_TABLES");
        checks.put("changed_fingerprint", "IMP_EXPERIMENT_RUN_REUSED");

        // Inject a lost commit outcome through the writer's test seam, never via production request config.
        String recovery = ns + "ab";
        Path recoveryJournal = root.resolve("recovery");
        var interrupted = new SparkMlIcebergWriter(spark, recovery, lease, recoveryJournal, table -> {
            if (table.equals("sequence_membership")) throw new IllegalStateException("IMP_COMMIT_OUTCOME_UNKNOWN");
        });
        expect("IMP_COMMIT_OUTCOME_UNKNOWN", () -> MlResultImportJob.importBundle(valid, recoveryJournal, context,
            interrupted, verifier(sql, recovery, lease), recovery));
        require(scalar(sql, "SELECT count(*) FROM " + recovery + ".experiment_run WHERE experiment_status='CANDIDATE'") == 0,
            "MLI03_QA_PARTIAL_PUBLISHED");
        long memberPin = Spark3Util.loadIcebergTable(spark, recovery + ".sequence_membership").currentSnapshot().snapshotId();
        var recovered = MlResultImportJob.importBundle(valid, recoveryJournal, context,
            new SparkMlIcebergWriter(spark, recovery, lease, recoveryJournal), verifier(sql, recovery, lease), recovery);
        require(recovered.path("membership_snapshot_id").asLong() == memberPin, "MLI03_QA_RECOVERY_REAPPENDED");
        require(spark.table(recovery + ".sequence_membership.snapshots").count() == 1, "MLI03_QA_RECOVERY_DUPLICATED");
        checks.put("partial_commit_hidden_until_candidate", true).put("recovery_reuses_snapshot", true);
        String blocker = ns + "cd";
        Path blockerJournal = root.resolve("blocker");
        expect("IMP_TRINO_QUALITY_BLOCKER", () -> MlResultImportJob.importBundle(valid, blockerJournal, context,
            new SparkMlIcebergWriter(spark, blocker, lease, blockerJournal),
            (commit, ctx, fingerprint) -> { throw new java.io.IOException("IMP_TRINO_QUALITY_BLOCKER"); }, blocker));
        require(scalar(sql, "SELECT count(*) FROM " + blocker + ".experiment_run WHERE experiment_status='REJECTED'") == 1,
            "MLI03_QA_BLOCKER_NOT_REJECTED");
        require(scalar(sql, "SELECT count(*) FROM " + blocker + ".experiment_run WHERE experiment_status='CANDIDATE'") == 0,
            "MLI03_QA_BLOCKER_PUBLISHED");
        checks.put("trino_blocker", "REJECTED");
        // Production consumer reads only pins held by a CANDIDATE registry row, not raw partial result tables.
        require(new MlResultSnapshotReader(spark, ns).serving(first.path("experiment_snapshot_id").asLong(),
            first.path("membership_snapshot_id").asLong(), first.path("summary_snapshot_id").asLong(), "exp_fixture").count() == 1,
            "MLI03_QA_SERVING_GATE_FAILED");
        require(goldBefore.equals(goldState(spark, gold)), "MLI03_QA_GOLD_CHANGED");
        checks.put("gold_unchanged", true);
        ObjectNode report = JSON.createObjectNode().put("task", "MLI-03").put("status", "PASSED")
            .put("synthetic_component_qa", true).put("scientific_approval", false).put("namespace", ns)
            .put("recovery_namespace", recovery).put("blocker_namespace", blocker).put("completed_at_utc", Instant.now().toString());
        report.set("input_snapshots", pins); report.set("publication", first); report.set("tables", before); report.set("checks", checks);
        return report;
    }

    private static MlResultImportJob.IndependentVerifier verifier(GoldPublisher.Sql sql, String ns, Runnable lease) {
        return (commit, context, fingerprint) -> new TrinoMlVerifier(sql, ns, lease).verify(commit, context, fingerprint);
    }

    static void rejectNonExported(Path registry, ObjectNode manifest, ObjectNode request, ObjectNode checks) throws Exception {
        for (String status : List.of("BUILDING", "DEVELOPMENT_PILOT")) {
            String name = "blocked-" + status + ".json";
            write(registry.resolve(name), manifest.deepCopy().put("dataset_status", status));
            ObjectNode blocked = request.deepCopy().put("dataset_manifest_ref", name)
                .put("dataset_manifest_sha256", digest(registry.resolve(name)));
            expect("IMP_UNKNOWN_DATASET", () -> TrustedDatasetResolver.resolve(blocked, registry, null));
            checks.put(status, "IMP_UNKNOWN_DATASET");
        }
    }

    private static ObjectNode tableStates(SparkSession spark, String ns) {
        ObjectNode result = JSON.createObjectNode();
        for (String table : List.of("sequence_membership", "sequence_summary", "experiment_run")) {
            var state = result.putObject(table).put("count", spark.table(ns + "." + table).count());
            var ids = state.putArray("snapshots");
            spark.table(ns + "." + table + ".snapshots").select("snapshot_id").orderBy("snapshot_id")
                .collectAsList().forEach(row -> ids.add(row.getLong(0)));
        }
        return result;
    }

    private static String goldState(SparkSession spark, String table) throws Exception {
        if (!spark.catalog().tableExists(table)) return "ABSENT";
        return spark.table(table).schema().json() + ":" + Spark3Util.loadIcebergTable(spark, table).currentSnapshot().snapshotId();
    }

    private static long scalar(GoldPublisher.Sql sql, String query) throws Exception {
        var result = sql.execute(query);
        require(result.rows().size() == 1 && result.rows().get(0).size() == 1
            && result.rows().get(0).get(0).canConvertToLong(), "MLI03_QA_TRINO_RESPONSE_INVALID");
        return result.rows().get(0).get(0).asLong();
    }

    private static Path copyBundle(Path source, Path target) throws Exception {
        Files.createDirectory(target);
        try (var paths = Files.walk(source)) {
            for (Path file : paths.toList()) {
                require(!Files.isSymbolicLink(file), "MLI03_QA_FIXTURE_SYMLINK");
                if (file.equals(source)) continue;
                Path to = target.resolve(source.relativize(file));
                if (Files.isDirectory(file)) Files.createDirectory(to); else Files.copy(file, to);
            }
        }
        return target;
    }

    private static void addRuntimeMetadata(Path bundle) throws Exception {
        Path path = bundle.resolve("experiment_config.json");
        ObjectNode config = (ObjectNode) JSON.readTree(Files.readAllBytes(path));
        config.put("runtime_environment_json", "{\"mode\":\"synthetic_component_qa\"}");
        Files.writeString(path, config.toPrettyString() + "\n"); rehash(bundle, "experiment_config.json");
    }

    private static void rehash(Path bundle, String file) throws Exception {
        Path path = bundle.resolve("_SUCCESS.json");
        ObjectNode marker = (ObjectNode) JSON.readTree(Files.readAllBytes(path));
        ((ObjectNode) marker.path("checksums")).put(file, digest(bundle.resolve(file)));
        Files.writeString(path, marker.toPrettyString() + "\n");
    }

    private static void writeMembership(Path path, List<JsonNode> rows) throws Exception {
        var schema = MlResultBundleReader.schema("memberships.parquet");
        var factory = new SimpleGroupFactory(schema);
        var conf = new Configuration(false);
        // No Hadoop .crc sidecars in a strict bundle: the marker owns byte checksums.
        conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        conf.setBoolean("fs.file.impl.disable.cache", true);
        try (var writer = ExampleParquetWriter.builder(new org.apache.hadoop.fs.Path(path.toUri()))
                .withType(schema).withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .withConf(conf).build()) {
            for (JsonNode row : rows) {
                Group group = factory.newGroup();
                for (JsonNode field : ResultBundleContract.contract().path("schemas").path("memberships.parquet").path("fields")) {
                    String key = field.path("name").asText(); JsonNode value = row.get(key);
                    if (value == null || value.isNull()) continue;
                    switch (field.path("type").asText()) {
                        case "string", "json_string" -> group.add(key, value.asText());
                        case "timestamp_utc" -> group.add(key, Instant.parse(value.asText()).toEpochMilli());
                        case "boolean" -> group.add(key, value.asBoolean());
                        case "integer" -> group.add(key, value.asInt());
                        case "long" -> group.add(key, value.asLong());
                        case "double" -> group.add(key, value.asDouble());
                        default -> throw new IllegalArgumentException("MLI03_QA_UNSUPPORTED_FIELD");
                    }
                }
                writer.write(group);
            }
        }
    }

    @FunctionalInterface private interface Attempt { void run() throws Exception; }
    private static void expect(String reason, Attempt attempt) throws Exception {
        try { attempt.run(); } catch (Exception error) {
            if (reason.equals(error.getMessage())) return;
            throw new IllegalStateException("MLI03_QA_UNEXPECTED_REASON:" + reason, error);
        }
        throw new IllegalStateException("MLI03_QA_EXPECTED_REJECTION:" + reason);
    }
    private static void require(boolean condition, String reason) { if (!condition) throw new IllegalStateException(reason); }
    private static String digest(Path path) throws Exception { return ResultBundleContract.sha256(Files.readAllBytes(path)); }
    private static void write(Path path, JsonNode document) throws Exception {
        Files.writeString(path, document.toPrettyString() + "\n", StandardOpenOption.CREATE_NEW);
    }
}
