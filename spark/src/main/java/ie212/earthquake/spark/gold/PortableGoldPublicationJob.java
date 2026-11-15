package ie212.earthquake.spark.gold;

import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.bronze.PortableBronzeProvenance;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.MinioBronzeObjectStore;
import java.net.URI;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.*;

/** DAT-03 part2. Isolated PREPARED -> six new snapshots -> actual Trino publication. */
public final class PortableGoldPublicationJob {
    public static final String DAG = "dat03_portable_gold";
    private static final ObjectMapper JSON = PortablePreparedCatalog.JSON;
    private PortableGoldPublicationJob() { }
    private static void require(boolean ok, String reason) throws Exception { PortableImportContract.require(ok, reason); }
    private static String sha(Path path) throws Exception { return PortableInputMaterializer.fileSha(path); }
    public static void validate(JsonNode r) throws Exception {
        require(PreparedGoldProof.VERSION.equals(r.path("version").asText()) && r.path("run_id").asText().matches("dat03-gold-[a-f0-9]{32}")
                && DAG.equals(r.path("dag_id").asText()) && PreparedGoldProof.NAMESPACE.equals(r.path("namespace").asText())
                && r.path("runner_sha256").asText().matches("[a-f0-9]{64}")
                && r.path("prepared_root").asText().matches("/opt/pipeline/staging/dat03-portable-import/[a-f0-9]{32}/prepared")
                && r.path("prepared_catalog_sha256").asText().matches("[a-f0-9]{64}"), "EXACT_PREPARED_PUBLICATION_REQUEST_REQUIRED");
        var proof = PreparedGoldProof.from(r.path("prepared_input"));
        require(proof.catalogSha256().equals(r.path("prepared_catalog_sha256").asText()), "PREPARED_REQUEST_PROOF_CHANGED");
        Instant processed = Instant.parse(r.path("processed_at_utc").asText());
        require(!processed.isBefore(Instant.parse(proof.sourceCutoffUtc()))
                && processed.atZone(ZoneOffset.UTC).toLocalDate().toString().equals(r.path("processing_date").asText()), "FROZEN_UTC_PROCESSING_CONTEXT_REQUIRED");
        require(r.path("expected_counts").isObject() && r.path("expected_counts").size() == 6, "SIX_PREPARED_COUNTS_REQUIRED");
        for (String table : GoldIcebergWriter.TABLES) {
            var count = r.path("expected_counts").path(table);
            require(count.isIntegralNumber() && count.canConvertToLong() && count.asLong() >= 0 && count.asLong() <= PreparedGoldProof.MAX_FACT_ROWS,
                    "PREPARED_MEASURED_CAPACITY_EXCEEDED");
        }
        for (String flag : List.of("published", "research_ready", "official_export_ready", "scope_completeness", "silver_bundle_sha256"))
            require(!r.has(flag), "CALLER_READINESS_OR_LEGACY_PROOF_FORBIDDEN");
    }
    private static Runnable lease(JsonNode r) {
        return () -> { try {
            Path owner = Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")), "owner.json");
            JsonNode live = PortablePreparedCatalog.read(owner, 65536);
            if (live.size() != 2 || !DAG.equals(live.path("dag_id").asText()) || !r.path("run_id").equals(live.path("run_id")))
                throw new IllegalStateException("PREPARED_GOLD_OWN_LEASE_NOT_HELD");
        } catch (Exception failure) { throw new IllegalStateException("PREPARED_GOLD_OWN_LEASE_NOT_HELD"); } };
    }
    /** Read-only full-file/checkpoint and fresh physical Bronze proof; shared by the explicit ML adapter. */
    public static JsonNode verifyInput(JsonNode r) throws Exception {
        validate(r);
        Path root = Path.of(r.path("prepared_root").asText());
        JsonNode catalog = PortablePreparedCatalog.verify(root, r.path("prepared_catalog_sha256").asText());
        JsonNode original = PortablePreparedCatalog.read(PortablePreparedCatalog.relative(root, catalog.path("request").path("relative_path").asText()), 1024 * 1024);
        var proof = new PreparedGoldProof(r.path("namespace").asText(), r.path("prepared_catalog_sha256").asText(),
                catalog.path("request").path("sha256").asText(), catalog.path("coverage").path("sha256").asText(),
                catalog.path("provenance").path("sha256").asText(), original.path("source_cutoff_utc").asText());
        require(proof.json().equals(r.path("prepared_input")), "PREPARED_COVERAGE_PROVENANCE_BINDING_CHANGED");
        for (String name : GoldIcebergWriter.TABLES)
            require(catalog.path("tables").path(name).path("row_count").equals(r.path("expected_counts").path(name)), "PREPARED_EXPECTED_COUNT_CHANGED");
        Path sourcePlan = PortablePreparedCatalog.relative(root.getParent(), original.path("bronze_plan").path("relative_path").asText());
        require(sha(sourcePlan).equals(original.path("bronze_plan").path("sha256").asText()), "PREPARED_BRONZE_PLAN_CHANGED");
        try (var bronze = MinioBronzeObjectStore.fromEnvironment()) {
            var provenance = PortableBronzeProvenance.verify(PortablePreparedCatalog.read(sourcePlan, 4 * 1024 * 1024), sha(sourcePlan), bronze);
            require(PortablePreparedCatalog.persistedJsonEquals(root.resolve("provenance.json"), provenance), "PREPARED_FRESH_PHYSICAL_SOURCE_CHANGED");
        }
        return catalog;
    }
    static GoldCommitScope scope(JsonNode r) {
        var zero = new TreeMap<String, Long>(); GoldIcebergWriter.TABLES.forEach(name -> zero.put(name, 0L));
        var context = new GoldRunContext(r.path("run_id").asText(), Instant.parse("2000-01-01T00:00:00Z"), Instant.parse("2024-01-01T00:00:00Z"),
                LocalDate.parse(r.path("processing_date").asText()), true, PreparedGoldProof.VERSION,
                List.of(Path.of(r.path("prepared_root").asText(), "catalog.json").toUri().toString()));
        return new GoldCommitScope(context, null, PreparedGoldProof.VERSION, PortableImportContract.ALL_MONTHS, zero, PreparedGoldProof.from(r.path("prepared_input")));
    }
    private static void anchor(SilverObjectStore store, JsonNode r, byte[] raw, boolean create) throws Exception {
        String key = "dat03-prepared-gold/" + r.path("run_id").asText() + "/request.json";
        if (!store.exists(key)) { require(create, "PREPARED_GOLD_REQUEST_ANCHOR_MISSING"); store.put(key, raw, "application/json"); }
        require(Arrays.equals(store.read(key), raw), "PREPARED_GOLD_REQUEST_ANCHOR_CHANGED");
    }
    private static GoldPublisher.Sql client(JsonNode r, String phase, int total, int[] ordinal) {
        var real = TrinoSqlClient.researchCatalog(URI.create("http://" + Objects.requireNonNull(System.getenv("TRINO_HOST")) + ":"
                + Objects.requireNonNull(System.getenv("TRINO_INTERNAL_PORT"))));
        return sql -> {
            System.out.println("event=prepared_gold_query run_id=" + r.path("run_id").asText() + " phase=" + phase + " query=" + (++ordinal[0]) + "/" + total);
            return real.execute(sql);
        };
    }
    public static int verificationQueries(JsonNode commit) { return 18 + new GoldVerificationPlan(commit).blockers().size()
            + 15 * GoldVerificationPlan.BUCKETED_FACT_BLOCKERS.size() + 32; }
    private static long scalar(GoldPublisher.Sql sql, String query) throws Exception {
        var rows = sql.execute(query).rows(); require(rows.size() == 1 && rows.get(0).size() == 1 && rows.get(0).get(0).isIntegralNumber(), "PREPARED_TRINO_SCALAR_REQUIRED");
        return rows.get(0).get(0).asLong();
    }
    static SilverObjectStore readOnly(SilverObjectStore source) {
        return new SilverObjectStore() {
            public byte[] read(String k) throws java.io.IOException { return source.read(k); }
            public boolean exists(String k) throws java.io.IOException { return source.exists(k); }
            public List<String> list(String k) throws java.io.IOException { return source.list(k); }
            public String uriForKey(String k) { return source.uriForKey(k); }
            public void put(String k, byte[] b, String t) throws java.io.IOException { throw new java.io.IOException("RERUN_JOURNAL_WRITE_FORBIDDEN"); }
            public void delete(String k) throws java.io.IOException { throw new java.io.IOException("RERUN_DELETE_FORBIDDEN"); }
            public void deletePrefix(String k) throws java.io.IOException { throw new java.io.IOException("RERUN_DELETE_FORBIDDEN"); }
            public void move(String a, String b) throws java.io.IOException { throw new java.io.IOException("RERUN_MOVE_FORBIDDEN"); }
        };
    }
    public static void main(String[] args) throws Exception {
        require((args.length == 4 || args.length == 6) && Set.of("preflight", "commit", "publication", "commit-rerun", "publication-rerun", "recover-commit", "recover-publication").contains(args[2])
                && args[3].matches("[a-f0-9]{32}"), "PREPARED_PHASE_ARGS_REQUIRED");
        Path requestFile = PortableInputMaterializer.safe(Path.of(args[0])), root = requestFile.getParent();
        require(root.toString().matches("/opt/pipeline/staging/dat03-portable-gold/[a-f0-9]{32}") && sha(requestFile).equals(args[1]), "PINNED_GOLD_OPERATION_ROOT_REQUIRED");
        JsonNode r = PortablePreparedCatalog.read(requestFile, 65536); validate(r);
        require(r.path("run_id").asText().equals("dat03-gold-" + root.getFileName()), "PREPARED_OPERATION_ID_CHANGED");
        Path runner = Path.of(PortableGoldPublicationJob.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        boolean recovery = args[2].startsWith("recover-"); JsonNode continuation = null;
        if (recovery) {
            require(args.length == 6, "EXPLICIT_PINNED_CONTINUATION_REQUIRED");
            continuation = PreparedGoldContinuation.validate(root, Path.of(args[4]), args[5], r, args[1], runner);
        } else require(sha(runner).equals(r.path("runner_sha256").asText()), "PREPARED_PUBLICATION_RUNNER_CHANGED");
        boolean rerun = args[2].endsWith("-rerun"); String phase = args[2].replace("-rerun", "").replace("recover-", ""), run = r.path("run_id").asText();
        JsonNode rerunPins = null;
        if (rerun) {
            require(args.length == 6 && Path.of(args[4]).equals(root.resolve("accepted-pins.json")) && sha(Path.of(args[4])).equals(args[5]), "TRUSTED_RERUN_PINS_REQUIRED");
            rerunPins = PortablePreparedCatalog.read(Path.of(args[4]), 65536);
            require(rerunPins.path("request_sha256").asText().equals(args[1]), "RERUN_REQUEST_CHANGED");
        } else require(recovery || args.length == 4, "RERUN_PINS_ONLY_IN_EXPLICIT_RERUN");
        Path output = root.resolve("results/" + args[3] + "-" + phase + ".json");
        require(!Files.exists(output), "IMMUTABLE_PHASE_OUTPUT_REQUIRED"); Files.createDirectories(output.getParent());
        var result = JSON.createObjectNode().put("version", PreparedGoldProof.VERSION).put("run_id", run).put("phase", phase).put("request_sha256", args[1])
                .put("prepared_catalog_sha256", r.path("prepared_catalog_sha256").asText()).put("official_export_ready", false).put("research_ready", false).put("ml_upstream_eligible", false);
        if (recovery) result.put("continuation_sha256", args[5]).put("actual_runner_sha256", sha(runner));
        if (phase.equals("preflight")) {
            require(!Files.exists(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")), "owner.json")), "PREPARED_PREFLIGHT_SOURCE_NOT_IDLE");
            verifyInput(r); int[] n = {0}; var sql = client(r, "preflight", 2, n);
            require(scalar(sql, "SELECT 1") == 1, "PREPARED_TRINO_NOT_READY");
            require(scalar(sql, "SELECT count(*) FROM iceberg.information_schema.tables WHERE table_schema='dat03_unified_20261114'") == 0, "APPROVED_NEW_NAMESPACE_NOT_EMPTY");
            result.put("status", "READ_ONLY_PREFLIGHT_PASSED").put("gold_published", false).put("sql_queries", n[0]);
        } else {
            Runnable held = lease(r); held.run(); verifyInput(r);
            try (var physicalStore = MinioSilverObjectStore.fromEnvironment()) {
                SilverObjectStore store = rerun ? readOnly(physicalStore) : physicalStore;
                if (recovery) {
                    anchor(store, r, Files.readAllBytes(requestFile), false);
                    require(SilverBundlePublisher.sha(store.read("gold-commits/" + run + "/identity.json"))
                            .equals(continuation.path("original_identity_raw_sha256").asText()), "RECOVERY_ORIGINAL_IDENTITY_CHANGED");
                }
                if (rerun) {
                    require(store.exists("gold-commits/" + run + "/identity.json")
                            && SilverBundlePublisher.sha(store.read("gold-commits/" + run + "/commit.json")).equals(rerunPins.path("commit_sha256").asText())
                            && SilverBundlePublisher.sha(store.read("gold-verification/" + run + "/publication.json")).equals(rerunPins.path("publication_sha256").asText()), "RERUN_STORED_IMMUTABLE_ARTIFACT_CHANGED");
                }
                if (phase.equals("commit")) {
                    var builder = GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-prepared-gold-commit")
                            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "16").config("spark.sql.adaptive.enabled", "false")
                            .config("spark.local.dir", root.resolve("spill").toString());
                    PortableGoldCapacityJob.readCompatibilityOptions().forEach(builder::config);
                    var spark = builder.getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
                    try {
                        var writer = new GoldIcebergWriter(spark, r.path("namespace").asText(), store, held);
                        if (recovery) {
                            require(continuation.path("before_snapshots").size() == 6, "SIX_PARTIAL_BASELINES_REQUIRED");
                            for (var item : writer.baselines().entrySet())
                                require(Long.toString(item.getValue()).equals(continuation.path("before_snapshots").path(item.getKey()).asText()), "RECOVERY_PARTIAL_SNAPSHOT_CHANGED");
                        }
                        String commitKey = "gold-commits/" + run + "/commit.json";
                        if (!store.exists("dat03-prepared-gold/" + run + "/request.json"))
                            require(writer.baselines().values().stream().allMatch(id -> id == 0), "PREPARED_NAMESPACE_ALREADY_OWNED");
                        anchor(store, r, Files.readAllBytes(requestFile), !rerun);
                        var data = PortablePreparedCatalog.asGoldInput(spark, Path.of(r.path("prepared_root").asText()), r.path("prepared_catalog_sha256").asText(), true);
                        Dataset<Row> events = data.eventCurrent().withColumn("gold_run_id", lit(run))
                                .withColumn("record_updated_at_utc", lit(Timestamp.from(Instant.parse(r.path("processed_at_utc").asText()))));
                        data = new GoldTransformationResult(events, events.filter(col("is_natural_earthquake").and(col("is_in_study_area"))), data.eventSourceBridge(),
                                data.dimDate(), data.dimRegion(), data.dimMagnitudeBand(), data.dimDepthBand(), data.currentObservationCount(), data.canonicalEventCount(), data.bridgeRowCount());
                        boolean reused = store.exists(commitKey);
                        JsonNode commit = writer.commit(data, scope(r), name -> System.out.println("event=prepared_gold_table_committed run_id=" + run + " table=" + name));
                        PreparedGoldProof.validateCommit(commit); result.set("commit", commit);
                        result.put("commit_sha256", SilverBundlePublisher.sha(store.read(commitKey))).put("reused", reused).put("gold_published", false).put("status", "COMMITTED");
                    } finally { spark.stop(); }
                } else {
                    anchor(store, r, Files.readAllBytes(requestFile), false);
                    byte[] raw = store.read("gold-commits/" + run + "/commit.json"); JsonNode commit = JSON.readTree(raw); PreparedGoldProof.validateCommit(commit);
                    JsonNode normalizedScope = JSON.readTree(JSON.writeValueAsBytes(GoldIcebergWriter.scopeJson(scope(r))));
                    require(commit.path("scope").equals(normalizedScope) && commit.path("gold_run_id").asText().equals(run), "PREPARED_STORED_SCOPE_CHANGED");
                    boolean reused = store.exists("gold-verification/" + run + "/publication.json"); int[] verifyN = {0}, publishN = {0};
                    var verifySql = client(r, "verify", verificationQueries(commit), verifyN);
                    var publishSql = client(r, "publish", reused ? 15 : 41, publishN);
                    final boolean[] publishing = {false};
                    var publisher = GoldPublisher.preparedCatalog(sql -> {
                        if (rerun && !sql.stripLeading().toUpperCase(Locale.ROOT).startsWith("SELECT "))
                            throw new java.io.IOException("RERUN_SQL_MUTATION_FORBIDDEN");
                        return (publishing[0] ? publishSql : verifySql).execute(sql);
                    }, store, held);
                    JsonNode verification = publisher.verify(run, r.path("namespace").asText(), commit.path("identity_sha256").asText());
                    publishing[0] = true; JsonNode publication = publisher.publish(run, r.path("namespace").asText(), commit.path("identity_sha256").asText());
                    var plan = new GoldVerificationPlan(commit);
                    for (String name : GoldIcebergWriter.TABLES) {
                        held.run(); require(scalar(publishSql, "SELECT snapshot_id FROM " + plan.metadata(name, "refs") + " WHERE name='main'") == plan.snapshot(name), "PREPARED_FINAL_SNAPSHOT_MOVED");
                        require(scalar(publishSql, "SELECT count(*) FROM " + plan.metadata(name, "snapshots")) == 1, "PREPARED_UNEXPECTED_EXTRA_SNAPSHOT");
                    }
                    require(scalar(publishSql, "SELECT count(*) FROM " + plan.ref("event_current") + " WHERE gold_run_id<>" + GoldVerificationPlan.quote(run)) == 0, "PREPARED_GOLD_RUN_PROVENANCE_CHANGED");
                    require(publication.path("published").asBoolean(false) && "PASSED".equals(publication.path("verify_status").asText()), "PREPARED_PUBLICATION_NOT_PASSED");
                    result.set("verification", verification); result.set("publication", publication); result.set("snapshots", commit.path("tables"));
                    result.put("commit_sha256", SilverBundlePublisher.sha(raw)).put("publication_sha256", SilverBundlePublisher.sha(store.read("gold-verification/" + run + "/publication.json")))
                            .put("verify_queries", verifyN[0]).put("publish_queries", publishN[0]).put("reused", reused).put("gold_published", true).put("status", "PUBLISHED");
                }
                held.run();
            }
        }
        PortablePreparedCatalog.save(output, result);
        System.out.println("event=prepared_gold_phase_completed run_id=" + run + " phase=" + phase + " status=" + result.path("status").asText());
    }
}
