package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/** Result-table writer with durable per-table recovery and post-verify publication. */
public final class SparkMlIcebergWriter implements MlIcebergWriter {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final SparkSession spark;
    private final String namespace;
    private final Runnable assertLease;
    private final Path journalRoot;
    private final Consumer<String> afterTableCommit;

    public SparkMlIcebergWriter(SparkSession spark, String namespace, Runnable assertLease) {
        this(spark, namespace, assertLease, null);
    }

    public SparkMlIcebergWriter(SparkSession spark, String namespace, Runnable assertLease, Path journalRoot) {
        this(spark, namespace, assertLease, journalRoot, ignored -> { });
    }

    SparkMlIcebergWriter(SparkSession spark, String namespace, Runnable assertLease, Path journalRoot,
                         Consumer<String> afterTableCommit) {
        if (namespace == null || !namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*"))
            throw new IllegalArgumentException("SAFE_ML_NAMESPACE_REQUIRED");
        this.spark = Objects.requireNonNull(spark);
        this.namespace = namespace;
        this.assertLease = Objects.requireNonNull(assertLease);
        this.journalRoot = journalRoot;
        this.afterTableCommit = Objects.requireNonNull(afterTableCommit);
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone")))
            throw new IllegalArgumentException("UTC_SESSION_REQUIRED");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + namespace);
    }

    @Override public ResultCommit commitResults(ResultBundleValidator.DecodedBundle bundle,
                                                 ResultBundleValidator.Context context,
                                                 String resultBundleSha256) throws Exception {
        assertLease.run();
        if (context.datasetPins() == null || context.experimentMetadata() == null)
            throw new IllegalArgumentException("IMP_EXPERIMENT_METADATA_REQUIRED");
        Reservation reservation = reserveExperiment(bundle, context, resultBundleSha256);
        if (!"IMPORT_VALIDATING".equals(reservation.status()))
            throw new IllegalStateException("IMP_EXPERIMENT_ALREADY_PUBLISHED");
        Journal journal = loadJournal(context, resultBundleSha256);
        long membership = snapshotFromJournalOrIceberg(journal.membershipSnapshot(), "sequence_membership", context,
            resultBundleSha256, bundle.memberships().size());
        if (membership <= 0) {
            membership = write(frame(bundle.memberships(), "memberships.parquet"), "sequence_membership", context, resultBundleSha256);
            journal = journal.withMembership(membership);
            persistJournal(context, journal);
        }
        long summary = snapshotFromJournalOrIceberg(journal.summarySnapshot(), "sequence_summary", context,
            resultBundleSha256, bundle.summaries().size());
        if (summary <= 0) {
            summary = write(frame(bundle.summaries(), "sequence_summary.parquet"), "sequence_summary", context, resultBundleSha256);
            journal = journal.withSummary(summary);
            persistJournal(context, journal);
        }
        return new ResultCommit(membership, summary);
    }

    /** Reserve the experiment ID in the namespace registry before result-table writes. */
    @Override public Reservation reserveExperiment(ResultBundleValidator.DecodedBundle bundle,
                                                   ResultBundleValidator.Context context,
                                                   String resultBundleSha256) throws Exception {
        assertLease.run();
        Journal journal = loadJournal(context, resultBundleSha256);
        String table = namespace + ".experiment_run";
        ensureExperimentTable(bundle, context, resultBundleSha256);
        Dataset<Row> existing = spark.table(table).filter(col("experiment_run_id").equalTo(context.runId()));
        long count = existing.count();
        if (count > 1) throw new IllegalStateException("IMP_EXPERIMENT_RUN_DUPLICATE");
        if (count == 1) {
            Row row = existing.first();
            if (!resultBundleSha256.equals(row.getAs("result_bundle_sha256")))
                throw new IllegalArgumentException("IMP_EXPERIMENT_RUN_REUSED");
            String status = row.getAs("experiment_status");
            if ("REJECTED".equals(status)) throw new IllegalStateException("IMP_REJECTED_RUN_TERMINAL");
            if ("CANDIDATE".equals(status)) {
                long membership = nullableSnapshot(row, "membership_snapshot_id");
                long summary = nullableSnapshot(row, "summary_snapshot_id");
                long experiment = findCandidateSnapshot(context.runId(), resultBundleSha256);
                if (membership <= 0 || summary <= 0 || experiment <= 0)
                    throw new IllegalStateException("IMP_CANDIDATE_SNAPSHOT_MISSING");
                return new Reservation(status, membership, summary, experiment);
            }
            if (!"IMPORT_VALIDATING".equals(status)) throw new IllegalStateException("IMP_INVALID_IMPORT_STATE");
            return new Reservation(status, nullableSnapshot(row, "membership_snapshot_id"),
                nullableSnapshot(row, "summary_snapshot_id"), 0L);
        }

        Dataset<Row> reservation = experimentFrame(bundle.success(), context, resultBundleSha256, 0L, 0L, true);
        reservation.writeTo(table).option("snapshot-property.ml.operation_id", context.runId())
            .option("snapshot-property.ml.result_bundle_sha256", resultBundleSha256)
            .option("snapshot-property.ml.expected_row_count", "1").append();
        long snapshot = currentSnapshotWithIdentity(table, context.runId(), resultBundleSha256);
        persistJournal(context, journal.withExperiment(snapshot));
        return new Reservation("IMPORT_VALIDATING", 0L, 0L, snapshot);
    }

    /** Persist a quality rejection in the namespace registry; infrastructure failures stay resumable. */
    @Override public void rejectExperiment(ResultBundleValidator.Context context, String resultBundleSha256,
                                           String reasonCode) throws Exception {
        assertLease.run();
        String table = namespace + ".experiment_run";
        if (!spark.catalog().tableExists(table)) return;
        Dataset<Row> existing = spark.table(table).filter(col("experiment_run_id").equalTo(context.runId()));
        long count = existing.count();
        if (count == 0) return;
        if (count != 1) throw new IllegalStateException("IMP_EXPERIMENT_RUN_DUPLICATE");
        Row row = existing.first();
        if (!resultBundleSha256.equals(row.getAs("result_bundle_sha256")))
            throw new IllegalArgumentException("IMP_EXPERIMENT_RUN_REUSED");
        String status = row.getAs("experiment_status");
        if ("CANDIDATE".equals(status) || "REJECTED".equals(status)) return;
        if (!"IMPORT_VALIDATING".equals(status)) throw new IllegalStateException("IMP_INVALID_IMPORT_STATE");
        existing.withColumn("experiment_status", lit("REJECTED"))
            .withColumn("status_reason_code", lit(reasonCode))
            .writeTo(table).option("snapshot-property.ml.operation_id", context.runId())
            .option("snapshot-property.ml.result_bundle_sha256", resultBundleSha256)
            .option("snapshot-property.ml.expected_row_count", "1").overwritePartitions();
        Row rejected = spark.table(table).filter(col("experiment_run_id").equalTo(context.runId())).first();
        if (!"REJECTED".equals(rejected.getAs("experiment_status")))
            throw new IllegalStateException("IMP_REJECTION_STATE_UNCONFIRMED");
    }

    @Override public long publishCandidate(ResultBundleValidator.DecodedBundle bundle,
                                            ResultBundleValidator.Context context,
                                            String resultBundleSha256, ResultCommit commit) throws Exception {
        assertLease.run();
        String table = namespace + ".experiment_run";
        Reservation reservation = reserveExperiment(bundle, context, resultBundleSha256);
        if ("CANDIDATE".equals(reservation.status())) {
            if (reservation.membershipSnapshot() != commit.membershipSnapshot()
                    || reservation.summarySnapshot() != commit.summarySnapshot())
                throw new IllegalStateException("IMP_CANDIDATE_SNAPSHOT_IDENTITY_MISMATCH");
            return reservation.experimentSnapshot();
        }
        Dataset<Row> existing = spark.table(table).filter(col("experiment_run_id").equalTo(context.runId()));
        if (existing.count() != 1 || !"IMPORT_VALIDATING".equals(existing.first().getAs("experiment_status")))
            throw new IllegalStateException("IMP_INVALID_IMPORT_STATE");
        Dataset<Row> row = experimentFrame(bundle.success(), context, resultBundleSha256,
            commit.membershipSnapshot(), commit.summarySnapshot(), false);
        row.writeTo(table).option("snapshot-property.ml.operation_id", context.runId())
            .option("snapshot-property.ml.result_bundle_sha256", resultBundleSha256)
            .option("snapshot-property.ml.expected_row_count", "1").overwritePartitions();
        long snapshot = currentSnapshotWithIdentity(table, context.runId(), resultBundleSha256);
        if (spark.table(table).filter(col("experiment_run_id").equalTo(context.runId())
                .and(col("experiment_status").equalTo("CANDIDATE"))).count() != 1)
            throw new IllegalStateException("IMP_CANDIDATE_READBACK_FAILED");
        persistJournal(context, loadJournal(context, resultBundleSha256).withExperiment(snapshot));
        return snapshot;
    }

    @Override public Commit commit(ResultBundleValidator.DecodedBundle bundle,
                                   ResultBundleValidator.Context context, String resultBundleSha256) {
        throw new IllegalStateException("IMP_PUBLICATION_REQUIRES_VERIFICATION");
    }

    @Override public Map<String, Long> verify(Commit commit, ResultBundleValidator.Context context) throws Exception {
        assertLease.run();
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("sequence_membership", snapshot("sequence_membership", commit.membershipSnapshot(), context.runId()).count());
        counts.put("sequence_summary", snapshot("sequence_summary", commit.summarySnapshot(), context.runId()).count());
        if (commit.experimentSnapshot() > 0) counts.put("experiment_run", snapshot("experiment_run", commit.experimentSnapshot(), context.runId()).count());
        if (counts.get("sequence_membership") <= 0 || counts.get("sequence_summary") <= 0
                || (commit.experimentSnapshot() > 0 && counts.get("experiment_run") != 1))
            throw new IllegalStateException("IMP_READBACK_COUNT_MISMATCH");
        return counts;
    }

    @Override public Map<String, Long> readback(ResultCommit commit, long experimentSnapshot,
                                                ResultBundleValidator.Context context) throws Exception {
        return verify(new Commit(commit.membershipSnapshot(), commit.summarySnapshot(), experimentSnapshot), context);
    }

    private long snapshotFromJournalOrIceberg(long pinned, String table, ResultBundleValidator.Context context,
                                              String fingerprint, long expectedRows) throws Exception {
        if (pinned > 0 && snapshotMatches(table, pinned, context.runId(), fingerprint, expectedRows)) return pinned;
        return findSnapshot(table, context.runId(), fingerprint, expectedRows);
    }

    private long findSnapshot(String table, String runId, String fingerprint, long expectedRows) throws Exception {
        String name = namespace + "." + table;
        if (!spark.catalog().tableExists(name)) return 0L;
        Table iceberg = Spark3Util.loadIcebergTable(spark, name);
        iceberg.refresh();
        List<Snapshot> snapshots = new ArrayList<>();
        iceberg.snapshots().forEach(snapshots::add);
        Collections.reverse(snapshots);
        for (Snapshot snapshot : snapshots) {
            if (runId.equals(snapshot.summary().get("ml.operation_id"))
                    && fingerprint.equals(snapshot.summary().get("ml.result_bundle_sha256"))
                    && snapshotMatches(table, snapshot.snapshotId(), runId, fingerprint, expectedRows)) return snapshot.snapshotId();
        }
        return 0L;
    }

    private boolean snapshotMatches(String table, long snapshotId, String runId, String fingerprint, long expectedRows) throws Exception {
        if (snapshotId <= 0) return false;
        String name = namespace + "." + table;
        if (!spark.catalog().tableExists(name)) return false;
        Table iceberg = Spark3Util.loadIcebergTable(spark, name);
        Snapshot snapshot = iceberg.snapshot(snapshotId);
        if (snapshot == null || !runId.equals(snapshot.summary().get("ml.operation_id"))
                || !fingerprint.equals(snapshot.summary().get("ml.result_bundle_sha256"))) return false;
        return snapshot(table, snapshotId, runId).count() == expectedRows;
    }

    private Dataset<Row> snapshot(String table, long snapshotId, String runId) {
        String name = namespace + "." + table;
        if (!spark.catalog().tableExists(name)) throw new IllegalStateException("IMP_TABLE_MISSING");
        return spark.read().format("iceberg").option("snapshot-id", Long.toString(snapshotId)).load(name)
            .filter(col("experiment_run_id").equalTo(runId));
    }

    private void ensureExperimentTable(ResultBundleValidator.DecodedBundle bundle,
                                       ResultBundleValidator.Context context, String fingerprint) {
        String table = namespace + ".experiment_run";
        if (spark.catalog().tableExists(table)) return;
        Dataset<Row> shape = experimentFrame(bundle.success(), context, fingerprint, 0L, 0L, true);
        String columns = Arrays.stream(shape.schema().fields()).map(field ->
            "`" + field.name() + "` " + field.dataType().sql()).reduce((left, right) -> left + "," + right).orElseThrow();
        spark.sql("CREATE TABLE " + table + " (" + columns + ") USING iceberg PARTITIONED BY (experiment_run_id) "
            + "TBLPROPERTIES ('format-version'='2','write.format.default'='parquet')");
    }

    private long currentSnapshotWithIdentity(String table, String runId, String fingerprint) throws Exception {
        Table iceberg = Spark3Util.loadIcebergTable(spark, table);
        iceberg.refresh();
        Snapshot snapshot = iceberg.currentSnapshot();
        if (snapshot == null || !runId.equals(snapshot.summary().get("ml.operation_id"))
                || !fingerprint.equals(snapshot.summary().get("ml.result_bundle_sha256")))
            throw new IllegalStateException("IMP_SNAPSHOT_IDENTITY_MISMATCH");
        return snapshot.snapshotId();
    }

    private long findCandidateSnapshot(String runId, String fingerprint) throws Exception {
        String table = namespace + ".experiment_run";
        Table iceberg = Spark3Util.loadIcebergTable(spark, table);
        iceberg.refresh();
        List<Snapshot> snapshots = new ArrayList<>();
        iceberg.snapshots().forEach(snapshots::add);
        Collections.reverse(snapshots);
        for (Snapshot snapshot : snapshots) {
            if (runId.equals(snapshot.summary().get("ml.operation_id"))
                    && fingerprint.equals(snapshot.summary().get("ml.result_bundle_sha256"))
                    && snapshot("experiment_run", snapshot.snapshotId(), runId)
                        .filter(col("experiment_status").equalTo("CANDIDATE")).count() == 1)
                return snapshot.snapshotId();
        }
        return 0L;
    }

    private static long nullableSnapshot(Row row, String field) {
        Object value = row.getAs(field);
        return value instanceof Number number ? number.longValue() : 0L;
    }

    private long write(Dataset<Row> frame, String table, ResultBundleValidator.Context context,
                       String resultBundleSha256) throws Exception {
        assertLease.run();
        String name = namespace + "." + table;
        if (!spark.catalog().tableExists(name)) {
            String columns = Arrays.stream(frame.schema().fields()).map(field ->
                "`" + field.name() + "` " + field.dataType().sql()).reduce((left, right) -> left + "," + right).orElseThrow();
            spark.sql("CREATE TABLE " + name + " (" + columns + ") USING iceberg PARTITIONED BY (experiment_run_id) "
                + "TBLPROPERTIES ('format-version'='2','write.format.default'='parquet')");
        }
        frame.writeTo(name).option("snapshot-property.ml.operation_id", context.runId())
            .option("snapshot-property.ml.result_bundle_sha256", resultBundleSha256)
            .option("snapshot-property.ml.expected_row_count", Long.toString(frame.count())).append();
        Table iceberg = Spark3Util.loadIcebergTable(spark, name);
        iceberg.refresh();
        Snapshot snapshot = iceberg.currentSnapshot();
        if (snapshot == null || !context.runId().equals(snapshot.summary().get("ml.operation_id"))
                || !resultBundleSha256.equals(snapshot.summary().get("ml.result_bundle_sha256")))
            throw new IllegalStateException("IMP_SNAPSHOT_IDENTITY_MISMATCH");
        afterTableCommit.accept(table);
        return snapshot.snapshotId();
    }

    private Dataset<Row> experimentFrame(JsonNode success, ResultBundleValidator.Context context,
                                         String fingerprint, long membershipSnapshot, long summarySnapshot,
                                         boolean validating) {
        ResultBundleValidator.ExperimentMetadata metadata = context.experimentMetadata();
        if (metadata == null || context.datasetPins() == null) throw new IllegalArgumentException("IMP_EXPERIMENT_METADATA_REQUIRED");
        StructType schema = new StructType(new StructField[] {
            field("schema_version", DataTypes.StringType, false), field("experiment_run_id", DataTypes.StringType, false),
            field("dataset_id", DataTypes.StringType, false), field("algorithm_name", DataTypes.StringType, false),
            field("algorithm_version", DataTypes.StringType, false), field("model_config_version", DataTypes.StringType, false),
            field("experiment_config_json", DataTypes.StringType, false), field("experiment_config_sha256", DataTypes.StringType, false),
            field("code_version", DataTypes.StringType, false), field("notebook_version", DataTypes.StringType, true),
            field("requirements_lock_sha256", DataTypes.StringType, false), field("runtime_environment_json", DataTypes.StringType, false),
            field("random_seed", DataTypes.LongType, true), field("result_artifact_uri", DataTypes.StringType, true),
            field("result_bundle_sha256", DataTypes.StringType, true), field("membership_row_count", DataTypes.LongType, true),
            field("summary_row_count", DataTypes.LongType, true), field("import_run_id", DataTypes.StringType, false),
            field("membership_snapshot_id", DataTypes.LongType, true), field("summary_snapshot_id", DataTypes.LongType, true),
            field("experiment_status", DataTypes.StringType, false), field("status_reason_code", DataTypes.StringType, true),
            field("created_at_utc", DataTypes.StringType, false), field("result_ready_at_utc", DataTypes.StringType, false),
            field("imported_at_utc", DataTypes.StringType, true), field("approved_at_utc", DataTypes.StringType, true),
            field("approved_by", DataTypes.StringType, true), field("dataset_registry_snapshot_id", DataTypes.StringType, false),
            field("candidate_source_snapshot_id", DataTypes.StringType, false), field("mainshock_source_snapshot_id", DataTypes.StringType, false),
            field("dataset_manifest_sha256", DataTypes.StringType, false), field("export_manifest_sha256", DataTypes.StringType, false)
        });
        Object[] values = new Object[] {
            success.path("schema_version").asText(), context.runId(), context.datasetId(), context.algorithm(),
            success.path("algorithm_version").asText(), context.modelVersion(), metadata.configJson(), metadata.configSha256(),
            success.path("code_version").asText(), metadata.notebookVersion(), metadata.requirementsLockSha256(), metadata.runtimeEnvironmentJson(),
            metadata.randomSeed(), metadata.artifactUri(), fingerprint, success.path("membership_row_count").asLong(), success.path("summary_row_count").asLong(),
            metadata.importRunId(), validating ? null : membershipSnapshot, validating ? null : summarySnapshot,
            validating ? "IMPORT_VALIDATING" : "CANDIDATE", null, metadata.createdAtUtc(), metadata.resultReadyAtUtc(),
            validating ? null : Instant.now().toString(), null, null, context.datasetPins().registrySnapshotId(),
            context.datasetPins().candidateSnapshotId(), context.datasetPins().mainshockSnapshotId(), context.datasetPins().manifestSha256(),
            context.datasetPins().exportManifestSha256()
        };
        return spark.createDataFrame(List.of(org.apache.spark.sql.RowFactory.create(values)), schema);
    }

    private static StructField field(String name, DataType type, boolean nullable) {
        return new StructField(name, type, nullable, Metadata.empty());
    }

    private Dataset<Row> frame(List<JsonNode> rows, String artifact) {
        StructType schema = schema(artifact);
        List<Row> values = rows.stream().map(row -> MlRowFactory.create(schema.fields(), row)).toList();
        return spark.createDataFrame(values, schema);
    }

    private static StructType schema(String artifact) {
        List<StructField> fields = new ArrayList<>();
        for (JsonNode field : ResultBundleContract.contract().path("schemas").path(artifact).path("fields")) {
            String type = field.path("type").asText();
            DataType data = switch (type) {
                case "string", "json_string", "timestamp_utc" -> DataTypes.StringType;
                case "boolean" -> DataTypes.BooleanType;
                case "integer" -> DataTypes.IntegerType;
                case "long" -> DataTypes.LongType;
                case "double" -> DataTypes.DoubleType;
                case "array<string>" -> DataTypes.createArrayType(DataTypes.StringType, false);
                default -> throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH");
            };
            fields.add(field(field.path("name").asText(), data, field.path("nullable").asBoolean()));
        }
        return new StructType(fields.toArray(StructField[]::new));
    }

    private Journal loadJournal(ResultBundleValidator.Context context, String fingerprint) throws Exception {
        if (journalRoot == null) return new Journal(namespace, context.runId(), fingerprint, 0, 0, 0);
        Path path = journalPath(context);
        if (!Files.exists(path)) return new Journal(namespace, context.runId(), fingerprint, 0, 0, 0);
        JsonNode node = JSON.readTree(Files.readAllBytes(path));
        if (!namespace.equals(node.path("namespace").asText()) || !context.runId().equals(node.path("run_id").asText())
                || !fingerprint.equals(node.path("fingerprint").asText())) throw new IllegalStateException("IMP_JOURNAL_IDENTITY_DRIFT");
        return new Journal(namespace, context.runId(), fingerprint, node.path("membership_snapshot_id").asLong(),
            node.path("summary_snapshot_id").asLong(), node.path("experiment_snapshot_id").asLong());
    }

    private void persistJournal(ResultBundleValidator.Context context, Journal journal) throws Exception {
        if (journalRoot == null) return;
        Path path = journalPath(context); Files.createDirectories(path.getParent());
        ObjectNode node = JSON.createObjectNode().put("version", "mli03-journal-v2").put("namespace", journal.namespace())
            .put("run_id", journal.runId()).put("fingerprint", journal.fingerprint())
            .put("membership_snapshot_id", journal.membershipSnapshot()).put("summary_snapshot_id", journal.summarySnapshot())
            .put("experiment_snapshot_id", journal.experimentSnapshot());
        Path tmp = Files.createTempFile(path.getParent(), ".mli03-journal-", ".tmp");
        try { Files.writeString(tmp, JSON.writeValueAsString(node) + "\n", StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); }
        finally { Files.deleteIfExists(tmp); }
    }

    private Path journalPath(ResultBundleValidator.Context context) {
        return journalRoot.resolve("tables").resolve("experiment-run-" + context.runId() + ".json");
    }

    private long latestSnapshot(String table) throws Exception {
        Table iceberg = Spark3Util.loadIcebergTable(spark, table); iceberg.refresh();
        Snapshot snapshot = iceberg.currentSnapshot(); if (snapshot == null) throw new IllegalStateException("IMP_SNAPSHOT_MISSING");
        return snapshot.snapshotId();
    }

    private record Journal(String namespace, String runId, String fingerprint, long membershipSnapshot,
                           long summarySnapshot, long experimentSnapshot) {
        Journal withMembership(long value) { return new Journal(namespace, runId, fingerprint, value, summarySnapshot, experimentSnapshot); }
        Journal withSummary(long value) { return new Journal(namespace, runId, fingerprint, membershipSnapshot, value, experimentSnapshot); }
        Journal withExperiment(long value) { return new Journal(namespace, runId, fingerprint, membershipSnapshot, summarySnapshot, value); }
    }

    private static final class MlRowFactory {
        static Row create(StructField[] fields, JsonNode row) {
            Object[] values = new Object[fields.length];
            for (int index = 0; index < fields.length; index++) values[index] = value(fields[index].dataType(), row.get(fields[index].name()));
            return org.apache.spark.sql.RowFactory.create(values);
        }
        private static Object value(DataType type, JsonNode value) {
            if (value == null || value.isNull()) return null;
            if (type.equals(DataTypes.StringType)) return value.asText();
            if (type.equals(DataTypes.BooleanType)) return value.asBoolean();
            if (type.equals(DataTypes.IntegerType)) return value.asInt();
            if (type.equals(DataTypes.LongType)) return value.asLong();
            if (type.equals(DataTypes.DoubleType)) return value.asDouble();
            if (type instanceof ArrayType) { List<String> values = new ArrayList<>(); value.forEach(item -> values.add(item.asText())); return values; }
            throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH");
        }
    }
}
