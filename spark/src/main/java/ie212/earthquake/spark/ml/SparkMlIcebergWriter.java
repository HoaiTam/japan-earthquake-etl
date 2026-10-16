package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.col;

/** Spark/Iceberg writer for the three ML import tables. Publication is only recorded after readback. */
public final class SparkMlIcebergWriter implements MlIcebergWriter {
    private final SparkSession spark;
    private final String namespace;
    private final Runnable assertLease;

    public SparkMlIcebergWriter(SparkSession spark, String namespace, Runnable assertLease) {
        if (namespace == null || !namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*")) throw new IllegalArgumentException("SAFE_ML_NAMESPACE_REQUIRED");
        this.spark = Objects.requireNonNull(spark); this.namespace = namespace; this.assertLease = Objects.requireNonNull(assertLease);
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone"))) throw new IllegalArgumentException("UTC_SESSION_REQUIRED");
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + namespace);
    }

    @Override public Commit commit(ResultBundleValidator.DecodedBundle bundle, ResultBundleValidator.Context context,
                                   String resultBundleSha256) throws Exception {
        assertLease.run();
        String runId = context.runId();
        String experimentTable = namespace + ".experiment_run";
        if (spark.catalog().tableExists(experimentTable)) {
            Dataset<Row> existing = spark.table(experimentTable).filter(col("experiment_run_id").equalTo(runId));
            if (existing.count() > 0) {
                Row row = existing.orderBy(col("membership_snapshot_id").desc()).first();
                if (!resultBundleSha256.equals(row.getAs("result_bundle_sha256")))
                    throw new IllegalArgumentException("IMP_EXPERIMENT_RUN_REUSED");
                return new Commit(row.getAs("membership_snapshot_id"), row.getAs("summary_snapshot_id"), latestSnapshot(experimentTable));
            }
        }
        for (String table : List.of("sequence_membership", "sequence_summary")) {
            String name = namespace + "." + table;
            if (spark.catalog().tableExists(name)
                    && spark.table(name).filter(col("experiment_run_id").equalTo(runId)).limit(1).count() > 0)
                throw new IllegalStateException("IMP_PARTIAL_COMMIT_RECOVERY");
        }
        Dataset<Row> memberships = frame(bundle.memberships(), "memberships.parquet");
        Dataset<Row> summaries = frame(bundle.summaries(), "sequence_summary.parquet");
        long membership = write(memberships, "sequence_membership", context, resultBundleSha256);
        long summary = write(summaries, "sequence_summary", context, resultBundleSha256);
        Dataset<Row> experiment = experimentFrame(bundle.success(), context, resultBundleSha256, membership, summary);
        long run = write(experiment, "experiment_run", context, resultBundleSha256);
        return new Commit(membership, summary, run);
    }

    @Override public Map<String, Long> verify(Commit commit, ResultBundleValidator.Context context) throws Exception {
        Map<String, Long> counts = new LinkedHashMap<>();
        assertLease.run();
        counts.put("sequence_membership", snapshot("sequence_membership", commit.membershipSnapshot(), context.runId()).count());
        counts.put("sequence_summary", snapshot("sequence_summary", commit.summarySnapshot(), context.runId()).count());
        counts.put("experiment_run", snapshot("experiment_run", commit.experimentSnapshot(), context.runId()).count());
        if (counts.get("sequence_membership") <= 0 || counts.get("sequence_summary") <= 0 || counts.get("experiment_run") != 1)
            throw new IllegalStateException("IMP_READBACK_COUNT_MISMATCH");
        return counts;
    }

    private Dataset<Row> snapshot(String table, long snapshotId, String runId) {
        String name = namespace + "." + table;
        if (!spark.catalog().tableExists(name)) throw new IllegalStateException("IMP_TABLE_MISSING");
        return spark.read().format("iceberg").option("snapshot-id", Long.toString(snapshotId)).load(name)
            .filter(col("experiment_run_id").equalTo(runId));
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
        frame.writeTo(name)
            .option("snapshot-property.ml.operation_id", context.runId())
            .option("snapshot-property.ml.result_bundle_sha256", resultBundleSha256)
            .option("snapshot-property.ml.expected_row_count", Long.toString(frame.count()))
            .append();
        Table iceberg = Spark3Util.loadIcebergTable(spark, name);
        iceberg.refresh();
        Snapshot snapshot = iceberg.currentSnapshot();
        if (snapshot == null || !context.runId().equals(snapshot.summary().get("ml.operation_id"))
                || !resultBundleSha256.equals(snapshot.summary().get("ml.result_bundle_sha256")))
            throw new IllegalStateException("IMP_SNAPSHOT_IDENTITY_MISMATCH");
        return snapshot.snapshotId();
    }

    private long latestSnapshot(String table) throws Exception {
        Table iceberg = Spark3Util.loadIcebergTable(spark, table);
        iceberg.refresh();
        Snapshot snapshot = iceberg.currentSnapshot();
        if (snapshot == null) throw new IllegalStateException("IMP_SNAPSHOT_MISSING");
        return snapshot.snapshotId();
    }

    private Dataset<Row> frame(List<JsonNode> rows, String artifact) {
        StructType schema = schema(artifact);
        List<Row> values = rows.stream().map(row -> RowFactory.create(schema.fields(), row)).toList();
        return spark.createDataFrame(values, schema);
    }

    private Dataset<Row> experimentFrame(JsonNode success, ResultBundleValidator.Context context,
                                         String fingerprint, long membershipSnapshot, long summarySnapshot) {
        StructType schema = new StructType(new StructField[] {
            new StructField("schema_version", DataTypes.StringType, false, Metadata.empty()),
            new StructField("experiment_run_id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("dataset_id", DataTypes.StringType, false, Metadata.empty()),
            new StructField("algorithm_name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("algorithm_version", DataTypes.StringType, false, Metadata.empty()),
            new StructField("model_config_version", DataTypes.StringType, false, Metadata.empty()),
            new StructField("package_version", DataTypes.StringType, false, Metadata.empty()),
            new StructField("code_version", DataTypes.StringType, false, Metadata.empty()),
            new StructField("result_artifact_uri", DataTypes.StringType, false, Metadata.empty()),
            new StructField("result_bundle_sha256", DataTypes.StringType, false, Metadata.empty()),
            new StructField("membership_row_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField("summary_row_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField("membership_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("summary_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("experiment_status", DataTypes.StringType, false, Metadata.empty())
        });
        Row row = org.apache.spark.sql.RowFactory.create(success.path("schema_version").asText(), context.runId(),
            context.datasetId(), context.algorithm(), success.path("algorithm_version").asText(), context.modelVersion(),
            success.path("package_version").asText(), success.path("code_version").asText(), success.path("artifact_uri").asText(),
            fingerprint, success.path("membership_row_count").asLong(), success.path("summary_row_count").asLong(),
            membershipSnapshot, summarySnapshot, "CANDIDATE");
        return spark.createDataFrame(List.of(row), schema);
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
            fields.add(new StructField(field.path("name").asText(), data, field.path("nullable").asBoolean(), Metadata.empty()));
        }
        return new StructType(fields.toArray(StructField[]::new));
    }

    private static final class RowFactory {
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
