package ie212.earthquake.spark.ml;

import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/** Exact-snapshot read interface used by Trino/runtime verification and serving checks. */
public final class MlResultSnapshotReader {
    private final SparkSession spark;
    private final String namespace;

    public MlResultSnapshotReader(SparkSession spark, String namespace) {
        if (namespace == null || !namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*"))
            throw new IllegalArgumentException("SAFE_ML_NAMESPACE_REQUIRED");
        this.spark = Objects.requireNonNull(spark);
        this.namespace = namespace;
        if (!"UTC".equals(spark.conf().get("spark.sql.session.timeZone")))
            throw new IllegalArgumentException("UTC_SESSION_REQUIRED");
    }

    public Dataset<Row> membership(long snapshotId, String experimentRunId) {
        return snapshot("sequence_membership", snapshotId, experimentRunId);
    }

    public Dataset<Row> summary(long snapshotId, String experimentRunId) {
        return snapshot("sequence_summary", snapshotId, experimentRunId);
    }

    public Dataset<Row> publication(long snapshotId, String experimentRunId) {
        Dataset<Row> rows = snapshot("experiment_run", snapshotId, experimentRunId)
            .filter(col("experiment_status").equalTo("CANDIDATE"));
        if (rows.limit(2).count() != 1) throw new IllegalStateException("IMP_PUBLICATION_NOT_CANDIDATE");
        return rows;
    }

    private Dataset<Row> snapshot(String table, long snapshotId, String experimentRunId) {
        if (snapshotId <= 0 || experimentRunId == null || experimentRunId.isBlank())
            throw new IllegalArgumentException("IMP_EXACT_SNAPSHOT_REQUIRED");
        String name = namespace + "." + table;
        if (!spark.catalog().tableExists(name)) throw new IllegalStateException("IMP_TABLE_MISSING");
        return spark.read().format("iceberg").option("snapshot-id", Long.toString(snapshotId)).load(name)
            .filter(col("experiment_run_id").equalTo(experimentRunId));
    }
}
