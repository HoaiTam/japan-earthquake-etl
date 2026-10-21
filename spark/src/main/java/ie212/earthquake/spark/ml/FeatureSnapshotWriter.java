package ie212.earthquake.spark.ml;

import static org.apache.spark.sql.functions.*;
import java.util.*;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.*;

/** Dataset-scoped immutable snapshots; retries reconcile snapshot metadata, never reappend. */
final class FeatureSnapshotWriter {
    private FeatureSnapshotWriter() { }
    static long commit(Dataset<Row> rows, String table, String datasetId, String identity, Runnable lease) throws Exception {
        FeatureExportBundle.require(table.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*\\.(?:mainshock_candidate_snapshot|sequence_candidate_snapshot)"), "DS_EXPORT_SAFE_TABLE_REQUIRED");
        FeatureExportBundle.require(rows.limit(1).count() == 1 && rows.filter(not(col("dataset_id").eqNullSafe(datasetId))).limit(1).count() == 0,
            "DS_EXPORT_SNAPSHOT_DATASET_CONFLICT");
        var spark = rows.sparkSession(); lease.run();
        if (!spark.catalog().tableExists(table)) {
            rows.limit(0).writeTo(table).using("iceberg").partitionedBy(col("dataset_id"),
                scala.collection.JavaConverters.asScalaBufferConverter(Collections.<Column>emptyList()).asScala().toSeq()).create();
        }
        var iceberg = Spark3Util.loadIcebergTable(spark, table); iceberg.refresh();
        var found = new ArrayList<Long>();
        for (var snapshot : iceberg.snapshots()) if (datasetId.equals(snapshot.summary().get("ml.dataset_id"))) {
            FeatureExportBundle.require(identity.equals(snapshot.summary().get("ml.export_identity_sha256")), "DS_EXPORT_SNAPSHOT_IDENTITY_CONFLICT");
            found.add(snapshot.snapshotId());
        }
        FeatureExportBundle.require(found.size() <= 1, "DS_EXPORT_DUPLICATE_SNAPSHOT");
        if (found.isEmpty()) {
            FeatureExportBundle.require(spark.table(table).filter(col("dataset_id").equalTo(datasetId)).limit(1).count() == 0, "DS_EXPORT_EXISTING_DATASET_ROWS");
            lease.run(); rows.writeTo(table).option("snapshot-property.ml.dataset_id", datasetId)
                .option("snapshot-property.ml.export_identity_sha256", identity).append();
            iceberg.refresh();
            for (var snapshot : iceberg.snapshots()) if (datasetId.equals(snapshot.summary().get("ml.dataset_id"))
                    && identity.equals(snapshot.summary().get("ml.export_identity_sha256"))) found.add(snapshot.snapshotId());
        }
        FeatureExportBundle.require(found.size() == 1, "DS_EXPORT_COMMIT_OUTCOME_UNKNOWN");
        var pinned = read(spark, table, found.get(0), datasetId);
        FeatureExportBundle.require(pinned.count() == rows.count()
            && MainshockWindowJob.fingerprint(pinned).equals(MainshockWindowJob.fingerprint(rows)), "DS_EXPORT_SNAPSHOT_READBACK_FAILED");
        var current = spark.table(table).filter(col("dataset_id").equalTo(datasetId));
        FeatureExportBundle.require(MainshockWindowJob.fingerprint(current).equals(MainshockWindowJob.fingerprint(pinned)), "DS_EXPORT_CURRENT_DATASET_CHANGED");
        return found.get(0);
    }
    static Dataset<Row> read(SparkSession spark, String table, long pin, String id) {
        spark.catalog().refreshTable(table);
        // Trino can write DELTA_LENGTH_BYTE_ARRAY metadata files that the
        // Iceberg 1.10/Spark 3.5 vectorized reader cannot decode. Scope the
        // compatible row reader to this exact pinned scan, without rewriting files.
        return spark.read().format("iceberg").option("snapshot-id", Long.toString(pin))
            .option(org.apache.iceberg.spark.SparkReadOptions.VECTORIZATION_ENABLED, "false")
            .load(table).filter(col("dataset_id").equalTo(id));
    }
}
