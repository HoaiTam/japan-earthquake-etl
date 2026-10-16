package ie212.earthquake.spark.ml;

import ie212.earthquake.spark.gold.GoldPublisher;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import static ie212.earthquake.spark.gold.GoldVerificationPlan.quote;

/** Independent Trino gate for exact ML result snapshots. */
public final class TrinoMlVerifier {
    private final GoldPublisher.Sql sql;
    private final String namespace;
    private final Runnable assertLease;

    public TrinoMlVerifier(GoldPublisher.Sql sql, String namespace, Runnable assertLease) {
        if (namespace == null || !namespace.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*")) throw new IllegalArgumentException("SAFE_ML_NAMESPACE_REQUIRED");
        this.sql = sql; this.namespace = namespace; this.assertLease = assertLease;
    }

    public Map<String, Long> verify(MlIcebergWriter.ResultCommit commit, ResultBundleValidator.Context context,
                                    String fingerprint) throws Exception {
        assertLease.run();
        long membership = exactCount("sequence_membership", commit.membershipSnapshot(), context, fingerprint);
        long summary = exactCount("sequence_summary", commit.summarySnapshot(), context, fingerprint);
        if (membership <= 0 || summary <= 0) throw new IOException("IMP_TRINO_QUALITY_BLOCKER");
        long duplicates = scalar("SELECT count(*) FROM (SELECT mainshock_event_id,candidate_event_id,count(*) c FROM "
            + ref("sequence_membership", commit.membershipSnapshot()) + " WHERE experiment_run_id=" + quote(context.runId())
            + " GROUP BY mainshock_event_id,candidate_event_id HAVING count(*) > 1)");
        if (duplicates != 0) throw new IOException("IMP_TRINO_DUPLICATE_GRAIN");
        long lineage = scalar("SELECT count(*) FROM " + ref("sequence_summary", commit.summarySnapshot())
            + " WHERE experiment_run_id=" + quote(context.runId()) + " AND dataset_id=" + quote(context.datasetId()));
        if (lineage != summary) throw new IOException("IMP_TRINO_LINEAGE_MISMATCH");
        Map<String, Long> result = new LinkedHashMap<>(); result.put("sequence_membership", membership); result.put("sequence_summary", summary);
        return result;
    }

    private long exactCount(String table, long snapshot, ResultBundleValidator.Context context, String fingerprint) throws Exception {
        if (snapshot <= 0) throw new IOException("IMP_TRINO_SNAPSHOT_REQUIRED");
        long count = scalar("SELECT count(*) FROM " + ref(table, snapshot) + " WHERE experiment_run_id=" + quote(context.runId()));
        long identity = scalar("SELECT count(*) FROM " + namespace + ".\"" + table + "$snapshots\" WHERE snapshot_id=" + snapshot
            + " AND summary['ml.operation_id']=" + quote(context.runId()) + " AND summary['ml.result_bundle_sha256']=" + quote(fingerprint));
        if (identity != 1) throw new IOException("IMP_TRINO_SNAPSHOT_IDENTITY_MISMATCH");
        return count;
    }

    private String ref(String table, long snapshot) { return namespace + "." + table + " FOR VERSION AS OF " + snapshot; }

    private long scalar(String query) throws Exception {
        GoldPublisher.Result result = sql.execute(query);
        if (result.rows().size() != 1 || result.rows().get(0).size() != 1 || !result.rows().get(0).get(0).canConvertToLong())
            throw new IOException("IMP_TRINO_RESPONSE_INVALID");
        return result.rows().get(0).get(0).asLong();
    }
}
