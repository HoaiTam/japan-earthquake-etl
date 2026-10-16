package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

/** Commit boundary for validated ML results. Implementations must keep rejected runs out of serving. */
public interface MlIcebergWriter {
    record Commit(long membershipSnapshot, long summarySnapshot, long experimentSnapshot) { }
    record ResultCommit(long membershipSnapshot, long summarySnapshot) { }
    record Reservation(String status, long membershipSnapshot, long summarySnapshot, long experimentSnapshot) { }

    /** Persist/check experiment identity before any result-table append. */
    default Reservation reserveExperiment(ResultBundleValidator.DecodedBundle bundle,
                                          ResultBundleValidator.Context context,
                                          String resultBundleSha256) throws Exception {
        return new Reservation("IMPORT_VALIDATING", 0L, 0L, 0L);
    }

    /** Mark a reserved experiment terminal after a quality blocker. */
    default void rejectExperiment(ResultBundleValidator.Context context, String resultBundleSha256,
                                  String reasonCode) throws Exception { }

    /** Commit only result tables.  Publication is a separate, post-verification step. */
    default ResultCommit commitResults(ResultBundleValidator.DecodedBundle bundle,
                                       ResultBundleValidator.Context context,
                                       String resultBundleSha256) throws Exception {
        Commit commit = commit(bundle, context, resultBundleSha256);
        return new ResultCommit(commit.membershipSnapshot(), commit.summarySnapshot());
    }

    /** Write the single experiment registry row after independent verification. */
    default long publishCandidate(ResultBundleValidator.DecodedBundle bundle,
                                  ResultBundleValidator.Context context,
                                  String resultBundleSha256, ResultCommit commit) throws Exception {
        return 0L;
    }

    /** Exact snapshot readback after publication. */
    default Map<String, Long> readback(ResultCommit commit, long experimentSnapshot,
                                       ResultBundleValidator.Context context) throws Exception {
        return verify(new Commit(commit.membershipSnapshot(), commit.summarySnapshot(), experimentSnapshot), context);
    }

    Commit commit(ResultBundleValidator.DecodedBundle bundle, ResultBundleValidator.Context context,
                  String resultBundleSha256) throws Exception;
    Map<String, Long> verify(Commit commit, ResultBundleValidator.Context context) throws Exception;
}
