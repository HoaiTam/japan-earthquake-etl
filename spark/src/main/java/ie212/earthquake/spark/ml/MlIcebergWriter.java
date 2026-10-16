package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;

/** Commit boundary for validated ML results. Implementations must keep rejected runs out of serving. */
public interface MlIcebergWriter {
    record Commit(long membershipSnapshot, long summarySnapshot, long experimentSnapshot) { }
    Commit commit(ResultBundleValidator.DecodedBundle bundle, ResultBundleValidator.Context context,
                  String resultBundleSha256) throws Exception;
    Map<String, Long> verify(Commit commit, ResultBundleValidator.Context context) throws Exception;
}
