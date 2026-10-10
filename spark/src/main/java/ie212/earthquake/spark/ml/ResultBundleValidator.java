package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

/** Reader/import implementations must supply verified Parquet types and candidate snapshot lineage. */
public interface ResultBundleValidator {
    record Candidate(String mainshockId, String eventId, String role) {}
    record Context(String datasetId, String datasetStatus, String runId, String algorithm,
                   String modelVersion, List<Candidate> candidates, String previousBundleSha256) {
        public Context { candidates = List.copyOf(candidates); }
    }
    record DecodedBundle(JsonNode success, Map<String, byte[]> artifacts,
                         List<JsonNode> memberships, List<JsonNode> summaries,
                         boolean exactParquetSchemaVerified) {
        public DecodedBundle {
            artifacts = Map.copyOf(artifacts); memberships = List.copyOf(memberships); summaries = List.copyOf(summaries);
        }
    }
    record Receipt(String bundleSha256, String nextStatus) {}
    Receipt validate(DecodedBundle bundle, Context context);
}
