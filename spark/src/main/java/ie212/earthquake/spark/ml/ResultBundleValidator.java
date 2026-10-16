package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;

/** Reader/import implementations must supply verified Parquet types and candidate snapshot lineage. */
public interface ResultBundleValidator {
    record Candidate(String mainshockId, String eventId, String role) {}
    /** Pins resolved from the trusted exported dataset registry, never from caller claims. */
    record DatasetPins(String manifestRef, String manifestSha256, String exportManifestSha256,
                       String registrySnapshotId, String candidateSnapshotId, String mainshockSnapshotId) {
        public DatasetPins {
            if (manifestRef == null || manifestRef.isBlank() || manifestSha256 == null || manifestSha256.isBlank()
                    || exportManifestSha256 == null || exportManifestSha256.isBlank()
                    || registrySnapshotId == null || registrySnapshotId.isBlank()
                    || candidateSnapshotId == null || candidateSnapshotId.isBlank()
                    || mainshockSnapshotId == null || mainshockSnapshotId.isBlank())
                throw new IllegalArgumentException("IMP_DATASET_PINS_REQUIRED");
        }
    }
    /** Immutable experiment metadata read from checked artifact bytes and run context. */
    record ExperimentMetadata(String configJson, String configSha256, String requirementsLockSha256,
                              String runtimeEnvironmentJson, String artifactUri, String importRunId,
                              String createdAtUtc, String resultReadyAtUtc, String notebookVersion,
                              Long randomSeed) {
        public ExperimentMetadata {
            for (String value : List.of(configJson, configSha256, requirementsLockSha256,
                    runtimeEnvironmentJson, artifactUri, importRunId, createdAtUtc, resultReadyAtUtc))
                if (value == null || value.isBlank()) throw new IllegalArgumentException("IMP_EXPERIMENT_METADATA_REQUIRED");
        }
    }
    record Context(String datasetId, String datasetStatus, String runId, String algorithm,
                   String modelVersion, List<Candidate> candidates, String previousBundleSha256,
                   DatasetPins datasetPins, ExperimentMetadata experimentMetadata) {
        public Context { candidates = List.copyOf(candidates); }
        /** Compatibility constructor for pure contract tests; production imports must resolve pins/metadata. */
        public Context(String datasetId, String datasetStatus, String runId, String algorithm,
                       String modelVersion, List<Candidate> candidates, String previousBundleSha256) {
            this(datasetId, datasetStatus, runId, algorithm, modelVersion, candidates, previousBundleSha256, null, null);
        }
        public Context withMetadata(ExperimentMetadata value) {
            return new Context(datasetId, datasetStatus, runId, algorithm, modelVersion, candidates,
                previousBundleSha256, datasetPins, value);
        }
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
