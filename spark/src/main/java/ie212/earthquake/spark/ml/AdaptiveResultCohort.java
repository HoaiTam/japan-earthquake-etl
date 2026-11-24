package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;

/** Identity shared by all imports of one adaptive result cohort in a namespace. */
record AdaptiveResultCohort(String reservationKey, String sha256, List<Shard> shards) {
    private static final ObjectMapper JSON = new ObjectMapper();

    record Shard(String id, String runId, String markerSha256) { }

    AdaptiveResultCohort {
        shards = List.copyOf(shards);
    }

    static AdaptiveResultCohort from(ResultBundleValidator.Context context, List<Shard> sortedShards) throws IOException {
        var scope = context.collectionScope();
        ArrayNode identity = JSON.createArrayNode().add("mli03-adaptive-cohort-v1").add(context.datasetId())
            .add(scope.version()).add(scope.collectionId()).add(context.algorithm()).add(context.modelVersion());
        String key = ResultBundleContract.sha256(JSON.writeValueAsBytes(identity));
        ArrayNode pins = identity.addArray();
        for (Shard shard : sortedShards)
            pins.addArray().add(shard.id()).add(shard.runId()).add(shard.markerSha256());
        return new AdaptiveResultCohort(key, ResultBundleContract.sha256(JSON.writeValueAsBytes(identity)), sortedShards);
    }

    ObjectNode toJson() {
        ObjectNode node = JSON.createObjectNode().put("sha256", sha256);
        ArrayNode pins = node.putArray("shards");
        for (Shard shard : shards)
            pins.addObject().put("shard_id", shard.id()).put("experiment_run_id", shard.runId())
                .put("success_marker_sha256", shard.markerSha256());
        return node;
    }

    static void write(ObjectNode node, ResultBundleValidator.Context context) {
        if (context.adaptiveCohort() != null) node.set("adaptive_cohort", context.adaptiveCohort().toJson());
    }

    static boolean matches(JsonNode node, ResultBundleValidator.Context context) {
        var cohort = context.adaptiveCohort();
        return cohort == null ? !node.has("adaptive_cohort") : cohort.toJson().equals(node.get("adaptive_cohort"));
    }
}
