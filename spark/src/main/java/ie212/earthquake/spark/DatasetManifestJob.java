package ie212.earthquake.spark;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.TrinoSqlClient;
import ie212.earthquake.spark.ml.DatasetManifestWriter;
import java.net.URI;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

/** SQL metadata job; request is exact and credentials remain in the runtime environment. */
public final class DatasetManifestJob {
    private DatasetManifestJob() { }
    public static void main(String[] args) throws Exception {
        var json = new ObjectMapper();
        if (args.length != 1 || Files.size(Path.of(args[0])) > 65536) throw new IllegalArgumentException("DS_EXACT_REQUEST_REQUIRED");
        var request = json.readTree(Files.readAllBytes(Path.of(args[0])));
        String run = request.path("run_id").asText(), dag = request.path("dag_id").asText();
        if (!run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}") || !dag.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}"))
            throw new IllegalArgumentException("DS_RUN_CONTEXT_REQUIRED");
        Path owner = Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")), "owner.json");
        Runnable lease = () -> { try {
            var current = json.readTree(Files.readAllBytes(owner));
            if (!run.equals(current.path("run_id").asText()) || !dag.equals(current.path("dag_id").asText())) throw new IllegalStateException();
        } catch (Exception e) { throw new IllegalStateException("DS_LEASE_NOT_HELD"); } };
        String host = System.getenv("TRINO_HOST"), port = System.getenv("TRINO_INTERNAL_PORT");
        if (host == null || !host.matches("[a-z0-9][a-z0-9.-]*") || port == null || !port.matches("[0-9]{1,5}"))
            throw new IllegalArgumentException("DS_TRINO_CONFIG_REQUIRED");
        var writer = new DatasetManifestWriter(new TrinoSqlClient(URI.create("http://" + host + ":" + port)), lease);
        ObjectNode report = json.createObjectNode(); report.put("task", "MLD-01"); report.put("engine", "TRINO");
        var manifests = report.putArray("manifests");
        if (!request.path("datasets").isArray() || request.path("datasets").size() != 2) throw new IllegalArgumentException("DS_TWO_SPLITS_REQUIRED");
        var splits = new HashSet<String>();
        for (var config : request.path("datasets")) {
            if (!splits.add(config.path("dataset_split").asText())) throw new IllegalArgumentException("DS_DUPLICATE_SPLIT");
            var first = writer.build(request.path("gold_namespace").asText(), request.path("publication_id").asText(),
                request.path("ml_namespace").asText(), config, run, Instant.now());
            var second = writer.build(request.path("gold_namespace").asText(), request.path("publication_id").asText(),
                request.path("ml_namespace").asText(), config, run, Instant.now());
            if (!first.equals(second)) throw new IllegalStateException("DS_RERUN_CHANGED");
            var entry = manifests.addObject(); entry.set("manifest", first); entry.put("pinned_gold_sql", DatasetManifestWriter.pinnedGoldSql(first));
        }
        report.put("rerun_unchanged", true);
        Files.write(Path.of(args[0]).toAbsolutePath().getParent().resolve("mld-01-report.json"), json.writeValueAsBytes(report));
        System.out.println("task=MLD-01 status=BUILDING splits=2 rerun_unchanged=true");
    }
}
