package ie212.earthquake.spark;

import com.fasterxml.jackson.databind.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.nio.file.*;
import java.net.URI;
import java.util.*;

/** One exact Gold commit, shared run lease, independent Trino verification/publication. */
public final class GoldVerificationJob {
    private static final ObjectMapper JSON=new ObjectMapper();
    private GoldVerificationJob() { }
    public static void main(String[] args) throws Exception {
        if(args.length!=1 || Files.size(Path.of(args[0]))>65536)throw new IllegalArgumentException("EXACT_VERIFICATION_REQUEST_REQUIRED");
        JsonNode request=JSON.readTree(Files.readAllBytes(Path.of(args[0])));
        String run=request.path("run_id").asText(),dag=request.path("dag_id").asText();
        if(!run.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,127}") || dag.isBlank())throw new IllegalArgumentException("RUN_CONTEXT_REQUIRED");
        Path guard=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{var owner=JSON.readTree(Files.readAllBytes(guard));
            if(!run.equals(owner.path("run_id").asText()) || !dag.equals(owner.path("dag_id").asText()))throw new IllegalStateException();
        }catch(Exception e){throw new IllegalStateException("GOLD_VERIFICATION_LEASE_NOT_HELD");}};
        String host=System.getenv("TRINO_HOST"),port=System.getenv("TRINO_INTERNAL_PORT");
        if(host==null || !host.matches("[a-z0-9][a-z0-9.-]*") || port==null || !port.matches("[0-9]{1,5}"))
            throw new IllegalArgumentException("TRINO_CONFIG_REQUIRED");
        try(var store=MinioSilverObjectStore.fromEnvironment()) {
            var publisher=new GoldPublisher(new TrinoSqlClient(URI.create("http://"+host+":"+port)),store,lease);
            var first=publisher.publish(request.path("gold_run_id").asText(),request.path("expected_namespace").asText(),request.path("expected_identity_sha256").asText());
            var second=publisher.publish(request.path("gold_run_id").asText(),request.path("expected_namespace").asText(),request.path("expected_identity_sha256").asText());
            if(!first.equals(second))throw new IllegalStateException("PUBLICATION_RERUN_CHANGED");
            var report=first.deepCopy();((com.fasterxml.jackson.databind.node.ObjectNode)report).put("rerun_unchanged",true);
            Files.write(Path.of(args[0]).toAbsolutePath().getParent().resolve("gld-04-report.json"),JSON.writeValueAsBytes(report));
            System.out.println(JSON.writeValueAsString(report));
        }
    }
}
