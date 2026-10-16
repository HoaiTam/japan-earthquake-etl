package ie212.earthquake.spark;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.gold.GoldIcebergWriter;
import ie212.earthquake.spark.jma.JmaArchiveInventory;
import ie212.earthquake.spark.orchestration.FullMonthStateResolver;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Exact request -> full-month Silver only. No download, Gold write, scheduler mutation or auto-build. */
public final class FullMonthStateJob {
    private FullMonthStateJob() { }
    public static void main(String[] args) {
        try { execute(args); }
        catch(Exception error) {
            // SDK/process exceptions may include endpoint/config; keep public stderr controlled.
            String reason=error.getMessage();
            if(reason==null || !reason.matches("[A-Z][A-Z0-9_:-]{0,180}")) reason="RESOLVER_GATE_FAILED";
            System.err.println("event=full_month_state_failed reason="+reason); System.exit(2);
        }
    }
    private static void execute(String[] args) throws Exception {
        boolean local=args.length==4 && "--local".equals(args[0]);
        require(local || args.length==1,"EXACT_REQUEST_REQUIRED"); Path requestPath=Path.of(args[local?1:0]);
        require(Files.size(requestPath)<=65536,"BOUNDED_REQUEST_REQUIRED");
        var request=JSON.readTree(Files.readAllBytes(requestPath)); validate(request);
        var inventory=JmaArchiveInventory.read(Path.of(System.getenv().getOrDefault("JMA_INVENTORY_FILE","config/jma/hypocenter_archives_v1.csv")));
        if(local) {
            require(snapshots(request.path("baseline_snapshots")).values().stream().allMatch(n->n==0),"LOCAL_MODE_REQUIRES_ZERO_GOLD_BASELINES");
            var spark=SparkSession.builder().master("local[1]").appName("orc-06-local-readback")
                    .config("spark.sql.session.timeZone","UTC").config("spark.ui.enabled","false")
                    .config("spark.sql.shuffle.partitions","1").getOrCreate();
            try {
                spark.sparkContext().setLogLevel("ERROR");
                var result=new FullMonthStateResolver(new FileBronzeObjectStore(Path.of(args[2])),new FileSilverObjectStore(Path.of(args[3])),
                        inventory,()->snapshots(request.path("baseline_snapshots")),()->{}).resolve(spark,request);
                System.out.println(JSON.writeValueAsString(result));
            } finally { spark.stop(); }
        } else {
            Runnable lease=()->{
                try {
                    var owner=JSON.readTree(Files.readAllBytes(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json")));
                    if(!request.path("dag_id").asText().equals(owner.path("dag_id").asText())
                            || !request.path("run_id").asText().equals(owner.path("run_id").asText())) throw new IllegalStateException();
                } catch(Exception error) { throw new IllegalStateException("EXACT_SOURCE_LEASE_REQUIRED"); }
            };
            lease.run(); var spark=GoldCommitJob.configured(System.getenv()).appName("orc-06-full-month-state").getOrCreate();
            try(var bronze=MinioBronzeObjectStore.fromEnvironment(); var silver=MinioSilverObjectStore.fromEnvironment()) {
                spark.sparkContext().setLogLevel("ERROR");
                var gold=new GoldIcebergWriter(spark,request.path("namespace").asText(),silver,lease);
                var result=new FullMonthStateResolver(bronze,silver,inventory,gold::baselines,lease).resolve(spark,request);
                System.out.println(JSON.writeValueAsString(result));
            } finally { spark.stop(); }
        }
    }
}
