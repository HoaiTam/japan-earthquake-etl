package ie212.earthquake.spark;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import ie212.earthquake.spark.orchestration.RealEtlAdapter;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.JmaArchiveInventory;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/** Fixed executable; business logs go to private files, stdout is one bounded receipt. */
public final class RealEtlPhaseJob {
    private RealEtlPhaseJob() { }
    public static void main(String[] args) {
        try { run(args); }
        catch(Exception error) {
            String reason=error.getMessage();
            if(reason==null || !reason.matches("[A-Z][A-Z0-9_]{0,100}(:[A-Za-z0-9_:.-]{1,128})?")) reason="REAL_ETL_GATE_FAILED";
            System.err.println("event=real_etl_phase_failed reason="+reason); System.exit(2);
        }
    }
    private static void run(String[] args) throws Exception {
        require(args.length==1,"EXACT_REAL_REQUEST_REQUIRED"); var path=Path.of(args[0]);
        require(Files.size(path)<=131072,"BOUNDED_REAL_REQUEST_REQUIRED"); var input=JSON.readTree(Files.readAllBytes(path));
        require(input.size()==4 && input.has("phase") && input.has("run_context") && input.has("upstream")
                && RealEtlAdapter.PROTOCOL.equals(input.path("contract_version").asText()),"REAL_PROTOCOL_REQUIRED");
        JsonNode context=input.path("run_context"); RealEtlAdapter.validateContext(context);
        Runnable lease=()->{try {
            var owner=JSON.readTree(Files.readAllBytes(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json")));
            require(text(context,"dag_id").equals(text(owner,"dag_id")) && text(context,"run_id").equals(text(owner,"run_id")),"EXACT_REAL_LEASE_REQUIRED");
        } catch(Exception e) { throw new IllegalStateException("EXACT_REAL_LEASE_REQUIRED"); }};
        lease.run(); var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("orc-07-real-etl")
                .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","1").getOrCreate();
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var silver=MinioSilverObjectStore.fromEnvironment()) {
            spark.sparkContext().setLogLevel("ERROR");
            String endpoint="http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT");
            var publisher=new GoldPublisher(new TrinoSqlClient(URI.create(endpoint)),silver,lease);
            var inventory=JmaArchiveInventory.read(Path.of(System.getenv().getOrDefault("JMA_INVENTORY_PATH","/opt/pipeline/config/jma/hypocenter_archives_v1.csv")));
            var adapter=new RealEtlAdapter(spark,bronze,silver,inventory,
                    new GoldIcebergWriter(spark,context.path("resolver_request").path("namespace").asText(),silver,lease),publisher,lease);
            var receipt=adapter.execute(text(input,"phase"),context,input.path("upstream"),path.toAbsolutePath().getParent());
            byte[] out=JSON.writeValueAsBytes(receipt); require(out.length<=65536,"BOUNDED_REAL_RECEIPT_REQUIRED");
            System.out.println(new String(out,java.nio.charset.StandardCharsets.UTF_8));
        } finally { spark.stop(); }
    }
}
