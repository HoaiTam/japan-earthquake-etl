package ie212.earthquake.spark;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;

/** Explicit MinIO/Iceberg/Trino boundary. Requires an already held shared source lease. */
public final class CurrentMonthGoldJob {
    private CurrentMonthGoldJob() { }
    public static void main(String[] args) {
        try { execute(args); }
        catch(Exception error) {
            String reason=error.getMessage();
            if(reason==null || !reason.matches("[A-Z][A-Z0-9_:-]{0,180}")) reason="ASOF_GOLD_FAILED";
            System.err.println("event=current_month_gold_failed reason="+reason);System.exit(2);
        }
    }
    static void execute(String[] args) throws Exception {
        require(args.length==1,"ASOF_EXACT_GOLD_ARGUMENT_REQUIRED");
        Path input=Path.of(args[0]);require(!Files.isSymbolicLink(input) && Files.size(input)<=32768,"ASOF_BOUNDED_REQUEST_REQUIRED");
        var request=JSON.readTree(Files.readAllBytes(input));CurrentMonthGold.validate(request);
        String catalog=System.getenv("ICEBERG_CATALOG_NAME");
        require((catalog+"."+OperationalGoldPolicy.SCHEMA).equals(text(request,"namespace")),"ASOF_CONFIGURED_CATALOG_REQUIRED");
        String host=System.getenv("TRINO_HOST"),port=System.getenv("TRINO_INTERNAL_PORT");
        require(host!=null && host.matches("[a-z0-9][a-z0-9.-]*") && port!=null && port.matches("[0-9]{1,5}"),"ASOF_TRINO_CONFIG_REQUIRED");
        var clock=Clock.systemUTC();
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var store=MinioSilverObjectStore.fromEnvironment()) {
            // Obtain context only from pinned persisted Silver, not caller-supplied dag/run ownership.
            var isolated=new CurrentMonthSilverResolver.OperationalStore(store);
            byte[] registry=isolated.read(CurrentMonthSilverResolver.root(text(request,"operation_id"))+"/registry.json");
            require(sha(registry).equals(text(request,"registry_sha256")),"ASOF_REGISTRY_PIN_CHANGED");
            var run=JSON.readTree(registry).path("request").path("run_context");
            Runnable lease=()->{
                try {
                    var owner=JSON.readTree(Files.readAllBytes(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json")));
                    require(text(run,"dag_id").equals(text(owner,"dag_id")) && text(run,"run_id").equals(text(owner,"run_id")),"ASOF_EXACT_SOURCE_LEASE_REQUIRED");
                } catch(Exception error) { throw new IllegalStateException("ASOF_EXACT_SOURCE_LEASE_REQUIRED"); }
            };
            lease.run();
            // Gold must never refetch a selected capture; an accidental network call fails immediately.
            var capture=new CurrentMonthCapture(bronze,UsgsRequestConfig.fromEnvironment(),q->{throw new IllegalStateException("ASOF_GOLD_SOURCE_REFETCH_FORBIDDEN");},clock,lease);
            var resolver=new CurrentMonthSilverResolver(store,capture,clock,lease);
            resolver.verify(text(request,"operation_id"),text(request,"registry_sha256"));
            var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("current-month-gold-publication")
                .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","1").getOrCreate();
            Path work=Files.createTempDirectory("current-month-gold-");
            try {
                spark.sparkContext().setLogLevel("ERROR");
                var result=new CurrentMonthGold(store,resolver,new TrinoSqlClient(URI.create("http://"+host+":"+port)),lease).execute(spark,request,work);
                System.out.println(JSON.writeValueAsString(result));
            } finally { spark.stop(); }
        }
    }
}
