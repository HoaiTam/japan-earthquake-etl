package ie212.earthquake.spark;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.net.URI;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Airflow phase bridge. Java alone owns capture, parsing, transformations and SQL gates. */
public final class CurrentMonthDailyJob {
    private CurrentMonthDailyJob() { }
    public static void main(String[] args) {
        try { execute(args); }
        catch(Exception error) {
            String reason=error.getMessage();
            if(reason==null || !reason.matches("[A-Z][A-Z0-9_:-]{0,180}")) reason="ASOF_DAILY_PHASE_FAILED";
            System.err.println("event=current_month_daily_failed reason="+reason);System.exit(2);
        }
    }
    static void execute(String[] args) throws Exception {
        require(args.length==2 && Set.of("capture","silver","audit").contains(args[0]),"ASOF_EXACT_DAILY_PHASE_REQUIRED");
        Path path=Path.of(args[1]);require(!Files.isSymbolicLink(path) && Files.size(path)<=32768,"ASOF_BOUNDED_REQUEST_REQUIRED");
        var request=JSON.readTree(Files.readAllBytes(path));var clock=Clock.systemUTC();boolean audit="audit".equals(args[0]);
        if(audit) CurrentMonthGold.validate(request);else CurrentMonthContract.validate(request,clock);
        Runnable guard=audit ? ()->{} : ()->{
            try {
                var owner=JSON.readTree(Files.readAllBytes(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json")));
                var run=request.path("run_context");
                require(text(run,"dag_id").equals(text(owner,"dag_id")) && text(run,"run_id").equals(text(owner,"run_id")),"ASOF_EXACT_SOURCE_LEASE_REQUIRED");
            } catch(Exception error) {throw new IllegalStateException("ASOF_EXACT_SOURCE_LEASE_REQUIRED");}
        };
        guard.run();var config=UsgsRequestConfig.fromEnvironment();
        var http=new UsgsHttpClient(config);
        try(var bronze=MinioBronzeObjectStore.fromEnvironment();var silver=MinioSilverObjectStore.fromEnvironment()) {
            var capture=new CurrentMonthCapture(bronze,config,q->{
                require("capture".equals(args[0]),"ASOF_REFETCH_FORBIDDEN");return http.fetch(q);
            },clock,guard);
            var resolver=new CurrentMonthSilverResolver(silver,capture,clock,guard);
            com.fasterxml.jackson.databind.JsonNode result;
            if("capture".equals(args[0])) {
                var selection=capture.capture(request);var out=JSON.createObjectNode();String op=CurrentMonthContract.operation(request);
                out.put("contract_version","current-month-capture-receipt-v1");out.put("operation_id",op);
                out.put("selection_sha256",digest(selection));out.set("selection",selection);result=out;
            } else if(audit) {
                require((System.getenv("ICEBERG_CATALOG_NAME")+"."+OperationalGoldPolicy.SCHEMA).equals(text(request,"namespace")),"ASOF_CONFIGURED_CATALOG_REQUIRED");
                String host=System.getenv("TRINO_HOST"),port=System.getenv("TRINO_INTERNAL_PORT");
                require(host!=null && host.matches("[a-z0-9][a-z0-9.-]*") && port!=null && port.matches("[0-9]{1,5}"),"ASOF_TRINO_CONFIG_REQUIRED");
                result=CurrentMonthAudit.verify(request,silver,resolver,new TrinoSqlClient(URI.create("http://"+host+":"+port)));
            } else {
                capture.verify(request);
                var spark=SparkSession.builder().master("local[1]").appName("current-month-daily-silver").config("spark.ui.enabled","false")
                    .config("spark.sql.session.timeZone","UTC").config("spark.sql.shuffle.partitions","1").getOrCreate();
                try { spark.sparkContext().setLogLevel("ERROR");result=resolver.resolve(spark,request); }finally {spark.stop();}
            }
            System.out.println(JSON.writeValueAsString(result));
        }
    }
}
