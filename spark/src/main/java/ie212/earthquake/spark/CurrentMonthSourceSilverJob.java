package ie212.earthquake.spark;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.silver.*;
import ie212.earthquake.spark.usgs.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Clock;
import java.util.*;
import org.apache.spark.sql.SparkSession;

/** Explicit component runner, not an Airflow scheduler or publication entrypoint. */
public final class CurrentMonthSourceSilverJob {
    private CurrentMonthSourceSilverJob() { }
    public static void main(String[] args) {
        try { execute(args); }
        catch(Exception error) {
            String reason=error.getMessage();
            if(reason==null || !reason.matches("[A-Z][A-Z0-9_:-]{0,180}")) reason="ASOF_COMPONENT_FAILED";
            System.err.println("event=current_month_component_failed reason="+reason); System.exit(2);
        }
    }
    static void execute(String[] args) throws Exception {
        boolean local=args.length==3 && "--local".equals(args[0]);
        boolean minio=args.length==2 && "--minio".equals(args[0]);
        require(local || minio,"ASOF_EXACT_COMPONENT_ARGUMENTS_REQUIRED");
        Path input=Path.of(args[1]); require(!Files.isSymbolicLink(input) && Files.size(input)<=32768,"ASOF_BOUNDED_REQUEST_REQUIRED");
        var request=JSON.readTree(Files.readAllBytes(input)); var clock=Clock.systemUTC(); CurrentMonthContract.validate(request,clock);
        var config=UsgsRequestConfig.fromEnvironment();
        if(local) {
            Path root=Path.of(args[2]).toAbsolutePath().normalize(), staging=Path.of("staging").toAbsolutePath().normalize();
            require(root.startsWith(staging) && !root.equals(staging),"ASOF_SCOPED_STAGING_ROOT_REQUIRED");
            for(Path p=root;p!=null;p=p.getParent()) require(!Files.isSymbolicLink(p),"ASOF_SYMLINK_ROOT_FORBIDDEN");
            Files.createDirectories(root);
            Path owner=root.resolve(".component-version");
            if(!Files.exists(owner)) {
                try(var children=Files.list(root)) { require(children.findAny().isEmpty(),"ASOF_UNOWNED_LOCAL_ROOT"); }
                try { Files.writeString(owner,CurrentMonthContract.COMPONENT,StandardOpenOption.CREATE_NEW); }
                catch(FileAlreadyExistsException ignored) { }
            }
            require(!Files.isSymbolicLink(owner) && Files.readString(owner).equals(CurrentMonthContract.COMPONENT),"ASOF_LOCAL_OWNER_CHANGED");
            for(String name:List.of("bronze","silver","component.lock")) require(!Files.isSymbolicLink(root.resolve(name)),"ASOF_SYMLINK_ROOT_FORBIDDEN");
            try(var paths=Files.walk(root)) {
                var iterator=paths.iterator(); int count=0;
                while(iterator.hasNext()) {
                    require(++count<=100000,"ASOF_LOCAL_PATH_BUDGET");
                    require(!Files.isSymbolicLink(iterator.next()),"ASOF_SYMLINK_ROOT_FORBIDDEN");
                }
            }
            // Fail for concurrent local component runs; no data/lock cleanup or foreign takeover.
            try(var channel=FileChannel.open(root.resolve("component.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
                var lock=channel.tryLock()) {
                require(lock!=null,"ASOF_LOCAL_COMPONENT_BUSY");
                run(request,config,new FileBronzeObjectStore(root.resolve("bronze")),new FileSilverObjectStore(root.resolve("silver")),clock,()->{});
            }
        } else {
            Runnable guard=()->{
                try {
                    var owner=JSON.readTree(Files.readAllBytes(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json")));
                    var context=request.path("run_context");
                    require(text(context,"dag_id").equals(text(owner,"dag_id")) && text(context,"run_id").equals(text(owner,"run_id")),"ASOF_EXACT_SOURCE_LEASE_REQUIRED");
                } catch(Exception error) { throw new IllegalStateException("ASOF_EXACT_SOURCE_LEASE_REQUIRED"); }
            };
            guard.run();
            try(var bronze=MinioBronzeObjectStore.fromEnvironment(); var silver=MinioSilverObjectStore.fromEnvironment()) {
                run(request,config,bronze,silver,clock,guard);
            }
        }
    }
    private static void run(com.fasterxml.jackson.databind.JsonNode request,UsgsRequestConfig config,BronzeObjectStore bronze,
            SilverObjectStore silver,Clock clock,Runnable guard) throws Exception {
        var capture=new CurrentMonthCapture(bronze,config,new UsgsHttpClient(config)::fetch,clock,guard);
        capture.capture(request);
        var spark=SparkSession.builder().master("local[1]").appName("current-month-source-silver")
                .config("spark.ui.enabled","false").config("spark.sql.session.timeZone","UTC")
                .config("spark.sql.shuffle.partitions","1").getOrCreate();
        try {
            spark.sparkContext().setLogLevel("ERROR");
            var receipt=new CurrentMonthSilverResolver(silver,capture,clock,guard).resolve(spark,request);
            System.out.println(JSON.writeValueAsString(receipt));
        } finally { spark.stop(); }
    }
}
