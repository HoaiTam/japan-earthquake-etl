package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.orchestration.*;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.jma.*;
import ie212.earthquake.spark.silver.*;
import java.net.*;
import java.net.http.HttpHeaders;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.apache.spark.sql.*;

/** Opt-in QA data plane. Fixtures stay explicitly synthetic; storage and publication are real. */
public final class PipelineQaSupport implements AutoCloseable {
    public final SparkSession spark;
    public final MinioBronzeObjectStore bronze;
    public final MinioSilverObjectStore silver;
    public final List<JmaArchiveEntry> inventory;
    public final GoldIcebergWriter writer;
    public final RealEtlAdapter adapter;
    public final String namespace;
    public final Path root;
    public final Runnable lease;
    public final Instant now=Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
    public final Path fixtures=Path.of("/opt/pipeline/qa-fixtures");
    public PipelineQaSupport(JsonNode request,Path root) throws Exception {
        this.root=root; namespace=text(request,"namespace");
        require(namespace.matches("[a-z][a-z0-9_]*\\.qa0[23]_[a-f0-9]{32}"),"ISOLATED_PIPELINE_QA_REQUIRED");
        var owner=request.path("lease_owner");
        lease=()->{try {
            var live=JSON.readTree(Files.readAllBytes(Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json")));
            require(live.equals(owner),"EXACT_PIPELINE_QA_LEASE_REQUIRED");
        } catch(Exception e) {throw new IllegalStateException("EXACT_PIPELINE_QA_LEASE_REQUIRED");}};
        lease.run();
        spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("pipeline-qa-acceptance")
            .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions","1")
            .config("spark.sql.adaptive.enabled","false").config("spark.sql.codegen.wholeStage","false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        bronze=MinioBronzeObjectStore.fromEnvironment(); silver=MinioSilverObjectStore.fromEnvironment();
        inventory=JmaArchiveInventory.read(Path.of("/opt/pipeline/config/jma/hypocenter_archives_v1.csv"));
        writer=new GoldIcebergWriter(spark,namespace,silver,lease);
        var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
        adapter=new RealEtlAdapter(spark,bronze,silver,inventory,writer,new GoldPublisher(sql,silver,lease),lease);
    }
    public ObjectNode usgs(String run,String from,String to,ObjectNode payload) throws Exception {
        return usgsBytes(run,from,to,JSON.writeValueAsBytes(payload));
    }
    public ObjectNode usgsBytes(String run,String from,String to,byte[] raw) throws Exception {
        lease.run(); Instant a=Instant.parse(from), b=Instant.parse(to);
        String url="https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventtype=earthquake&starttime="
            +URLEncoder.encode(from,java.nio.charset.StandardCharsets.UTF_8)+"&endtime="
            +URLEncoder.encode(b.minusMillis(1).toString(),java.nio.charset.StandardCharsets.UTF_8)
            +"&minlatitude=20.0&maxlatitude=50.0&minlongitude=120.0&maxlongitude=155.0&orderby=time-asc&limit=20000&offset=1";
        var response=new UsgsHttpResponse("synthetic-pipeline-qa",new UsgsRequest(URI.create(url),a,b,20000,1),200,
            HttpHeaders.of(Map.of("content-type",List.of("application/geo+json")),(x,y)->true),raw,1,Duration.ZERO);
        var written=new UsgsBronzeWriter(bronze).write(new UsgsBronzeWriteRequest(response,run,1,
            LocalDate.ofInstant(now,ZoneOffset.UTC),now,true,run,60000));
        require(written.ready(),"QA_USGS_BRONZE_REJECTED");
        var pin=JSON.createObjectNode(); pin.put("source_system","USGS"); pin.put("manifest_uri",bronze.uriForKey(written.manifestKey()));
        pin.put("manifest_sha256",sha(bronze.read(written.manifestKey()))); pin.put("sha256",written.sha256());
        pin.put("window_start_utc",from); pin.put("window_end_utc",to); return pin;
    }
    public ObjectNode payload() throws Exception {return (ObjectNode)JSON.readTree(Files.readAllBytes(fixtures.resolve("usgs/success.geojson")));}
    public ObjectNode jma(String run,JmaArchiveEntry entry,byte[] raw,String release) throws Exception {
        lease.run();
        var written=new JmaBronzeWriter(bronze).write(new JmaBronzeWriteRequest(entry,raw,sha(raw),raw.length,release,
            new JmaHttpMetadata(200,"application/zip",(long)raw.length,null,null,entry.sourceUrl()),run,1,
            LocalDate.ofInstant(now,ZoneOffset.UTC),now,true,run,60000,null));
        require(written.ready(),"QA_JMA_BRONZE_REJECTED");
        var pin=JSON.createObjectNode(); pin.put("source_system","JMA_BULLETIN"); pin.put("manifest_uri",bronze.uriForKey(written.manifestKey()));
        pin.put("manifest_sha256",sha(bronze.read(written.manifestKey()))); pin.put("sha256",sha(raw));
        pin.put("year",entry.year()); pin.put("segment",entry.segment()); pin.put("catalog_release",release); return pin;
    }
    public JmaArchiveEntry entry(int year,String segment) {return inventory.stream().filter(e->e.year()==year && e.segment().equals(segment)).findFirst().orElseThrow();}
    public byte[] archive(JmaArchiveEntry entry,List<String> records) throws Exception {
        var out=new java.io.ByteArrayOutputStream();
        try(var zip=new java.util.zip.ZipOutputStream(out)) {
            var member=new java.util.zip.ZipEntry(entry.memberName()); member.setTime(0); zip.putNextEntry(member);
            for(String row:records) zip.write((row+"\n").getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
            zip.closeEntry();
        }
        return out.toByteArray();
    }
    public String jmaRecord(String jstDigits) throws Exception {
        String row=Files.readAllLines(fixtures.resolve("jma/fixed-width/success.hyp")).get(0);
        return row.substring(0,1)+jstDigits+row.substring(17);
    }
    public ObjectNode context(String run,List<String> target,List<JsonNode> pins,JsonNode parent) throws Exception {
        String op=RealEtlAdapter.operation("pipeline_qa",run);
        var r=JSON.createObjectNode(); r.put("resolver_version",VERSION); r.put("run_id",op); r.put("dag_id","pipeline_qa");
        r.put("namespace",namespace); r.put("config_version","qa-runtime-v1"); r.put("code_version","qa-runtime-v1");
        r.put("processed_at_utc",now.toString()); r.put("source_cutoff_utc",now.toString());
        r.set("target_months",JSON.valueToTree(target)); r.set("baseline_snapshots",JSON.valueToTree(writer.baselines()));
        r.set("bronze_inputs",JSON.valueToTree(pins));
        if(parent==null) r.putNull("baseline"); else {
            String old=text(parent,"operation_id"); byte[] registry=silver.read(FullMonthStateResolver.root(old)+"/registry.json");
            String key="gold-commits/"+old+"/commit.json";
            var baseline=r.putObject("baseline"); baseline.put("run_id",old); baseline.put("registry_sha256",sha(registry));
            baseline.put("commit_key",key); baseline.put("commit_sha256",sha(silver.read(key)));
        }
        var c=JSON.createObjectNode(); c.put("contract_version",RealEtlAdapter.PROTOCOL); c.put("mode","real");
        c.put("dag_id","pipeline_qa"); c.put("run_id",run); c.put("operation_id",op); c.put("trigger","manual");
        c.put("window_start_utc",start(Collections.min(target)).toString()); c.put("window_end_utc",end(Collections.max(target)).toString());
        c.putNull("source_summary_sha256"); c.set("resolver_request",r); c.put("context_sha256",digest(c)); return c;
    }
    public ObjectNode execute(ObjectNode context) throws Exception {
        String op=text(context,"operation_id"); Path work=root.resolve(op); Files.createDirectories(work);
        var record=JSON.createObjectNode(); record.set("context",context); var receipts=record.putArray("receipts");
        ObjectNode prior=null;
        for(String phase:RealEtlAdapter.PHASES) {
            lease.run(); prior=adapter.execute(phase,context,prior,work); receipts.add(prior);
            save(work.resolve(phase+".json"),artifact(prior));
        }
        require(prior.path("published").asBoolean(),"QA_GOLD_NOT_PUBLISHED");
        record.put("operation_id",op); record.set("publication",artifact(prior));
        record.set("commit",JSON.readTree(silver.read("gold-commits/"+op+"/commit.json")));
        record.set("silver",JSON.readTree(silver.read("real-etl/"+op+"/silver.json")));
        record.set("readback",state()); save(work.resolve("acceptance.json"),record);
        return record;
    }
    public JsonNode artifact(JsonNode receipt) throws Exception {
        byte[] raw=silver.read(key(text(receipt,"artifact_uri"),silver::uriForKey));
        var value=JSON.readTree(raw); require(digest(value).equals(text(receipt,"artifact_sha256")),"QA_RECEIPT_ARTIFACT_CHANGED"); return value;
    }
    public ObjectNode state() throws Exception {
        var state=JSON.createObjectNode(); var snapshots=writer.baselines(); state.set("snapshots",JSON.valueToTree(snapshots));
        for(var e:snapshots.entrySet()) if(e.getValue()>0) {
            var rows=spark.read().format("iceberg").option("snapshot-id",Long.toString(e.getValue())).load(namespace+"."+e.getKey());
            state.put(e.getKey()+"_count",rows.count());
        }
        return state;
    }
    public Dataset<Row> events(long snapshot) {return spark.read().format("iceberg").option("snapshot-id",Long.toString(snapshot)).load(namespace+".event_current");}
    public List<SilverObservation> active(JsonNode record) throws Exception {
        String op=text(record,"operation_id"); var registry=JSON.readTree(silver.read(FullMonthStateResolver.root(op)+"/registry.json"));
        var reader=new FullMonthBronzeReader(bronze,inventory); var inputs=new ArrayList<FullMonthBronzeReader.Input>();
        for(var pin:registry.path("active_inputs")) inputs.add(reader.read(pin,now));
        return reader.parse(inputs,months(registry.path("complete_months")));
    }
    public void oldReadable(JsonNode commit) throws Exception {
        for(var name:GoldIcebergWriter.TABLES) {
            var pin=commit.path("tables").path(name);
            require(spark.read().format("iceberg").option("snapshot-id",pin.path("snapshot_id").asText()).load(namespace+"."+name).count()==pin.path("count").asLong(),"QA_OLD_SNAPSHOT_UNREADABLE");
        }
    }
    public static void save(Path path,JsonNode value) throws Exception {
        byte[] bytes=JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(value);
        if(Files.exists(path)) require(Arrays.equals(Files.readAllBytes(path),bytes),"QA_EVIDENCE_EXISTS_DIFFERENT");
        else Files.write(path,bytes,StandardOpenOption.CREATE_NEW);
    }
    @Override public void close() throws Exception {try{bronze.close();silver.close();}finally{spark.stop();}}
}
