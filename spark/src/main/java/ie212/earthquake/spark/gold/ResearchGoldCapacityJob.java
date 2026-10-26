package ie212.earthquake.spark.gold;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.safe;
import static ie212.earthquake.spark.usgs.ResearchCatalogState.save;
import static org.apache.spark.sql.functions.*;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.ml.LocalFeatureJob;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;

/** Read-only engineering stress, NEVER research rows or lake writes. Uses the exact writer fingerprint. */
public final class ResearchGoldCapacityJob {
    private ResearchGoldCapacityJob() { }
    static void validate(com.fasterxml.jackson.databind.JsonNode r) throws Exception {
        var keys=new HashSet<String>();r.fieldNames().forEachRemaining(keys::add);
        require(keys.equals(Set.of("version","namespace","table","snapshot_id","source_count","copies","shuffle_partitions","required_rows","runner_sha256","writer_class_sha256"))
                && "dat03-spark-gold-capacity-v1".equals(text(r,"version"))
                && text(r,"namespace").matches("iceberg\\.dat03_e2e_[a-f0-9]{32}")
                && Set.of("event_current","event_source_bridge").contains(text(r,"table")),"SPARK_CAPACITY_EXACT_REQUEST_REQUIRED");
        for(String key:List.of("snapshot_id","source_count","copies","shuffle_partitions","required_rows"))
            require(r.path(key).isIntegralNumber() && r.path(key).canConvertToLong(),"SPARK_CAPACITY_TYPED_LIMITS_REQUIRED");
        require(r.path("snapshot_id").asLong()>0 && r.path("source_count").asLong()>100000 && r.path("source_count").asLong()<=200000
                && r.path("copies").asLong()>=1 && r.path("copies").asLong()<=64
                && Set.of(1L,4L,16L).contains(r.path("shuffle_partitions").asLong())
                && r.path("required_rows").asLong()>0 && Math.multiplyExact(r.path("source_count").asLong(),r.path("copies").asLong())>=r.path("required_rows").asLong()
                && text(r,"runner_sha256").matches("[a-f0-9]{64}") && text(r,"writer_class_sha256").matches("[a-f0-9]{64}"),"SPARK_CAPACITY_WORKLOAD_REQUIRED");
    }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"SPARK_CAPACITY_REQUEST_REQUIRED");Path input=safe(Path.of(args[0]));
        require(input.toString().matches("/opt/pipeline/staging/dat03-spark-capacity/[a-f0-9]{32}/request.json")
                && Files.isRegularFile(input) && Files.size(input)<=65536,"SPARK_CAPACITY_EXACT_ROOT_REQUIRED");
        var r=JSON.readTree(Files.readAllBytes(input));validate(r);
        require(LocalFeatureJob.codeSha().equals(text(r,"runner_sha256")),"SPARK_CAPACITY_RUNNER_CHANGED");
        try(var code=GoldIcebergWriter.class.getResourceAsStream("GoldIcebergWriter.class")) {
            byte[] bytes=Objects.requireNonNull(code).readNBytes(1_048_577);
            require(bytes.length<=1_048_576 && sha(bytes).equals(text(r,"writer_class_sha256")),"SPARK_CAPACITY_WRITER_CHANGED");
        }
        var spark=GoldCommitJob.configured(System.getenv()).master("local[1]").appName("dat03-read-only-gold-capacity")
                .config("spark.ui.enabled","false").config("spark.sql.shuffle.partitions",r.path("shuffle_partitions").asText())
                .config("spark.sql.adaptive.enabled","false").config("spark.sql.codegen.wholeStage","false").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        try {
            String name=text(r,"table"),key=name.equals("event_current")?"canonical_event_id":"source_observation_id";
            var source=spark.read().format("iceberg").option("snapshot-id",r.path("snapshot_id").asText()).load(text(r,"namespace")+"."+name);
            long start=System.nanoTime();require(source.count()==r.path("source_count").asLong(),"SPARK_CAPACITY_SOURCE_COUNT_CHANGED");
            var copies=spark.range(r.path("copies").asLong()).withColumnRenamed("id","_capacity_copy_id");
            var rows=source.crossJoin(copies);
            for(String id:List.of("canonical_event_id","source_observation_id"))if(Arrays.asList(source.columns()).contains(id))
                rows=rows.withColumn(id,concat(col(id),lit("-stress-copy-"),col("_capacity_copy_id").cast("string")));
            rows=rows.drop("_capacity_copy_id");long expected=Math.multiplyExact(r.path("source_count").asLong(),r.path("copies").asLong());
            require(rows.count()==expected && rows.groupBy(key).count().filter(col("count").notEqual(1)).limit(1).count()==0,"SPARK_CAPACITY_UNIQUENESS_FAILED");
            long before=System.nanoTime();
            // Deliberate diagnostic reflection: do not fork/reimplement the algorithm or change production writer visibility.
            var method=GoldIcebergWriter.class.getDeclaredMethod("fingerprint",Dataset.class);method.setAccessible(true);
            String first=(String)method.invoke(null,rows);long between=System.nanoTime();String second=(String)method.invoke(null,rows);long after=System.nanoTime();
            require(first.equals(second),"SPARK_CAPACITY_FINGERPRINT_CHANGED");
            var report=JSON.createObjectNode().put("version","dat03-spark-gold-capacity-v1").put("status","SPARK_FINGERPRINT_STRESS_PASSED")
                    .put("request_sha256",sha(Files.readAllBytes(input))).put("table",name).put("rows",expected).put("required_rows",r.path("required_rows").asLong())
                    .put("shuffle_partitions",r.path("shuffle_partitions").asInt()).put("fingerprint_sha256",first).put("repeat_fingerprint_sha256",second)
                    .put("writer_class_sha256",text(r,"writer_class_sha256"))
                    .put("count_unique_seconds",(before-start)/1e9).put("first_fingerprint_seconds",(between-before)/1e9).put("repeat_fingerprint_seconds",(after-between)/1e9)
                    .put("synthetic_stress_only",true).put("data_mutations",false).put("full_capacity_accepted",false).put("research_ready",false);
            save(input.getParent().resolve("result.json"),report);
            System.out.println("event=dat03_spark_capacity_passed table="+name+" rows="+expected);
        } finally {spark.stop();}
    }
}
