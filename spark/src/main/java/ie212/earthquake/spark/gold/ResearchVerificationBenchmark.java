package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.file.*;
import java.util.*;

/** Read-only A/B measurement; never publishes, releases leases or certifies a catalog. */
public final class ResearchVerificationBenchmark {
    private static final ObjectMapper JSON = new ObjectMapper();
    private ResearchVerificationBenchmark() { }
    static long scalar(GoldPublisher.Result result) {
        if (result.rows().size()!=1 || result.rows().get(0).size()!=1
                || !result.rows().get(0).get(0).isIntegralNumber()
                || !result.rows().get(0).get(0).canConvertToLong()
                || result.rows().get(0).get(0).asLong()<0)
            throw new IllegalArgumentException("BENCHMARK_INTEGRAL_RESULT_REQUIRED");
        return result.rows().get(0).get(0).asLong();
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=3 || !GoldVerificationPlan.BUCKETED_FACT_BLOCKERS.contains(args[1]))
            throw new IllegalArgumentException("BENCHMARK_COMMIT_CHECK_OUTPUT_REQUIRED");
        Path input=Path.of(args[0]), output=Path.of(args[2]);
        if(Files.size(input)>131072 || Files.exists(output))throw new IllegalArgumentException("BENCHMARK_NEW_OUTPUT_REQUIRED");
        byte[] bytes=Files.readAllBytes(input);
        var plan=new GoldVerificationPlan(JSON.readTree(bytes));
        if(!plan.namespace().matches("iceberg\\.dat03_catalog_[a-f0-9]{32}"))
            throw new IllegalArgumentException("BENCHMARK_ISOLATED_CATALOG_REQUIRED");
        var sql=TrinoSqlClient.researchCatalog(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
        var report=JSON.createObjectNode().put("version","dat03-readonly-verification-ab-v1")
                .put("commit_sha256",HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
                .put("check",args[1]).put("published",false).put("acceptance",false);
        var trials=report.putArray("trials");
        // Same literal snapshots and exact existing SQL generators. No sampling or weakened check.
        long whole=-1, sum=0;
        for(int i=-1;i<16;i++) {
            String label=i<0?"whole":"bucket-"+Integer.toHexString(i);
            String query=i<0?plan.blockers().get(args[1]):plan.factBlockerBucket(args[1],Integer.toHexString(i));
            long start=System.nanoTime();
            long actual=scalar(sql.execute(query));
            double seconds=(System.nanoTime()-start)/1e9;
            trials.addObject().put("variant",label).put("actual",actual).put("seconds",seconds);
            System.out.println("event=verification_benchmark variant="+label+" actual="+actual+" seconds="+seconds);
            if(i<0)whole=actual;else sum=Math.addExact(sum,actual);
        }
        if(whole!=sum)throw new IllegalStateException("BENCHMARK_EQUIVALENCE_FAILED");
        report.put("equivalent",true).put("whole_actual",whole).put("bucket_sum_actual",sum);
        Files.write(output,JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(report),StandardOpenOption.CREATE_NEW);
        System.out.println("event=verification_benchmark_complete equivalent=true acceptance=false");
    }
}
