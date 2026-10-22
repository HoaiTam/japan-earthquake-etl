package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.*;
import ie212.earthquake.spark.silver.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;

/** Reads only an exact, freshly verified Silver bundle; no prefix scan or latest lookup. */
public final class GoldInputReader {
    private static final ObjectMapper JSON = new ObjectMapper();
    private GoldInputReader() { }
    public static GoldTransformationResult read(SparkSession spark, SilverObjectStore store, String silverRun,
            String expectedBundleSha, GoldRunContext context, java.time.Instant processed, Path sharedRoot) throws Exception {
        return read(spark,store,silverRun,expectedBundleSha,context,processed,sharedRoot,null);
    }
    /** Operational adapter filters the cumulative state BEFORE transformation/replacement. */
    public static GoldTransformationResult read(SparkSession spark, SilverObjectStore store, String silverRun,
            String expectedBundleSha, GoldRunContext context, java.time.Instant processed, Path sharedRoot, List<String> months) throws Exception {
        var bundle = new SilverBundlePublisher(store).verify(silverRun);
        byte[] bytes = store.read(SilverBundlePublisher.root(silverRun) + "/manifest.json");
        if (!SilverBundlePublisher.sha(bytes).equals(expectedBundleSha)
                || !bundle.path("context").path("quality_passed").asBoolean(false)
                || !bundle.path("context").path("reconciliation_balanced").asBoolean(false))
            throw new IOException("VERIFIED_SILVER_PIN_REQUIRED");
        String uri = store.uriForKey(SilverBundlePublisher.root(silverRun) + "/manifest.json");
        if (!context.inputManifestUris().equals(List.of(uri))) throw new IOException("EXACT_SILVER_CONTEXT_REQUIRED");
        Files.createDirectories(sharedRoot);
        var observations = dataset(spark, store, bundle, "source_observation", sharedRoot);
        var memberships = dataset(spark, store, bundle, "canonical_membership", sharedRoot);
        var links = dataset(spark, store, bundle, "source_link", sharedRoot);
        if(months==null && bundle.path("context").has("publication_quality"))
            throw new IOException("ASOF_EXPLICIT_GOLD_ADAPTER_REQUIRED");
        if(months!=null) {
            if(!"PROVISIONAL".equals(bundle.path("context").path("publication_quality").asText()) || links.count()!=0)
                throw new IOException("ASOF_USGS_ONLY_SILVER_REQUIRED");
            observations=observations.filter(GoldIcebergWriter.monthPredicate(months));
            var ids=observations.filter(org.apache.spark.sql.functions.col("is_current_source_revision")).select("source_observation_id");
            memberships=memberships.join(ids,"source_observation_id","inner");
        }
        return new GoldEventTransformer().transform(observations, memberships, links,
                spark.createDataFrame(List.<Row>of(), GoldEventTransformer.REGION_SCHEMA), context, processed);
    }
    private static Dataset<Row> dataset(SparkSession spark, SilverObjectStore store, JsonNode bundle,
            String name, Path root) throws Exception {
        var manifest = JSON.readTree(store.read(SilverBundlePublisher.root(bundle.path("run_id").asText()) + "/"+name+"/manifest.json"));
        List<String> paths = new ArrayList<>(); int index = 0;
        for (var file : manifest.path("files")) {
            byte[] bytes = store.read(file.path("relative_path").asText());
            if (!SilverBundlePublisher.sha(bytes).equals(file.path("sha256").asText())) throw new IOException("SILVER_CHANGED_DURING_READ");
            Path target = root.resolve(name+"-"+index+++".parquet");
            if (Files.exists(target) && !Arrays.equals(Files.readAllBytes(target), bytes)) throw new IOException("STAGING_CONFLICT");
            Files.write(target, bytes); paths.add(target.toAbsolutePath().toString());
            if (Files.getFileStore(target).supportsFileAttributeView("posix"))
                Files.setPosixFilePermissions(target, java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
        }
        return spark.read().parquet(paths.toArray(String[]::new));
    }
}
