package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.apache.spark.sql.*;

/** Explicit collection layout. A shard is an execution scope, NEVER a separate dataset. */
public final class ResearchCollectionExportBundle {
    public static final String VERSION="dat03-collection-export-v1";
    private ResearchCollectionExportBundle() { }

    static void config(JsonNode parent) throws Exception {
        var c=JSON.readTree(parent.path("filter_config_json").asText()).path("collection_export");
        require(c.isObject() && c.size()==2 && VERSION.equals(c.path("version").asText()),"COLLECTION_EXPORT_CONFIG_REQUIRED");
        ResearchCollectionPlan.Limits.read(c.path("layout_limits"));
        require(WindowCoveragePolicy.STRICT.equals(WindowCoveragePolicy.fromManifest(parent)),"COLLECTION_STRICT_SPLIT_REQUIRED");
    }
    static ObjectNode identity(JsonNode prepared,String mainSha,String featureSha) throws Exception {
        var plan=prepared.path("plan");
        return JSON.createObjectNode().put("version",VERSION).put("dataset_id",plan.path("dataset_id").asText())
                .put("plan_sha256",plan.path("plan_sha256").asText()).put("prepared_manifest_sha256",digest(prepared))
                .put("mainshock_content_sha256",mainSha).put("feature_content_sha256",featureSha);
    }
    static Dataset<Row> read(SparkSession spark,Path root,JsonNode plan,String file) {
        String[] paths=new String[plan.path("shards").size()];int n=0;
        for(var shard:plan.path("shards")) paths[n++]=root.resolve(shard.path("shard_id").asText()+"/"+file).toString();
        return spark.read().parquet(paths);
    }
    static ObjectNode materialize(SparkSession spark,Path preparedRoot,JsonNode prepared,ObjectNode validated,Path output) throws Exception {
        require("VALIDATED".equals(validated.path("dataset_status").asText()),"DS_VALIDATED_DATASET_REQUIRED");
        config(validated); Path root=safeRoot(output),target=root.resolve("dataset_id="+validated.path("dataset_id").asText());
        if(Files.exists(target,LinkOption.NOFOLLOW_LINKS)) {
            var existing=verify(spark,target);
            require(existing.path("export_identity_sha256").equals(validated.path("export_identity_sha256")),"COLLECTION_EXPORT_IDENTITY_CONFLICT");
            return existing;
        }
        Path attempt=Files.createTempDirectory(root,".collection-export-attempt-");
        var files=new TreeMap<String,String>();
        for(var shard:prepared.path("plan").path("shards")) {
            String name=shard.path("shard_id").asText(); Files.createDirectory(attempt.resolve(name));
            for(String file:List.of("features.parquet","mainshocks.parquet")) {
                Path source=preparedRoot.resolve(name+"/"+file),dest=attempt.resolve(name+"/"+file);
                require(!Files.isSymbolicLink(source) && Files.isRegularFile(source) && Files.size(source)<=67_108_864,"COLLECTION_FILE_INVALID");
                Files.copy(source,dest); files.put(name+"/"+file,WindowArtifactInput.sha(dest));
            }
        }
        immutable(attempt.resolve("collection-preparation.json"),bytes(prepared));
        files.put("collection-preparation.json",WindowArtifactInput.sha(attempt.resolve("collection-preparation.json")));
        StringBuilder dataIndex=new StringBuilder();files.forEach((name,sha)->dataIndex.append(sha).append("  ").append(name).append('\n'));
        immutable(attempt.resolve("export-checksums.sha256"),dataIndex.toString().getBytes(StandardCharsets.US_ASCII));
        var manifest=validated.deepCopy().put("dataset_status","EXPORTED").put("export_version",VERSION)
                .put("export_manifest_ref","exports/"+validated.path("dataset_id").asText()+"/export-checksums.sha256")
                .put("export_manifest_sha256",WindowArtifactInput.sha(attempt.resolve("export-checksums.sha256")));
        manifest.set("collection_plan",prepared.path("plan").deepCopy());
        immutable(attempt.resolve("dataset_manifest.json"),bytes(manifest));
        files.put("dataset_manifest.json",WindowArtifactInput.sha(attempt.resolve("dataset_manifest.json")));
        files.put("export-checksums.sha256",WindowArtifactInput.sha(attempt.resolve("export-checksums.sha256")));
        var index=new StringBuilder();files.forEach((name,sha)->index.append(sha).append("  ").append(name).append('\n'));
        immutable(attempt.resolve("checksums.sha256"),index.toString().getBytes(StandardCharsets.US_ASCII));
        verify(spark,attempt);Files.move(attempt,target,StandardCopyOption.ATOMIC_MOVE);return manifest;
    }
    static Map<String,String> index(Path root,String file) throws Exception {
        Path p=root.resolve(file); require(Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(p)
                && Files.size(p)<=32768,"COLLECTION_EXPORT_INDEX_INVALID");
        var out=new TreeMap<String,String>();
        for(String line:Files.readAllLines(p,StandardCharsets.US_ASCII)) {
            require(line.matches("[a-f0-9]{64}  (dataset_manifest\\.json|collection-preparation\\.json|export-checksums\\.sha256|shard_[0-9]{5}/(?:features|mainshocks)\\.parquet)"),"COLLECTION_EXPORT_INDEX_INVALID");
            String name=line.substring(66);require(out.put(name,line.substring(0,64))==null,"COLLECTION_EXPORT_INDEX_INVALID");
            Path item=root.resolve(name); require(Files.isRegularFile(item,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(item)
                    && Files.size(item)<=67_108_864 && WindowArtifactInput.sha(item).equals(line.substring(0,64)),"COLLECTION_EXPORT_CHECKSUM_MISMATCH");
        }
        return out;
    }
    public static ObjectNode verify(SparkSession spark,Path source) throws Exception {
        require(Files.isDirectory(source,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(source),"COLLECTION_DIRECTORY_REQUIRED");
        Path root=safeRoot(source); var expected=index(root,"checksums.sha256");
        var manifest=(ObjectNode)JSON.readTree(Files.readAllBytes(root.resolve("dataset_manifest.json")));
        require(VERSION.equals(manifest.path("export_version").asText()) && "EXPORTED".equals(manifest.path("dataset_status").asText()),"COLLECTION_EXPORT_NOT_EXPORTED");
        config(manifest); var prepared=JSON.readTree(Files.readAllBytes(root.resolve("collection-preparation.json")));var plan=prepared.path("plan");
        require(plan.equals(manifest.path("collection_plan")) && "RESEARCH_COLLECTION_PREPARED".equals(prepared.path("status").asText())
                && plan.path("dataset_id").equals(manifest.path("dataset_id")) && plan.path("collection_id").equals(manifest.path("collection_id"))
                && plan.path("window_coverage").equals(manifest.path("window_coverage")),"COLLECTION_EXPORT_PLAN_CHANGED");
        for(String key:List.of("dataset_id","identity_payload_sha256","gold_table_name","physical_gold_table_name","gold_snapshot_id","gold_publication_id",
                "publication_bundle_sha256","dataset_split","period_start_utc","period_end_utc_exclusive","observation_cutoff_utc","filter_config_json","created_at_utc"))
            require(manifest.path(key).toString().equals(prepared.path("parent_manifest").path(key).toString()),"COLLECTION_EXPORT_PARENT_CHANGED");
        for(String key:DatasetIdentityPlan.VERSIONS)require(manifest.path(key).equals(prepared.path("parent_manifest").path(key)),"COLLECTION_EXPORT_PARENT_CHANGED");
        require(digest(JSON.readTree(manifest.path("filter_config_json").asText()).path("feature_scaling_config"))
                .equals(digest(prepared.path("scaling_config"))),"COLLECTION_SCALING_PIN_CONFLICT");
        var declared=new TreeSet<>(Set.of("dataset_manifest.json","collection-preparation.json","export-checksums.sha256"));
        var dirs=new TreeSet<String>();var summaries=new ArrayList<ResearchCollectionPlan.Window>();var seen=new HashSet<String>();long rows=0;
        var scaling=FeatureScalingConfig.fromJson(prepared.path("scaling_config"));
        for(var shard:plan.path("shards")) {
            String name=shard.path("shard_id").asText();require(name.matches("shard_[0-9]{5}") && dirs.add(name),"COLLECTION_SHARD_SET_INVALID");
            for(String f:List.of("features.parquet","mainshocks.parquet")) declared.add(name+"/"+f);
            var main=spark.read().parquet(root.resolve(name+"/mainshocks.parquet").toString()).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
            var features=spark.read().parquet(root.resolve(name+"/features.parquet").toString()).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK());
            try {
            var quality=validate(features,main,manifest,scaling);
            require(digest(quality).equals(digest(prepared.path("artifacts").path(name).path("quality")))
                    && quality.path("feature_schema").equals(manifest.path("feature_schema"))
                    && quality.path("feature_schema_sha256").equals(manifest.path("feature_schema_sha256"))
                    && MainshockWindowJob.fingerprint(main).equals(prepared.path("artifacts").path(name).path("mainshock_content_sha256").asText())
                    && features.count()==shard.path("feature_row_count").asLong() && main.count()==shard.path("mainshock_count").asLong()
                    && features.filter(not(col("mc_value").eqNullSafe(plan.path("global_mc").path("mc_value").asDouble()))).limit(1).count()==0,
                    "COLLECTION_EXPORT_SHARD_CHANGED");
            for(var row:main.select("mainshock_event_id","candidate_count").collectAsList()) {
                require(seen.add(row.getString(0)),"COLLECTION_DUPLICATE_MAINSHOCK");summaries.add(new ResearchCollectionPlan.Window(row.getString(0),row.getLong(1)));
            }
            rows+=features.count();
            } finally {main.unpersist();features.unpersist();}
        }
        require(rows==manifest.path("feature_row_count").asLong() && seen.size()==manifest.path("mainshock_count").asLong(),"COLLECTION_TOTALS_UNBALANCED");
        ResearchCollectionPlan.verify(plan,prepared.path("parent_manifest"),plan.path("coverage"),plan.path("global_mc"),plan.path("window_coverage"),summaries,
                ResearchCollectionPlan.Limits.read(plan.path("layout_limits")));
        var configLimits=JSON.readTree(manifest.path("filter_config_json").asText()).path("collection_export").path("layout_limits");
        require(configLimits.equals(plan.path("layout_limits")),"COLLECTION_EXPORT_LAYOUT_CHANGED");
        var found=new TreeSet<String>();try(var paths=Files.walk(root)) { for(var path:paths.toList()) if(!path.equals(root)) {
            require(!Files.isSymbolicLink(path),"COLLECTION_SYMLINK_REJECTED");String relative=root.relativize(path).toString().replace('\\','/');
            if(Files.isDirectory(path)) require(dirs.contains(relative),"COLLECTION_EXTRA_DIRECTORY");else found.add(relative);
        }}
        var all=new TreeSet<>(declared);all.add("checksums.sha256");
        require(found.equals(all) && expected.keySet().equals(declared),"COLLECTION_EXPORT_FILE_SET_CHANGED");
        var data=new TreeMap<>(expected);data.remove("dataset_manifest.json");data.remove("export-checksums.sha256");
        require(data.equals(index(root,"export-checksums.sha256")) && WindowArtifactInput.sha(root.resolve("export-checksums.sha256"))
                .equals(manifest.path("export_manifest_sha256").asText()),"COLLECTION_EXPORT_INDEX_CHANGED");
        String mainSha=MainshockWindowJob.fingerprint(read(spark,root,plan,"mainshocks.parquet"));
        String featureSha=MainshockWindowJob.fingerprint(read(spark,root,plan,"features.parquet"));
        require(featureSha.equals(manifest.path("feature_content_sha256").asText()) && mainSha.equals(manifest.path("mainshock_content_sha256").asText())
                && digest(identity(prepared,mainSha,featureSha)).equals(manifest.path("export_identity_sha256").asText()),"COLLECTION_EXPORT_IDENTITY_CONFLICT");
        return manifest;
    }
}
