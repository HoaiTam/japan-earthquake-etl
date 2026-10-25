package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.GoldCommitJob;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

/** Full-pin preparation only. Does NOT transition official lifecycle, commit Iceberg, or authorize fit/import. */
public final class ResearchCollectionJob {
    private ResearchCollectionJob() { }
    static ObjectNode mcProof(JsonNode parent,GoldAuditResult audit) {
        var out=JSON.createObjectNode().put("scope","FULL_PINNED_SPLIT").put("mc_value",audit.mcApplied())
                .put("mc_config_json",audit.mcConfigJson()).put("mc_config_sha256",audit.mcConfigSha256())
                .put("input_event_count",audit.inputEventCount()).put("eligible_event_count",audit.eligibleEventCount())
                .put("excluded_event_count",audit.excludedEventCount());
        for(String k:List.of("dataset_id","gold_snapshot_id","period_start_utc","period_end_utc_exclusive")) out.set(k,parent.path(k));
        return out;
    }
    public static ObjectNode prepare(SparkSession spark,GoldPublisher.Sql sql,SilverObjectStore store,String ns,String id,
            String run,ResearchCollectionPlan.Limits limits,Path output,Runnable lease) throws Exception {
        require(ns.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*") && id.matches("ds_[a-f0-9]{64}"),"COLLECTION_EXACT_DATASET_REQUIRED");
        lease.run(); var parent=FeatureExportJob.manifest(sql,ns,id);
        require("BUILDING".equals(parent.path("dataset_status").asText()),"COLLECTION_BUILDING_PARENT_REQUIRED");
        String physical=parent.path("physical_gold_table_name").asText(), gold=physical.substring(0,physical.lastIndexOf('.'));
        var pubs=DatasetManifestWriter.records(sql.execute("SELECT * FROM "+gold+".publication_status WHERE publication_id="
                +GoldVerificationPlan.quote(parent.path("gold_publication_id").asText())));
        require(pubs.size()==1 && "PASSED".equals(pubs.get(0).path("verify_status").asText())
                && parent.path("gold_snapshot_id").equals(pubs.get(0).path("snapshot_id")),"DS_GOLD_NOT_PUBLISHED");
        // Same identity, publication-time cutoff and persisted config gates as the official audit.
        parent=PinnedAuditResolver.resolve(ns,id,run,sql,lease);
        var coverage=FeatureExportCoverage.resolve(store,pubs.get(0),parent);
        var audit=GoldInputAuditJob.runPinnedAudit(spark,parent,GoldAuditConfig.fromManifest(parent,run,"collection-audit.json"),Instant.now());
        audit.assertReconciled(); require(audit.completenessResult().isReliable() && Double.isFinite(audit.mcApplied()),"DS_EXPORT_MC_UNRELIABLE");
        var audited=audit.auditedEvents().persist(StorageLevel.MEMORY_AND_DISK());
        try {
            // A conservative pre-window metadata bound; no candidate/event catalog is collected by the driver.
            require(audited.filter(col("is_eligible").and(col("magnitude").gt(5.5)).and(col("depth_km").between(50,200)))
                    .limit(ResearchCollectionPlan.MAX_MAINSHOCKS+1).count()<=ResearchCollectionPlan.MAX_MAINSHOCKS,"COLLECTION_MAINSHOCK_METADATA_LIMIT");
            var windows=new MainshockWindowEngine().execute(audited,MainshockWindowJob.selectionConfig(parent),
                    MainshockWindowJob.windowConfig(parent),audit.mcApplied(),id,Instant.parse(parent.path("created_at_utc").asText()),WindowCoveragePolicy.fromManifest(parent));
            return materialize(parent,coverage,mcProof(parent,audit),windows,
                    FeatureScalingConfig.fromJson(JSON.readTree(parent.path("filter_config_json").asText()).path("feature_scaling_config")),limits,output,lease);
        } finally { audited.unpersist(); }
    }
    /** Component API: caller must resolve full pin/coverage/audit in prepare(); never exposed as JSON rows/Mc input. */
    static ObjectNode materialize(JsonNode parent,JsonNode coverage,JsonNode mc,MainshockWindowResult windows,
            FeatureScalingConfig scaling,ResearchCollectionPlan.Limits limits,Path output,Runnable lease) throws Exception {
        windows.assertInvariants();
        var main=windows.mainshockSnapshot().persist(StorageLevel.MEMORY_AND_DISK());
        var candidates=windows.candidateSnapshot().persist(StorageLevel.MEMORY_AND_DISK());
        try {
            var summaries=new ArrayList<ResearchCollectionPlan.Window>();
            for(var row:main.select("mainshock_event_id","candidate_count","resource_guard_status").orderBy("mainshock_event_id")
                    .limit(ResearchCollectionPlan.MAX_MAINSHOCKS+1).collectAsList()) {
                require("PASS".equals(row.getString(2)),"COLLECTION_WINDOW_RESOURCE_BLOCKED");
                summaries.add(new ResearchCollectionPlan.Window(row.getString(0),row.getLong(1)));
            }
            var plan=ResearchCollectionPlan.create(parent,coverage,mc,windows.windowCoverageReport(),summaries,limits);
            require(plan.path("feature_row_count").asLong()==candidates.count(),"COLLECTION_CANDIDATE_COUNT_MISMATCH");
            Path root=safeRoot(output), target=root.resolve(plan.path("collection_id").asText());
            Path receipts=safeRoot(root.resolve("receipts")), receiptPath=receipts.resolve(plan.path("collection_id").asText()+".json");
            boolean existing=Files.exists(target,LinkOption.NOFOLLOW_LINKS);
            require(existing || !Files.exists(receiptPath,LinkOption.NOFOLLOW_LINKS),"COLLECTION_ARTIFACT_MISSING_NO_REGENERATION");
            if(existing) {
                require(Files.isRegularFile(receiptPath,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(receiptPath)
                        && Files.size(receiptPath)<=65536,"COLLECTION_TRUSTED_RECEIPT_MISSING");
                var pin=JSON.readTree(Files.readAllBytes(receiptPath));
                var checked=verify(main.sparkSession(),target,pin.path("manifest_sha256").asText(),pin.path("checksums_sha256").asText());
                ResearchCollectionPlan.verify(checked.path("plan"),parent,coverage,mc,windows.windowCoverageReport(),summaries,limits);
            }
            Path attempt=existing?target:Files.createTempDirectory(root,".collection-attempt-");
            var report=JSON.createObjectNode().put("status","RESEARCH_COLLECTION_PREPARED").put("research_ready",false)
                    .put("official_exported",false).put("parent_status_unchanged","BUILDING");
            report.set("parent_manifest",parent.deepCopy());
            report.set("plan",plan); report.set("scaling_config",JSON.readTree(parent.path("filter_config_json").asText()).path("feature_scaling_config"));
            var artifacts=report.putObject("artifacts");
            for(var shard:plan.path("shards")) {
                lease.run(); var ids=new ArrayList<String>(); shard.path("mainshocks").forEach(r->ids.add(r.path("mainshock_event_id").asText()));
                var selectedMain=main.filter(col("mainshock_event_id").isin(ids.toArray()));
                var selectedCandidates=candidates.filter(col("mainshock_event_id").isin(ids.toArray()));
                var result=new SpacetimeFeatureEngine().execute(selectedMain,selectedCandidates,scaling); result.assertValid();
                var features=columns(result.features()).persist(StorageLevel.MEMORY_AND_DISK());
                try {
                    var quality=validate(features,selectedMain,parent,scaling);
                    String name=shard.path("shard_id").asText(); Path folder=attempt.resolve(name);
                    if(!existing) {
                        Files.createDirectory(folder);
                        LocalFeatureJob.parquet(selectedMain,folder.resolve("mainshocks.parquet"),"dataset_id","mainshock_event_id");
                        LocalFeatureJob.parquet(features,folder.resolve("features.parquet"),"dataset_id","mainshock_event_id","candidate_event_id");
                    } else require(MainshockWindowJob.fingerprint(features).equals(MainshockWindowJob.fingerprint(main.sparkSession().read()
                            .parquet(folder.resolve("features.parquet").toString()))),"COLLECTION_PINNED_CONTENT_CONFLICT");
                    var item=artifacts.putObject(name); item.set("quality",quality);
                    item.put("mainshock_content_sha256",MainshockWindowJob.fingerprint(selectedMain))
                            .put("features_sha256",WindowArtifactInput.sha(folder.resolve("features.parquet")))
                            .put("mainshocks_sha256",WindowArtifactInput.sha(folder.resolve("mainshocks.parquet")));
                } finally { features.unpersist(); }
            }
            if(existing) {
                require(digest(report).equals(digest(JSON.readTree(Files.readAllBytes(target.resolve("collection-manifest.json"))))),"COLLECTION_RERUN_CONFLICT");
                return ((ObjectNode)JSON.readTree(Files.readAllBytes(receiptPath))).put("reused",true);
            }
            immutable(attempt.resolve("collection-manifest.json"),bytes(report));
            var index=new StringBuilder();
            try(var files=Files.walk(attempt)) {for(var path:files.filter(Files::isRegularFile).sorted().toList())
                index.append(WindowArtifactInput.sha(path)).append("  ").append(attempt.relativize(path).toString().replace('\\','/')).append('\n');}
            immutable(attempt.resolve("checksums.sha256"),index.toString().getBytes(StandardCharsets.US_ASCII));
            String manifestSha=WindowArtifactInput.sha(attempt.resolve("collection-manifest.json")), indexSha=WindowArtifactInput.sha(attempt.resolve("checksums.sha256"));
            immutable(attempt.resolve("_PREPARED"),manifestSha.getBytes(StandardCharsets.US_ASCII));
            verify(main.sparkSession(),attempt,manifestSha,indexSha); lease.run();
            Files.move(attempt,target,StandardCopyOption.ATOMIC_MOVE);
            var receipt=JSON.createObjectNode().put("status","RESEARCH_COLLECTION_PREPARED").put("collection_id",plan.path("collection_id").asText())
                    .put("dataset_id",parent.path("dataset_id").asText()).put("plan_sha256",plan.path("plan_sha256").asText())
                    .put("manifest_sha256",manifestSha).put("checksums_sha256",indexSha).put("mainshock_count",plan.path("mainshock_count").asLong())
                    .put("feature_row_count",plan.path("feature_row_count").asLong()).put("shard_count",plan.path("shards").size())
                    .put("research_ready",false).put("official_exported",false).put("reused",false);
            immutable(receiptPath,bytes(receipt)); return receipt;
        } finally { main.unpersist(); candidates.unpersist(); }
    }
    public static ObjectNode verify(SparkSession spark,Path directory,String manifestSha,String indexSha) throws Exception {
        require(manifestSha.matches("[a-f0-9]{64}") && indexSha.matches("[a-f0-9]{64}"),"COLLECTION_EXTERNAL_PINS_REQUIRED");
        require(Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(directory),"COLLECTION_DIRECTORY_REQUIRED");
        Path root=safeRoot(directory), metadata=root.resolve("collection-manifest.json"), index=root.resolve("checksums.sha256");
        require(Files.isRegularFile(metadata,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(metadata) && Files.size(metadata)<=4*1024*1024
                && Files.isRegularFile(index,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(index) && Files.size(index)<=32768
                && manifestSha.equals(WindowArtifactInput.sha(metadata)) && indexSha.equals(WindowArtifactInput.sha(index)),"COLLECTION_CHECKSUM_MISMATCH");
        var report=(ObjectNode)JSON.readTree(Files.readAllBytes(metadata)); var plan=report.path("plan");
        require("RESEARCH_COLLECTION_PREPARED".equals(report.path("status").asText()) && report.path("research_ready").isBoolean()
                && !report.path("research_ready").asBoolean() && report.path("official_exported").isBoolean() && !report.path("official_exported").asBoolean()
                && "BUILDING".equals(report.path("parent_status_unchanged").asText()),"COLLECTION_LIFECYCLE_INVALID");
        var unsigned=(ObjectNode)plan.deepCopy(); unsigned.remove(List.of("collection_id","plan_sha256"));
        require(digest(unsigned).equals(plan.path("plan_sha256").asText()) && ("col_"+digest(unsigned)).equals(plan.path("collection_id").asText()),"COLLECTION_PLAN_CHANGED");
        var expected=new TreeSet<>(Set.of("collection-manifest.json","checksums.sha256","_PREPARED"));
        var dirs=new TreeSet<String>(); var seen=new TreeSet<String>(); long total=0;
        var summaries=new ArrayList<ResearchCollectionPlan.Window>();
        var scaling=FeatureScalingConfig.fromJson(report.path("scaling_config"));
        require(digest(JSON.readTree(report.path("parent_manifest").path("filter_config_json").asText()).path("feature_scaling_config"))
                .equals(digest(report.path("scaling_config"))),"COLLECTION_SCALING_PIN_CONFLICT");
        for(var shard:plan.path("shards")) {
            String name=shard.path("shard_id").asText(); require(name.matches("shard_[0-9]{5}") && dirs.add(name),"COLLECTION_SHARD_SET_INVALID");
            require(Files.isDirectory(root.resolve(name),LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(root.resolve(name)),"COLLECTION_SHARD_SET_INVALID");
            for(String file:List.of("features.parquet","mainshocks.parquet")) require(Files.isRegularFile(root.resolve(name+"/"+file),LinkOption.NOFOLLOW_LINKS)
                    && !Files.isSymbolicLink(root.resolve(name+"/"+file)) && Files.size(root.resolve(name+"/"+file))<=67_108_864,"COLLECTION_FILE_INVALID");
            for(String file:List.of("features.parquet","mainshocks.parquet")) expected.add(name+"/"+file);
            var main=spark.read().parquet(root.resolve(name+"/mainshocks.parquet").toString());
            var features=spark.read().parquet(root.resolve(name+"/features.parquet").toString());
            require(Arrays.asList(features.columns()).equals(COLUMNS) && features.filter(not(col("mc_value").eqNullSafe(plan.path("global_mc").path("mc_value").asDouble())))
                    .limit(1).count()==0,"COLLECTION_GLOBAL_MC_CONFLICT");
            var identity=JSON.createObjectNode().put("dataset_id",plan.path("dataset_id").asText()).put("feature_version",plan.path("feature_version").asText())
                    .put("scaling_config_version",plan.path("scaling_config_version").asText());
            var quality=validate(features,main,identity,scaling); var artifact=report.path("artifacts").path(name);
            require(digest(quality).equals(digest(artifact.path("quality"))) && main.count()==shard.path("mainshock_count").asLong()
                    && features.count()==shard.path("feature_row_count").asLong()
                    && MainshockWindowJob.fingerprint(main).equals(artifact.path("mainshock_content_sha256").asText())
                    && WindowArtifactInput.sha(root.resolve(name+"/features.parquet")).equals(artifact.path("features_sha256").asText())
                    && WindowArtifactInput.sha(root.resolve(name+"/mainshocks.parquet")).equals(artifact.path("mainshocks_sha256").asText()),"COLLECTION_SHARD_READBACK_FAILED");
            var actual=new TreeMap<String,Long>(); for(var row:main.select("mainshock_event_id","candidate_count").collectAsList()) {
                require(seen.add(row.getString(0)),"COLLECTION_DUPLICATE_MAINSHOCK"); actual.put(row.getString(0),row.getLong(1));
                summaries.add(new ResearchCollectionPlan.Window(row.getString(0),row.getLong(1)));
            }
            var declared=new TreeMap<String,Long>(); for(var row:shard.path("mainshocks")) require(declared.put(row.path("mainshock_event_id").asText(),row.path("candidate_row_count").asLong())==null,"COLLECTION_DUPLICATE_MAINSHOCK");
            require(actual.equals(declared),"COLLECTION_ASSIGNMENT_OR_PIN_CONFLICT"); total+=features.count();
        }
        require(total==plan.path("feature_row_count").asLong() && seen.size()==plan.path("mainshock_count").asLong()
                && report.path("artifacts").size()==dirs.size(),"COLLECTION_TOTALS_UNBALANCED");
        ResearchCollectionPlan.verify(plan,report.path("parent_manifest"),plan.path("coverage"),plan.path("global_mc"),plan.path("window_coverage"),
                summaries,ResearchCollectionPlan.Limits.read(plan.path("layout_limits")));
        var found=new TreeSet<String>(); try(var paths=Files.walk(root)) {for(var file:paths.toList()) if(!file.equals(root)) {
            require(!Files.isSymbolicLink(file),"COLLECTION_SYMLINK_REJECTED"); String relative=root.relativize(file).toString().replace('\\','/');
            if(Files.isDirectory(file)) require(dirs.contains(relative),"COLLECTION_EXTRA_DIRECTORY");
            else {require(Files.isRegularFile(file) && Files.size(file)<=67_108_864,"COLLECTION_FILE_INVALID"); found.add(relative);}
        }}
        require(found.equals(expected) && Files.readString(root.resolve("_PREPARED")).equals(manifestSha),"COLLECTION_FILE_SET_CHANGED");
        var indexed=new TreeSet<String>(); for(String line:Files.readAllLines(index)) {
            require(line.matches("[a-f0-9]{64}  (collection-manifest\\.json|shard_[0-9]{5}/(features|mainshocks)\\.parquet)"),"COLLECTION_INDEX_INVALID");
            String name=line.substring(66); require(indexed.add(name) && WindowArtifactInput.sha(root.resolve(name)).equals(line.substring(0,64)),"COLLECTION_INDEX_INVALID");
        }
        expected.removeAll(Set.of("checksums.sha256","_PREPARED")); require(indexed.equals(expected),"COLLECTION_INDEX_INVALID");
        return report;
    }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"COLLECTION_REQUEST_REQUIRED"); Path path=Path.of(args[0]);
        require(Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) && Files.size(path)<=65536,"COLLECTION_REQUEST_REQUIRED");
        var request=JSON.readTree(Files.readAllBytes(path));
        var fields=new HashSet<String>(); request.fieldNames().forEachRemaining(fields::add);
        require(fields.equals(Set.of("collection_version","ml_namespace","dataset_id","dag_id","run_id","layout_limits","output_root","runner_sha256"))
                && LocalFeatureJob.codeSha().equals(request.path("runner_sha256").asText()),"COLLECTION_EXACT_REQUEST_AND_RUNNER_REQUIRED");
        require(ResearchCollectionPlan.VERSION.equals(request.path("collection_version").asText()),"COLLECTION_VERSION_REQUIRED");
        String run=request.path("run_id").asText(), dag=request.path("dag_id").asText();
        require(run.matches("[A-Za-z0-9_-]{1,128}") && "dat03_collection_prepare".equals(dag),"COLLECTION_OWNER_REQUIRED");
        var owner=Path.of(Objects.requireNonNull(System.getenv("SOURCE_GUARD_ROOT")),"owner.json");
        Runnable lease=()->{try{var live=JSON.readTree(Files.readAllBytes(owner)); require(run.equals(live.path("run_id").asText()) && dag.equals(live.path("dag_id").asText()),"COLLECTION_LEASE_REQUIRED");}
            catch(Exception error){throw new IllegalStateException("COLLECTION_LEASE_REQUIRED");}};
        lease.run(); var spark=GoldCommitJob.configured(System.getenv()).appName("dat03-collection-prepare").getOrCreate(); spark.sparkContext().setLogLevel("ERROR");
        try(var store=MinioSilverObjectStore.fromEnvironment()) {
            var sql=new TrinoSqlClient(URI.create("http://"+System.getenv("TRINO_HOST")+":"+System.getenv("TRINO_INTERNAL_PORT")));
            var result=prepare(spark,sql,store,request.path("ml_namespace").asText(),request.path("dataset_id").asText(),run,
                    ResearchCollectionPlan.Limits.read(request.path("layout_limits")),Path.of(request.path("output_root").asText()),lease);
            immutable(path.getParent().resolve("receipt.json"),bytes(result)); System.out.println(result);
        } finally {spark.stop();}
    }
}
