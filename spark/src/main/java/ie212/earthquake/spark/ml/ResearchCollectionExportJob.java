package ie212.earthquake.spark.ml;

import static ie212.earthquake.spark.ml.FeatureExportBundle.*;
import static org.apache.spark.sql.functions.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.gold.*;
import ie212.earthquake.spark.silver.SilverObjectStore;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.*;

/** Official, opt-in collection export. Full-parent lifecycle; bounded whole-window shard handoff. */
public final class ResearchCollectionExportJob {
    private ResearchCollectionExportJob() { }
    static ObjectNode prepared(SparkSession spark,GoldPublisher.Sql sql,SilverObjectStore store,String ns,String id,String run,
            Path output,Runnable lease) throws Exception {
        var parent=FeatureExportJob.manifest(sql,ns,id);ResearchCollectionExportBundle.config(parent);
        var c=JSON.readTree(parent.path("filter_config_json").asText()).path("collection_export");
        Path root=safeRoot(output.resolve("collection-prepared").resolve(id)),receipt=root.resolve("preparation-receipt.json");
        ObjectNode pin;
        if(!Files.exists(receipt,LinkOption.NOFOLLOW_LINKS)) {
            require("BUILDING".equals(parent.path("dataset_status").asText()),"COLLECTION_PREPARATION_MISSING_NO_REGENERATION");
            pin=ResearchCollectionJob.prepare(spark,sql,store,ns,id,run,ResearchCollectionPlan.Limits.read(c.path("layout_limits")),root,lease);
            immutable(receipt,bytes(pin));
        } else {
            require(Files.isRegularFile(receipt,LinkOption.NOFOLLOW_LINKS) && !Files.isSymbolicLink(receipt) && Files.size(receipt)<=65536,"COLLECTION_TRUSTED_RECEIPT_MISSING");
            pin=(ObjectNode)JSON.readTree(Files.readAllBytes(receipt));
        }
        require(pin.path("collection_id").asText().matches("col_[a-f0-9]{64}"),"COLLECTION_EXACT_ID_REQUIRED");
        Path folder=root.resolve(pin.path("collection_id").asText());
        var result=ResearchCollectionJob.verify(spark,folder,pin.path("manifest_sha256").asText(),pin.path("checksums_sha256").asText());
        require(result.path("plan").path("layout_limits").equals(c.path("layout_limits")),"COLLECTION_EXPORT_LAYOUT_CHANGED");
        var original=result.path("parent_manifest");
        for(String key:List.of("dataset_id","identity_payload_sha256","gold_snapshot_id","gold_publication_id","physical_gold_table_name",
                "publication_bundle_sha256","filter_config_json","observation_cutoff_utc","created_at_utc"))
            require(original.path(key).equals(parent.path(key)),"COLLECTION_PARENT_PIN_CHANGED");
        String physical=parent.path("physical_gold_table_name").asText(),gold=physical.substring(0,physical.lastIndexOf('.'));
        var pub=DatasetManifestWriter.records(sql.execute("SELECT * FROM "+gold+".publication_status WHERE publication_id="
                +GoldVerificationPlan.quote(parent.path("gold_publication_id").asText())));
        require(pub.size()==1 && "PASSED".equals(pub.get(0).path("verify_status").asText()),"DS_GOLD_NOT_PUBLISHED");
        require(FeatureExportCoverage.resolve(store,pub.get(0),parent).equals(result.path("plan").path("coverage")),"COLLECTION_COVERAGE_PIN_CHANGED");
        result.put("prepared_root",folder.toString());return result;
    }
    static Path preparedPath(Path output,String id,JsonNode prepared) {
        return output.resolve("collection-prepared").resolve(id).resolve(prepared.path("plan").path("collection_id").asText());
    }
    static ObjectNode persistedPreparation(JsonNode prepared) {
        var copy=(ObjectNode)prepared.deepCopy();copy.remove("prepared_root");return copy;
    }
    public static ObjectNode export(SparkSession spark,GoldPublisher.Sql sql,SilverObjectStore store,String ns,String id,String run,
            Path output,Path registry,Runnable lease,boolean validateOnly) throws Exception {
        require(ns.matches("[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*") && id.matches("ds_[a-f0-9]{64}"),"COLLECTION_EXACT_DATASET_REQUIRED");
        lease.run();var original=FeatureExportJob.manifest(sql,ns,id);DatasetManifestWriter.pinnedGoldSql(original);
        require(Set.of("BUILDING","VALIDATED","EXPORTED").contains(original.path("dataset_status").asText()),"DS_EXPORT_NOT_ACTIVE");
        String physical=original.path("physical_gold_table_name").asText(),gold=physical.substring(0,physical.lastIndexOf('.'));
        require(!ns.equals(gold) && ns.split("\\.")[0].equals(gold.split("\\.")[0]),"DS_EXPORT_NAMESPACE_CONFLICT");
        Path root=safeRoot(output),trustedRoot=safeRoot(registry),target=root.resolve("dataset_id="+id);
        var prep=persistedPreparation(prepared(spark,sql,store,ns,id,run,root,lease));var plan=prep.path("plan");
        Path from=preparedPath(root,id,prep);
        var main=ResearchCollectionExportBundle.read(spark,from,plan,"mainshocks.parquet");
        var features=columns(ResearchCollectionExportBundle.read(spark,from,plan,"features.parquet"));
        String mainSha=MainshockWindowJob.fingerprint(main),featureSha=MainshockWindowJob.fingerprint(features);
        String identity=digest(ResearchCollectionExportBundle.identity(prep,mainSha,featureSha));
        var audit=plan.path("global_mc").path("audit_manifest");
        require(audit.isObject() && audit.path("dataset_id").equals(original.path("dataset_id")),"COLLECTION_FULL_AUDIT_REQUIRED");
        long mainPin,candidatePin;
        if("BUILDING".equals(original.path("dataset_status").asText())) {
            mainPin=FeatureSnapshotWriter.commit(main,ns+".mainshock_candidate_snapshot",id,identity,lease);
            candidatePin=FeatureSnapshotWriter.commit(features,ns+".sequence_candidate_snapshot",id,identity,lease);
        } else {
            // A missing committed table/snapshot on a terminal rerun is corruption,
            // never permission to regenerate a new snapshot behind the old receipt.
            mainPin=original.path("mainshock_snapshot_id").asLong();candidatePin=original.path("candidate_snapshot_id").asLong();
            require(mainPin>0 && candidatePin>0 && identity.equals(original.path("export_identity_sha256").asText()),"COLLECTION_VALIDATED_PIN_CONFLICT");
        }
        long mainCount=plan.path("mainshock_count").asLong(),rowCount=plan.path("feature_row_count").asLong();
        FeatureExportJob.verifyPin(sql,ns+".mainshock_candidate_snapshot",mainPin,id,identity,mainCount);
        FeatureExportJob.verifyPin(sql,ns+".sequence_candidate_snapshot",candidatePin,id,identity,rowCount);
        if("BUILDING".equals(original.path("dataset_status").asText()))for(var entry:Map.of("mainshock_snapshot_id","bigint","candidate_snapshot_id","bigint","export_identity_sha256","varchar",
                "export_version","varchar","collection_id","varchar","collection_shard_count","bigint").entrySet()) {
            lease.run();sql.execute("ALTER TABLE "+ns+".dataset_manifest ADD COLUMN IF NOT EXISTS "+entry.getKey()+" "+entry.getValue());
        }
        if("BUILDING".equals(original.path("dataset_status").asText())) {
            var changes=new LinkedHashMap<String,JsonNode>();
            for(String key:List.of("input_event_count","eligible_event_count","excluded_event_count","exclusion_counts_json","mc_method","mc_value","mc_region_version","mc_config_json","mc_config_sha256")) {
                require(audit.hasNonNull(key),"COLLECTION_FULL_AUDIT_REQUIRED");changes.put(key,audit.path(key));
            }
            var values=JSON.createObjectNode().put("mainshock_count",mainCount).put("candidate_row_count",rowCount).put("feature_row_count",rowCount)
                    .put("feature_schema_sha256",digest(LocalFeatureJob.schema(features))).put("mainshock_snapshot_id",mainPin).put("candidate_snapshot_id",candidatePin)
                    .put("export_identity_sha256",identity).put("dataset_status","VALIDATED").put("export_version",ResearchCollectionExportBundle.VERSION)
                    .put("collection_id",plan.path("collection_id").asText()).put("collection_shard_count",plan.path("shards").size())
                    .put("updated_at_utc",Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString());
            values.fields().forEachRemaining(e->changes.put(e.getKey(),e.getValue()));FeatureExportJob.update(sql,ns,id,"BUILDING",changes,lease);
        }
        var current=(ObjectNode)FeatureExportJob.manifest(sql,ns,id).deepCopy();
        require(identity.equals(current.path("export_identity_sha256").asText()) && mainPin==current.path("mainshock_snapshot_id").asLong()
                && candidatePin==current.path("candidate_snapshot_id").asLong() && plan.path("collection_id").equals(current.path("collection_id")),"COLLECTION_VALIDATED_PIN_CONFLICT");
        var receipt=JSON.createObjectNode().put("dataset_id",id).put("collection_id",plan.path("collection_id").asText())
                .put("mainshock_count",mainCount).put("candidate_row_count",rowCount).put("feature_row_count",rowCount).put("shard_count",plan.path("shards").size())
                .put("mainshock_snapshot_id",Long.toString(mainPin)).put("candidate_snapshot_id",Long.toString(candidatePin))
                .put("invalid_feature_count",0).put("quality_gate_status","PASSED");
        if(validateOnly && !"EXPORTED".equals(current.path("dataset_status").asText()))
            return receipt.put("status","FEATURE_SNAPSHOT_VALIDATED").put("exported",false);
        if(!"EXPORTED".equals(current.path("dataset_status").asText())) {
            var validated=current.deepCopy().put("dataset_status","VALIDATED").put("mainshock_snapshot_id",Long.toString(mainPin))
                    .put("candidate_snapshot_id",Long.toString(candidatePin)).put("feature_content_sha256",featureSha).put("mainshock_content_sha256",mainSha)
                    .put("export_artifact_uri","ml-export/dataset_id="+id+"/");
            validated.set("coverage",plan.path("coverage").deepCopy());validated.set("window_coverage",plan.path("window_coverage").deepCopy());
            validated.set("feature_schema",LocalFeatureJob.schema(features));validated.set("vector_columns",JSON.valueToTree(SpacetimeFeatureEngine.VECTOR));
            var bundle=ResearchCollectionExportBundle.materialize(spark,from,prep,validated,root);lease.run();
            var changes=new LinkedHashMap<String,JsonNode>();for(String key:List.of("export_artifact_uri","export_manifest_sha256"))changes.put(key,bundle.path(key));
            changes.put("dataset_status",JSON.getNodeFactory().textNode("EXPORTED"));
            changes.put("updated_at_utc",JSON.getNodeFactory().textNode(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS).toString()));
            FeatureExportJob.update(sql,ns,id,"VALIDATED",changes,lease);
        }
        var bundle=ResearchCollectionExportBundle.verify(spark,target);current=(ObjectNode)FeatureExportJob.manifest(sql,ns,id).deepCopy();
        require("EXPORTED".equals(current.path("dataset_status").asText()) && bundle.path("export_manifest_sha256").equals(current.path("export_manifest_sha256"))
                && bundle.path("export_identity_sha256").equals(current.path("export_identity_sha256")),"COLLECTION_EXPORTED_PIN_CONFLICT");
        var exactFeatures=FeatureSnapshotWriter.read(spark,ns+".sequence_candidate_snapshot",candidatePin,id);
        require(MainshockWindowJob.fingerprint(exactFeatures).equals(featureSha),"DS_EXPORT_PINNED_CONTENT_CONFLICT");
        var table=Spark3Util.loadIcebergTable(spark,ns+".dataset_manifest");table.refresh();long registryPin=0;
        var snapshots=new ArrayList<org.apache.iceberg.Snapshot>();table.snapshots().forEach(snapshots::add);snapshots.sort(Comparator.comparingLong(org.apache.iceberg.Snapshot::timestampMillis));
        for(var s:snapshots) if(FeatureSnapshotWriter.read(spark,ns+".dataset_manifest",s.snapshotId(),id).filter(col("dataset_status").equalTo("EXPORTED")
                .and(col("export_manifest_sha256").equalTo(current.path("export_manifest_sha256").asText()))).count()==1) {registryPin=s.snapshotId();break;}
        require(registryPin>0 && FeatureExportJob.scalar(sql,"SELECT count(*) FROM "+ns+".dataset_manifest FOR VERSION AS OF "+registryPin
                +" WHERE dataset_id="+GoldVerificationPlan.quote(id)+" AND dataset_status='EXPORTED'")==1,"DS_EXPORT_REGISTRY_PIN_REQUIRED");
        var trusted=bundle.deepCopy().put("registry_snapshot_id",Long.toString(registryPin));var shards=trusted.putObject("collection_shards");
        Path trustedExport=safeRoot(trustedRoot.resolve("exports").resolve(id));
        immutable(trustedExport.resolve("export-checksums.sha256"),Files.readAllBytes(target.resolve("export-checksums.sha256")));
        for(var shard:plan.path("shards")) {
            lease.run();String name=shard.path("shard_id").asText();var ids=new ArrayList<String>();shard.path("mainshocks").forEach(m->ids.add(m.path("mainshock_event_id").asText()));
            var lineage=JSON.createObjectNode().put("dataset_id",id).put("collection_id",plan.path("collection_id").asText()).put("shard_id",name);
            var candidates=lineage.putArray("candidates");
            for(var row:exactFeatures.filter(col("mainshock_event_id").isin(ids.toArray())).select("mainshock_event_id","candidate_event_id","relative_time_role")
                    .orderBy("mainshock_event_id","candidate_event_id").limit((int)MAX_ROWS+1).collectAsList())
                candidates.addObject().put("mainshock_event_id",row.getString(0)).put("candidate_event_id",row.getString(1)).put("role",row.getString(2));
            require(candidates.size()==shard.path("feature_row_count").asLong() && candidates.size()<=MAX_ROWS,"COLLECTION_LINEAGE_COUNT_CHANGED");
            byte[] data=bytes(lineage);require(data.length<=64*1024*1024,"COLLECTION_LINEAGE_METADATA_LIMIT");
            immutable(trustedExport.resolve(name+".json"),data);
            shards.putObject(name).put("lineage_ref","exports/"+id+"/"+name+".json").put("lineage_sha256",WindowArtifactInput.sha(trustedExport.resolve(name+".json")))
                    .put("mainshock_count",shard.path("mainshock_count").asLong()).put("feature_row_count",shard.path("feature_row_count").asLong());
        }
        immutable(trustedRoot.resolve(id+".json"),bytes(trusted));
        return receipt.put("status","DATASET_EXPORTED").put("exported",true).put("registry_snapshot_id",Long.toString(registryPin))
                .put("dataset_manifest_ref",id+".json").put("dataset_manifest_sha256",WindowArtifactInput.sha(trustedRoot.resolve(id+".json")))
                .put("bundle_manifest_sha256",WindowArtifactInput.sha(target.resolve("dataset_manifest.json")))
                .put("checksums_sha256",WindowArtifactInput.sha(target.resolve("checksums.sha256")))
                .put("reused","EXPORTED".equals(original.path("dataset_status").asText()));
    }
}
