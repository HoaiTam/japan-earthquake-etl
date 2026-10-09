package ie212.earthquake.spark.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/** Pure contract checks. No registry mutation, lifecycle persistence, Parquet I/O or lake writes. */
public final class ResultBundleContract implements ResultBundleValidator {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonNode CONTRACT;
    static {
        try (var stream = ResultBundleContract.class.getResourceAsStream("/ml/result-bundle-v1.json")) {
            CONTRACT = JSON.readTree(Objects.requireNonNull(stream));
        } catch (Exception error) { throw new ExceptionInInitializerError(error); }
    }
    public static JsonNode contract() { return CONTRACT.deepCopy(); }
    public static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception error) { throw new IllegalStateException(error); }
    }
    private static void require(boolean pass, String reason) {
        if (!pass) throw new IllegalArgumentException(reason);
    }
    private static boolean enumValue(String field, JsonNode value) {
        for (JsonNode item : CONTRACT.get(field)) if (item.equals(value)) return true;
        return false;
    }
    private static String str(JsonNode row, String key) { return row.path(key).asText(); }
    private static boolean missing(JsonNode row, String key) { return !row.hasNonNull(key); }
    private static void safe(JsonNode node) {
        require(node!=null,"IMP_SCHEMA_MISMATCH");
        if (node.isTextual()) {
            String value = node.asText();
            require(!value.matches("(?is).*(?:[a-z]:[\\\\/]|/Users/|/home/|://[^/]+@|[?&](?:token|password|secret|signature)=|gh[pousr]_[A-Za-z0-9]{36,}).*"), "IMP_UNSAFE_ARTIFACT_METADATA");
        } else if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                require(!entry.getKey().matches("(?i).*(?:password|access_token|secret_key|private_key|credential).*"), "IMP_UNSAFE_ARTIFACT_METADATA");
                safe(entry.getValue());
            });
        } else if (node.isArray()) node.forEach(ResultBundleContract::safe);
    }
    private static void schema(JsonNode row, String artifact) {
        JsonNode fields = CONTRACT.path("schemas").path(artifact).path("fields");
        require(row.isObject() && row.size() == fields.size(), "IMP_SCHEMA_MISMATCH");
        for (JsonNode field : fields) {
            String name = field.get("name").asText(), type = field.get("type").asText();
            require(row.has(name), "IMP_SCHEMA_MISMATCH");
            JsonNode value = row.get(name);
            if (value.isNull()) { require(field.get("nullable").asBoolean(), "IMP_SCHEMA_MISMATCH"); continue; }
            boolean valid = switch(type) {
                case "string", "json_string", "timestamp_utc" -> value.isTextual();
                case "boolean" -> value.isBoolean();
                case "long" -> value.isIntegralNumber() && value.canConvertToLong();
                case "integer" -> value.isIntegralNumber() && value.canConvertToInt();
                case "double" -> value.isNumber() && Double.isFinite(value.doubleValue());
                case "array<string>" -> value.isArray();
                default -> false;
            };
            require(valid, "IMP_SCHEMA_MISMATCH");
            if (type.equals("timestamp_utc")) {
                try { require(value.asText().endsWith("Z"), "IMP_SCHEMA_MISMATCH"); Instant.parse(value.asText()); }
                catch (java.time.format.DateTimeParseException error) { throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH"); }
            }
            if (type.equals("json_string")) {
                try { safe(JSON.readTree(value.asText())); } catch (java.io.IOException error) { throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH"); }
            }
            if (type.equals("array<string>")) value.forEach(item -> require(item.isTextual(), "IMP_SCHEMA_MISMATCH"));
        }
        safe(row);
    }
    private static void lineage(JsonNode row, Context context) {
        require(str(row,"schema_version").equals("1.0"), "IMP_SCHEMA_MISMATCH");
        require(str(row,"dataset_id").equals(context.datasetId())
            && str(row,"experiment_run_id").equals(context.runId()) && str(row,"algorithm_name").equals(context.algorithm())
            && str(row,"model_config_version").equals(context.modelVersion()), "IMP_DATASET_LINEAGE_MISMATCH");
    }
    private static String key(JsonNode row) { return str(row,"mainshock_event_id") + "\u0000" + str(row,"candidate_event_id"); }
    private static void probability(JsonNode row, String field, boolean required) {
        require(required ? !missing(row,field) && row.get(field).isNumber() && Double.isFinite(row.get(field).asDouble())
            && row.get(field).asDouble() >= 0 && row.get(field).asDouble() <= 1 : missing(row,field), "IMP_INVALID_PROBABILITY");
    }

    @Override public Receipt validate(DecodedBundle bundle, Context context) {
        require("EXPORTED".equals(context.datasetStatus()), "IMP_UNKNOWN_DATASET");
        require(enumValue("algorithms",JSON.getNodeFactory().textNode(context.algorithm())), "IMP_SCHEMA_MISMATCH");
        JsonNode success = bundle.success();
        for (JsonNode field : CONTRACT.get("success_required")) require(success.hasNonNull(field.asText()), "IMP_INCOMPLETE_BUNDLE");
        safe(success);
        require("COMPLETED".equals(str(success,"status")), "IMP_INCOMPLETE_BUNDLE");
        lineage(success,context);
        for (String version : List.of("algorithm_version","package_version")) require(success.get(version).isTextual() && !str(success,version).isBlank(), "IMP_SCHEMA_MISMATCH");
        try { require(str(success,"completed_at_utc").endsWith("Z"), "IMP_SCHEMA_MISMATCH"); Instant.parse(str(success,"completed_at_utc")); }
        catch (java.time.format.DateTimeParseException error) { throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH"); }
        require(str(success,"artifact_uri").matches("(?:s3|https)://[^?#]+"), "IMP_UNSAFE_ARTIFACT_METADATA");
        require(bundle.exactParquetSchemaVerified(), "IMP_SCHEMA_MISMATCH");
        TreeMap<String,String> hashes = new TreeMap<>();
        Set<String> allowed = new HashSet<>();
        CONTRACT.get("required_artifacts").forEach(name -> allowed.add(name.asText()));
        require(bundle.artifacts().keySet().containsAll(allowed) && success.get("checksums").isObject()
            && success.get("checksums").size()==bundle.artifacts().size(), "IMP_INCOMPLETE_BUNDLE");
        for (String name : bundle.artifacts().keySet()) {
            require(allowed.contains(name) || name.matches("optional-model-artifacts/[A-Za-z0-9_-]+(?:\\.[A-Za-z0-9_-]+)?"),"IMP_UNSAFE_ARTIFACT_METADATA");
            String hash=sha256(bundle.artifacts().get(name));
            require(success.path("checksums").path(name).isTextual() && hash.equals(success.path("checksums").path(name).asText()), "IMP_CHECKSUM_MISMATCH");
            hashes.put(name,hash);
            if (name.endsWith(".json")) {
                try {
                    JsonNode metadata=JSON.readTree(bundle.artifacts().get(name)); safe(metadata);
                    if (allowed.contains(name)) require(str(metadata,"dataset_id").equals(context.datasetId()) && str(metadata,"experiment_run_id").equals(context.runId()), "IMP_DATASET_LINEAGE_MISMATCH");
                    if (name.equals("experiment_config.json")) {
                        require(str(metadata,"algorithm_name").equals(context.algorithm()) && str(metadata,"model_config_version").equals(context.modelVersion()), "IMP_DATASET_LINEAGE_MISMATCH");
                        for (String field:List.of("algorithm_version","package_version","code_version"))
                            require(metadata.path(field).isTextual() && !str(metadata,field).isBlank() && str(metadata,field).equals(str(success,field)),"IMP_DATASET_LINEAGE_MISMATCH");
                    } else if (name.equals("experiment_metrics.json")) require(metadata.path("membership_row_count").isIntegralNumber() && metadata.path("summary_row_count").isIntegralNumber()
                        && metadata.get("membership_row_count").equals(success.get("membership_row_count")) && metadata.get("summary_row_count").equals(success.get("summary_row_count")),"IMP_SUMMARY_COUNT_MISMATCH");
                } catch (java.io.IOException error) { throw new IllegalArgumentException("IMP_SCHEMA_MISMATCH"); }
            }
            if(name.equals("requirements-lock.txt")) safe(JSON.getNodeFactory().textNode(new String(bundle.artifacts().get(name),StandardCharsets.UTF_8)));
        }
        String fingerprint=sha256((context.datasetId()+"\n"+context.runId()+"\n"+context.algorithm()+"\n"+context.modelVersion()+"\n"+hashes).getBytes(StandardCharsets.UTF_8));
        require(context.previousBundleSha256()==null || fingerprint.equals(context.previousBundleSha256()), "IMP_EXPERIMENT_RUN_REUSED");
        require(success.get("membership_row_count").isIntegralNumber() && success.get("summary_row_count").isIntegralNumber()
            && success.get("membership_row_count").asLong()==bundle.memberships().size()
            && success.get("summary_row_count").asLong()==bundle.summaries().size(), "IMP_SUMMARY_COUNT_MISMATCH");
        Map<String,Candidate> candidates=new HashMap<>();
        for (Candidate candidate:context.candidates()) require(candidates.put(candidate.mainshockId()+"\u0000"+candidate.eventId(),candidate)==null,"IMP_DUPLICATE_GRAIN");
        Map<String,List<JsonNode>> windows=new HashMap<>(); Set<String> seen=new HashSet<>();
        boolean window=context.algorithm().equals("WINDOW"), hdb=context.algorithm().startsWith("HDBSCAN");
        for (JsonNode row:bundle.memberships()) {
            schema(row,"memberships.parquet"); lineage(row,context);
            require(seen.add(key(row)),"IMP_DUPLICATE_GRAIN"); Candidate candidate=candidates.get(key(row));
            require(candidate!=null,"IMP_UNKNOWN_EVENT_ID");
            require(enumValue("roles",row.get("event_role_candidate")) && candidate.role().equals(str(row,"event_role_candidate"))
                && (candidate.mainshockId().equals(candidate.eventId())==candidate.role().equals("MAINSHOCK")),"IMP_EVENT_ROLE_MISMATCH");
            probability(row,"membership_probability",hdb);
            require(row.get("normalized_distance").asDouble()>=0,"IMP_SCHEMA_MISMATCH");
            if (window) require(missing(row,"cluster_id") && missing(row,"is_noise") && missing(row,"is_mainshock_cluster_member"),"IMP_NOISE_CLUSTER_INCONSISTENT");
            else {
                require(!missing(row,"cluster_id") && !missing(row,"is_noise") && !missing(row,"is_mainshock_cluster_member"),"IMP_NOISE_CLUSTER_INCONSISTENT");
                long label=row.get("cluster_id").asLong(); boolean noise=row.get("is_noise").asBoolean();
                require(label>=-1 && (label==-1)==noise && (!noise || !row.get("is_sequence_member").asBoolean()),"IMP_NOISE_CLUSTER_INCONSISTENT");
            }
            require(enumValue("multi_sequence_status",row.get("multi_sequence_status")),"IMP_SCHEMA_MISMATCH");
            boolean member=row.get("is_sequence_member").asBoolean(); String multi=str(row,"multi_sequence_status");
            require(member ? !multi.equals("NOT_MEMBER") : multi.equals("NOT_MEMBER"),"IMP_SCHEMA_MISMATCH");
            require(missing(row,"resolution_rank") || row.get("resolution_rank").asInt()>0,"IMP_SCHEMA_MISMATCH");
            require(!multi.equals("AMBIGUOUS") || missing(row,"resolution_rank"),"IMP_SCHEMA_MISMATCH");
            windows.computeIfAbsent(candidate.mainshockId(),ignored->new ArrayList<>()).add(row);
        }
        // Every candidate, including noise and failed-window assignments, must have a row.
        require(seen.equals(candidates.keySet()),"IMP_SUMMARY_COUNT_MISMATCH");
        Set<String> summaries=new HashSet<>();
        for (JsonNode row:bundle.summaries()) {
            schema(row,"sequence_summary.parquet"); lineage(row,context);
            String main=str(row,"mainshock_event_id"); require(summaries.add(main),"IMP_DUPLICATE_GRAIN");
            List<JsonNode> rows=windows.get(main); require(rows!=null,"IMP_UNKNOWN_EVENT_ID");
            require(row.get("candidate_count").asLong()==rows.size(),"IMP_SUMMARY_COUNT_MISMATCH");
            List<JsonNode> primary=rows.stream().filter(r->str(r,"event_role_candidate").equals("MAINSHOCK")).toList();
            require(primary.size()==1,"IMP_EVENT_ROLE_MISMATCH");
            if (!window) {
                long label=primary.get(0).get("cluster_id").asLong();
                for (JsonNode item:rows) require(item.get("is_mainshock_cluster_member").asBoolean()==(label>=0 && item.get("cluster_id").asLong()==label),"IMP_NOISE_CLUSTER_INCONSISTENT");
                if (label==-1) require(str(row,"failure_reason_code").equals("EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")
                    && rows.stream().noneMatch(item->item.get("is_sequence_member").asBoolean()),"IMP_NOISE_CLUSTER_INCONSISTENT");
            }
            String status=str(row,"window_result_status");
            require(Set.of("SUCCESS","FAILED","SKIPPED").contains(status),"IMP_SCHEMA_MISMATCH");
            require(row.get("runtime_seconds").asDouble()>=0,"IMP_SCHEMA_MISMATCH");
            if (status.equals("SUCCESS")) {
                long members=rows.stream().filter(r->r.get("is_sequence_member").asBoolean()).count();
                long post=rows.stream().filter(r->r.get("is_sequence_member").asBoolean() && str(r,"event_role_candidate").equals("POST")).count();
                long pre=rows.stream().filter(r->r.get("is_sequence_member").asBoolean() && str(r,"event_role_candidate").equals("PRE")).count();
                require(!missing(row,"sequence_member_count") && !missing(row,"aftershock_count") && !missing(row,"foreshock_candidate_count")
                    && row.get("sequence_member_count").asLong()==members && row.get("aftershock_count").asLong()==post && row.get("foreshock_candidate_count").asLong()==pre,"IMP_SUMMARY_COUNT_MISMATCH");
                require(!missing(row,"productive_sequence_detected") && row.get("productive_sequence_detected").asBoolean()==(post>0),"IMP_SUMMARY_COUNT_MISMATCH");
                if (window) require(missing(row,"cluster_count") && missing(row,"noise_count") && missing(row,"noise_ratio"),"IMP_NOISE_CLUSTER_INCONSISTENT");
                else {
                    long noise=rows.stream().filter(item->item.get("is_noise").asBoolean()).count();
                    long clusters=rows.stream().map(item->item.get("cluster_id").asLong()).filter(label->label>=0).distinct().count();
                    require(!missing(row,"noise_count") && row.get("noise_count").asLong()==noise
                        && !missing(row,"cluster_count") && row.get("cluster_count").asLong()==clusters
                        && !missing(row,"noise_ratio") && Math.abs(row.get("noise_ratio").asDouble()-(double)noise/rows.size())<1e-9,"IMP_SUMMARY_COUNT_MISMATCH");
                }
                if (hdb && members>0) {
                    double mean=rows.stream().filter(item->item.get("is_sequence_member").asBoolean()).mapToDouble(item->item.get("membership_probability").asDouble()).average().orElseThrow();
                    require(!missing(row,"mean_membership_probability") && Math.abs(row.get("mean_membership_probability").asDouble()-mean)<1e-9,"IMP_SUMMARY_COUNT_MISMATCH");
                }
            } else require(!missing(row,"failure_reason_code") && !str(row,"failure_reason_code").isBlank()
                && missing(row,"productive_sequence_detected") && missing(row,"sequence_member_count") && missing(row,"aftershock_count"),"IMP_SCHEMA_MISMATCH");
            if (!hdb) require(missing(row,"mean_membership_probability") && missing(row,"nested_membership_probability"),"IMP_INVALID_PROBABILITY");
            for (String field:List.of("noise_ratio","mean_membership_probability","nested_membership_probability"))
                if (!missing(row,field)) probability(row,field,true);
            for (String field:List.of("sequence_duration_hours","spatial_extent_km","peak_memory_mb"))
                if (!missing(row,field)) require(row.get(field).asDouble()>=0,"IMP_SCHEMA_MISMATCH");
            require(!missing(row,"metric_reason_codes") && row.get("metric_reason_codes").size()>0
                || !missing(row,"dbcv_score") && !missing(row,"omori_p") && !missing(row,"peak_memory_mb"),"IMP_SCHEMA_MISMATCH");
        }
        require(summaries.equals(windows.keySet()),"IMP_SUMMARY_COUNT_MISMATCH");
        Map<String,List<JsonNode>> eventMemberships=new HashMap<>();
        bundle.memberships().stream().filter(row->row.get("is_sequence_member").asBoolean()).forEach(row->eventMemberships.computeIfAbsent(str(row,"candidate_event_id"),ignored->new ArrayList<>()).add(row));
        for (List<JsonNode> rows:eventMemberships.values()) {
            if (rows.size()==1) require(str(rows.get(0),"multi_sequence_status").equals("UNIQUE")
                && rows.get(0).path("resolution_rank").asInt()==1,"IMP_SCHEMA_MISMATCH");
            else {
                boolean ambiguous=rows.stream().allMatch(row->str(row,"multi_sequence_status").equals("AMBIGUOUS"));
                require(ambiguous || rows.stream().allMatch(row->str(row,"multi_sequence_status").equals("MULTIPLE")),"IMP_SCHEMA_MISMATCH");
                if (!ambiguous) {
                    Set<Integer> ranks=new HashSet<>();
                    for(JsonNode row:rows) require(!missing(row,"resolution_rank") && ranks.add(row.get("resolution_rank").asInt()),"IMP_DUPLICATE_GRAIN");
                    require(ranks.contains(1),"IMP_SCHEMA_MISMATCH");
                }
            }
        }
        return new Receipt(fingerprint,"RESULT_READY");
    }

    public static void assertTransition(String from, String to, boolean scientificReview,
            Long membershipSnapshot, Long summarySnapshot) {
        boolean allowed=false;
        for (JsonNode next:CONTRACT.path("lifecycle").path(from)) if (next.asText().equals(to)) allowed=true;
        require(allowed,"IMP_INVALID_LIFECYCLE");
        if (to.equals("CANDIDATE")) require(membershipSnapshot!=null && membershipSnapshot>0 && summarySnapshot!=null && summarySnapshot>0,"IMP_COMMIT_FAILED");
        if (to.equals("APPROVED")) require(scientificReview,"IMP_SCIENTIFIC_REVIEW_REQUIRED");
    }
}
