package ie212.earthquake.spark.gold;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import ie212.earthquake.spark.orchestration.CurrentMonthContract;
import java.util.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;

/** Reserved namespace plus persisted snapshot policy, never a caller eligibility switch. */
public final class OperationalGoldPolicy {
    public static final String VERSION = "current-month-gold-v1";
    public static final String SCHEMA = "operational_current_month";
    public static final String PROPERTY = "gold.operational_policy_sha256";
    private OperationalGoldPolicy() { }
    public static boolean reserved(String name) {
        return name != null && Arrays.asList(name.split("\\.")).contains(SCHEMA);
    }
    public static void researchOnly(String tableOrNamespace) {
        if (reserved(tableOrNamespace)) throw new IllegalArgumentException("PROVISIONAL_GOLD_ML_FORBIDDEN");
    }
    public static void validate(String namespace, JsonNode policy) {
        try { validateChecked(namespace,policy); }
        catch(java.io.IOException error) { throw new IllegalArgumentException(error.getMessage(),error); }
    }
    private static void validateChecked(String namespace, JsonNode policy) throws java.io.IOException {
        require(namespace != null && namespace.matches("[a-z][a-z0-9_]*\\."+SCHEMA),"ASOF_RESERVED_GOLD_NAMESPACE_REQUIRED");
        CurrentMonthContract.fields(policy,Set.of("contract_version","coverage_mode","publication_quality","source_policy",
            "required_event_sources","jma_role","research_ready","ml_upstream_eligible","registry_sha256","silver_receipt_sha256",
            "event_cutoff_utc","observation_cutoff_utc","month_coverage"));
        require(VERSION.equals(text(policy,"contract_version")),"ASOF_GOLD_POLICY_REQUIRED");
        var flags=CurrentMonthContract.flags();
        for (String key:List.of("coverage_mode","publication_quality","source_policy","required_event_sources","jma_role",
                "research_ready","ml_upstream_eligible")) require(flags.path(key).equals(policy.path(key)),"ASOF_GOLD_POLICY_PROMOTION_BLOCKED");
        for(String key:List.of("registry_sha256","silver_receipt_sha256"))
            require(text(policy,key).matches("[a-f0-9]{64}"),"ASOF_EXACT_SILVER_PINS_REQUIRED");
        require(!utc(policy,"observation_cutoff_utc").isBefore(utc(policy,"event_cutoff_utc")),"ASOF_GOLD_CUTOFF_INVALID");
        require(policy.path("month_coverage").isObject() && !policy.path("month_coverage").isEmpty(),"ASOF_GOLD_COVERAGE_REQUIRED");
    }
    public static String sha(JsonNode policy) {
        try { return digest(policy); }
        catch(java.io.IOException error) { throw new IllegalArgumentException("ASOF_GOLD_POLICY_INVALID",error); }
    }
    public static void attach(ObjectNode target,JsonNode policy) {
        target.set("operational_policy",policy.deepCopy());
        for(String key:List.of("coverage_mode","publication_quality","source_policy","research_ready","ml_upstream_eligible"))
            target.set(key,policy.path(key));
    }
    /** Exact pin remains independently classified even if a caller forges a PASSED metadata row. */
    public static String researchSnapshotPredicate() {
        return " AND element_at(summary,'"+PROPERTY+"') IS NULL";
    }
}
