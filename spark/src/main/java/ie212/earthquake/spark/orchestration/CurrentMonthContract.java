package ie212.earthquake.spark.orchestration;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;

/** Component protocol only: never grants Gold publication, research or scheduler acceptance. */
public final class CurrentMonthContract {
    public static final String COMPONENT = "current-month-source-silver-v1";
    public static final String COVERAGE = "OBSERVED_AS_OF";
    public static final int MAX_PARTS = 64;
    public static final long MAX_BYTES = 128L * 1024 * 1024;
    public record Scope(String month, Instant start, Instant end) { }
    private CurrentMonthContract() { }

    public static void fields(JsonNode node, Set<String> expected) throws IOException {
        require(node.isObject(), "ASOF_OBJECT_REQUIRED");
        var actual = new HashSet<String>(); node.fieldNames().forEachRemaining(actual::add);
        require(actual.equals(expected), "ASOF_EXACT_FIELDS_REQUIRED");
    }
    public static String operation(JsonNode request) throws IOException {
        var run = request.path("run_context");
        return "cm1-" + sha((text(run,"dag_id") + "|" + text(run,"run_id")).getBytes(java.nio.charset.StandardCharsets.UTF_8)).substring(0,40);
    }
    public static Instant cutoff(JsonNode request) throws IOException {
        return utc(request.path("run_context"),"data_interval_end").truncatedTo(ChronoUnit.DAYS);
    }
    public static List<Scope> scopes(JsonNode request) throws IOException {
        Instant end = cutoff(request), from = end.minus(3,ChronoUnit.DAYS);
        if (from.isBefore(USGS_START)) from = USGS_START;
        var out = new ArrayList<Scope>();
        for (var cursor = YearMonth.from(from.atZone(ZoneOffset.UTC)); start(cursor.toString()).isBefore(end); cursor = cursor.plusMonths(1)) {
            Instant a = start(cursor.toString()), b = end(cursor.toString());
            out.add(new Scope(cursor.toString(),a.isBefore(USGS_START)?USGS_START:a,b.isAfter(end)?end:b));
        }
        require(!out.isEmpty() && out.size()<=2,"ASOF_MONTH_BUDGET");
        return List.copyOf(out);
    }
    public static void validate(JsonNode request, Clock clock) throws IOException {
        fields(request,Set.of("contract_version","run_context","baseline"));
        require(COMPONENT.equals(text(request,"contract_version")),"ASOF_COMPONENT_VERSION_REQUIRED");
        var run = request.path("run_context");
        fields(run,Set.of("dag_id","run_id","run_type","data_interval_start","data_interval_end","conf"));
        require("orc_02_daily_sources".equals(text(run,"dag_id")) && "scheduled".equals(text(run,"run_type"))
                && run.path("conf").isObject() && run.path("conf").isEmpty(),"ASOF_SCHEDULED_EMPTY_CONF_REQUIRED");
        String id = text(run,"run_id");
        require(id.matches("scheduled__\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d{1,6})?(?:Z|\\+00:00)"),"ASOF_RUN_ID_REQUIRED");
        Instant.parse(id.substring("scheduled__".length()));
        Instant a = utc(run,"data_interval_start"), b = utc(run,"data_interval_end");
        var time = b.atOffset(ZoneOffset.UTC).toLocalTime();
        require(Duration.between(a,b).equals(Duration.ofDays(1)) && !b.isAfter(clock.instant())
                && !time.isBefore(LocalTime.of(0,15)) && time.getSecond()==0 && time.getNano()==0,"ASOF_CLOSED_DAILY_INTERVAL_REQUIRED");
        require(cutoff(request).isAfter(USGS_START),"ASOF_TARGET_BEFORE_SEED");
        var parent = request.path("baseline");
        if (!parent.isNull()) {
            fields(parent,Set.of("operation_id","registry_sha256"));
            require(text(parent,"operation_id").matches("cm1-[a-f0-9]{40}") && !operation(request).equals(text(parent,"operation_id"))
                    && text(parent,"registry_sha256").matches("[a-f0-9]{64}"),"ASOF_EXACT_PARENT_REQUIRED");
        }
        require(JSON.writeValueAsBytes(request).length<=32768,"ASOF_BOUNDED_REQUEST_REQUIRED");
        scopes(request);
    }
    public static ObjectNode flags() {
        var out = JSON.createObjectNode(); out.put("contract_version",COMPONENT);
        out.put("coverage_mode",COVERAGE); out.put("publication_quality","PROVISIONAL");
        out.put("source_policy","USGS_OPERATIONAL_WITH_JMA_HISTORICAL_BASELINE");
        out.putArray("required_event_sources").add("USGS"); out.put("jma_role","HISTORICAL_READINESS_ONLY");
        out.put("published",false); out.put("research_ready",false); out.put("ml_upstream_eligible",false);
        return out;
    }
}
