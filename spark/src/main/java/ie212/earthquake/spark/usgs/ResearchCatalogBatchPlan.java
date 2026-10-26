package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;

/** Planning bound only. The resolver still enforces its unchanged 100k actual-selected-row guard. */
public final class ResearchCatalogBatchPlan {
    public static final String VERSION="dat03-catalog-batches-v2";
    public static final long PROFILE_ROWS=75_000;
    public static final int SHUFFLE_PARTITIONS=16;
    public static final long START_RESERVE_SECONDS=3600;
    public static boolean mayStart(long remainingSeconds) {return remainingSeconds>=START_RESERVE_SECONDS;}
    private ResearchCatalogBatchPlan() { }
    public static List<String> next(JsonNode plan,int completed,int remainingMonths) throws Exception {
        require(completed>=0 && completed<=288 && remainingMonths>=0 && remainingMonths<=288,"CATALOG_BATCH_CURSOR_INVALID");
        require("dat03-rollout-plan-v1".equals(plan.path("version").asText()) && plan.path("batches").isArray()
                && plan.path("batches").size()==288,"CATALOG_BATCH_PLAN_REQUIRED");
        var result=new ArrayList<String>();long total=0;String split=null;
        for(int index=completed;index<288 && result.size()<Math.min(12,remainingMonths);index++) {
            var row=plan.path("batches").get(index);String month=text(row,"month"),s=index<225?"REPRODUCTION":"EXTENSION";
            require(ResearchCatalogState.MONTHS.get(index).equals(month) && s.equals(text(row,"split"))
                    && row.path("profile_accepted_rows").isIntegralNumber() && row.path("profile_accepted_rows").canConvertToLong(),"CATALOG_BATCH_PLAN_CHANGED");
            long count=row.path("profile_accepted_rows").asLong();require(count>=0 && count<=PROFILE_ROWS,"CATALOG_BATCH_MONTH_NEEDS_CAPACITY_REVIEW");
            if(split!=null && (!split.equals(s) || total+count>PROFILE_ROWS))break;
            result.add(month);total=Math.addExact(total,count);split=s;
        }
        return List.copyOf(result);
    }
}
