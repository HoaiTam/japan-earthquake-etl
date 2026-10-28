package ie212.earthquake.spark.usgs;

import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import ie212.earthquake.spark.jma.*;
import java.nio.file.*;
import java.util.*;

/** Offline original-byte/provenance verification; never constructs Spark or a store. */
public final class ResearchRangeInputCheck {
    private ResearchRangeInputCheck() { }
    private static String rawSha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    public static void main(String[] args) throws Exception {
        require(args.length==1,"RANGE_INPUT_ROOT_REQUIRED");
        var root=ResearchRangeState.safe(Path.of(args[0]));
        var r=ResearchRangeState.read(root.resolve("request.json"),65536);ResearchRangeState.validate(r);
        var inventory=new ArrayList<JmaArchiveEntry>();
        for(String name:List.of("baseline.csv","boundary.csv")) {
            require(rawSha(Files.readAllBytes(ResearchRangeState.safe(root.resolve(name))))
                    .equals(text(r,name.equals("baseline.csv")?"baseline_inventory_sha256":"boundary_inventory_sha256")),"RANGE_INVENTORY_CHANGED");
            inventory.addAll(JmaArchiveInventory.read(root.resolve(name)));
        }
        var summary=ResearchRangeState.read(root.resolve("usgs-summary.json"),4*1024*1024);
        require(rawSha(Files.readAllBytes(root.resolve("usgs-summary.json"))).equals(text(r,"usgs_summary_sha256")),"RANGE_SUMMARY_CHANGED");
        for(var pin:r.path("sources")) {
            String name=text(pin,"name");var metaFile=ResearchRangeState.safe(root.resolve("inputs/"+name+".json"));
            var rawFile=ResearchRangeState.safe(root.resolve("inputs/"+name+(name.startsWith("h")?".zip":".geojson")));
            var meta=ResearchRangeState.read(metaFile,65536);
            require(Files.isRegularFile(rawFile) && Files.size(rawFile)==pin.path("bytes").asLong(),"RANGE_RAW_BOUND");
            byte[] raw=Files.readAllBytes(rawFile);
            require(rawSha(Files.readAllBytes(metaFile)).equals(text(pin,"metadata_sha256"))
                    && rawSha(raw).equals(text(pin,"raw_sha256")) && rawSha(raw).equals(text(meta,"sha256"))
                    && meta.path("bytes").isIntegralNumber() && meta.path("bytes").asLong()==raw.length
                    && !utc(meta,"retrieved_at_utc").isAfter(utc(r,"source_cutoff_utc")),"RANGE_SOURCE_PROVENANCE_CHANGED");
            if(name.startsWith("h")) {
                int year=Integer.parseInt(name.substring(1));
                var entry=inventory.stream().filter(x->x.year()==year && x.segment().equals("full-year")).findFirst().orElseThrow();
                require(entry.sourceUrl().toString().equals(text(meta,"final_url")) && meta.path("http_status").isInt()
                        && meta.path("http_status").asInt()==200 && meta.path("inventory_metadata_drift").isBoolean()
                        && !meta.path("inventory_metadata_drift").asBoolean(),"RANGE_JMA_PROVENANCE_INVALID");
                require(text(meta,"catalog_release").matches("[A-Za-z0-9][A-Za-z0-9_.~-]{0,255}")
                        && !text(meta,"catalog_release").contains(".."),"RANGE_JMA_RELEASE_INVALID");
            } else {
                var checked=ResearchUsgsCaptureCheck.check(meta,raw);int matches=0;
                for(var row:summary.path("captures"))if(row.path("month").equals(checked.path("month"))) {
                    require(checked.equals(row.path("check")),"RANGE_USGS_SUMMARY_CHANGED");matches++;
                }
                require(matches==1,"RANGE_EXACT_USGS_SUMMARY_REQUIRED");
            }
        }
        System.out.println("RANGE_ORIGINAL_37_SOURCES_VERIFIED; no Bronze/Silver/Gold writes; research_ready=false");
    }
}
