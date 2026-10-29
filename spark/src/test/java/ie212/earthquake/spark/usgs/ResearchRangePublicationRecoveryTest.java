package ie212.earthquake.spark.usgs;

import static org.junit.jupiter.api.Assertions.*;
import static ie212.earthquake.spark.orchestration.FullMonthContract.*;
import java.nio.file.*;
import java.util.*;
import java.util.jar.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchRangePublicationRecoveryTest {
    @TempDir Path temp;
    Path jar(String name,Map<String,String> entries) throws Exception {
        var path=temp.resolve(name);try(var out=new JarOutputStream(Files.newOutputStream(path))) {
            for(var e:entries.entrySet()) {out.putNextEntry(new JarEntry(e.getKey()));out.write(e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));out.closeEntry();}
        }return path;
    }
    @Test void overlayCannotChangeEvenPublisherTransportOrManifest() throws Exception {
        var entries=Map.of("META-INF/MANIFEST.MF","old","GoldPublisher.class","same","TrinoSqlClient.class","same","etl.class","same");
        var before=jar("before.jar",entries);var good=new HashMap<>(entries);good.put(ResearchRangePublicationRecovery.ENTRY,"new");
        ResearchRangePublicationRecovery.compatible(before,jar("good.jar",good));
        for(String name:entries.keySet()) {
            var changed=new HashMap<>(good);changed.put(name,"changed");
            assertThrows(Exception.class,()->ResearchRangePublicationRecovery.compatible(before,jar("changed.jar",changed)));
        }
        var extra=new HashMap<>(good);extra.put("Other.class","bad");
        assertThrows(Exception.class,()->ResearchRangePublicationRecovery.compatible(before,jar("extra.jar",extra)));
        var removed=new HashMap<>(good);removed.remove("etl.class");
        assertThrows(Exception.class,()->ResearchRangePublicationRecovery.compatible(before,jar("removed.jar",removed)));
    }
    @Test void trangPendingBatchIsAnExactPrefixAfter33NotAnAssumedSingleMonth() throws Exception {
        var r=ResearchRangeStateTest.request("rosy179");var ctx=JSON.createObjectNode();
        ctx.putObject("resolver_request").set("target_months",JSON.valueToTree(List.of("2021-07","2021-08","2021-09")));
        assertEquals(List.of("2021-07","2021-08","2021-09"),ResearchRangePublicationRecovery.targets(r,33,ctx));
        for(var t:List.of(List.of("2021-08"),List.of("2021-07","2021-09"),List.of("2024-01"),List.<String>of())) {
            ctx.with("resolver_request").set("target_months",JSON.valueToTree(t));
            assertThrows(Exception.class,()->ResearchRangePublicationRecovery.targets(r,33,ctx));
        }
        assertThrows(Exception.class,()->ResearchRangePublicationRecovery.targets(r,0,ctx));
        assertThrows(Exception.class,()->ResearchRangePublicationRecovery.targets(r,63,ctx));
    }
    @Test void originalFailureEvidenceCannotBeEditedOrEscapeRoot() throws Exception {
        temp=temp.toRealPath();
        var pins=JSON.createObjectNode();for(int i=0;i<12;i++) {
            var file=temp.resolve("file"+i);Files.writeString(file,"original");pins.put(file.getFileName().toString(),sha(Files.readAllBytes(file)));
        }
        ResearchRangePublicationRecovery.unchanged(temp,pins);
        Files.writeString(temp.resolve("file0"),"edited");
        assertThrows(Exception.class,()->ResearchRangePublicationRecovery.unchanged(temp,pins));
        pins.remove("file0");pins.put("../secret","a".repeat(64));
        assertThrows(Exception.class,()->ResearchRangePublicationRecovery.unchanged(temp,pins));
    }
}
