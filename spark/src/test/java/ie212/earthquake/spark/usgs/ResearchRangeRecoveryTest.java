package ie212.earthquake.spark.usgs;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.*;
import java.util.*;
import java.util.jar.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResearchRangeRecoveryTest {
    @TempDir Path temp;
    Path jar(String name,Map<String,String> entries) throws Exception {
        var path=temp.resolve(name);
        try(var out=new JarOutputStream(Files.newOutputStream(path))) {
            for(var e:entries.entrySet()) {out.putNextEntry(new JarEntry(e.getKey()));out.write(e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8));out.closeEntry();}
        }
        return path;
    }
    @Test void recoveryCannotSilentlyChangeEtlOrRemoveOriginalCode() throws Exception {
        String publisher="ie212/earthquake/spark/gold/GoldPublisher.class";
        String recovery="ie212/earthquake/spark/usgs/ResearchRangeRecoveryJob.class";
        var before=jar("before.jar",Map.of(publisher,"old","etl.class","same"));
        ResearchRangeRecoveryJob.compatible(before,jar("allowed.jar",Map.of(publisher,"new","etl.class","same",recovery,"new")));
        assertThrows(Exception.class,()->ResearchRangeRecoveryJob.compatible(before,jar("changed.jar",Map.of(publisher,"new","etl.class","changed",recovery,"new"))));
        assertThrows(Exception.class,()->ResearchRangeRecoveryJob.compatible(before,jar("removed.jar",Map.of(publisher,"new",recovery,"new"))));
        assertThrows(Exception.class,()->ResearchRangeRecoveryJob.compatible(before,jar("extra.jar",Map.of(publisher,"new","etl.class","same","other.class","new"))));
    }
}
