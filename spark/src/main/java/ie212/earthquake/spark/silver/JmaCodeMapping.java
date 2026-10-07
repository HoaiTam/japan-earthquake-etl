package ie212.earthquake.spark.silver;

import java.util.Map;
import java.util.Set;

/** Versioned native code mapping; case is significant, particularly D/d, V/v and K/k. */
public final class JmaCodeMapping {
    public static final String VERSION = "jma-codes-v1";
    public static final Set<String> AGENCIES = Set.of("J", "U", "I");
    public static final Map<String, String> EVENT_TYPES = Map.of(
            "1", "EARTHQUAKE", "2", "EARTHQUAKE", "3", "ARTIFICIAL",
            "4", "ERUPTION", "5", "EARTHQUAKE");
    public static final Map<String, String> MAGNITUDE_TYPES = Map.of(
            "J", "local-office", "D", "displacement", "d", "displacement-two-stations",
            "V", "velocity", "v", "velocity-few-stations", "W", "moment",
            "B", "body-wave", "S", "surface-wave");
    public static final Set<String> INTENSITY_CODES = Set.of(
            "1", "2", "3", "4", "5", "6", "7", "A", "B", "C", "D",
            "R", "M", "S", "L", "F", "X");
    public static final Set<String> TSUNAMI_CODES = Set.of("1", "2", "3", "4", "5", "6", "T");
    public static final Map<String, String> DETERMINATION_FLAGS = Map.of(
            "K", "manual-high", "S", "manual-low-examined", "k", "manual-middle",
            "s", "manual-low", "A", "automatic-middle", "a", "automatic-low",
            "N", "unresolved-or-fixed", "F", "distant");

    private JmaCodeMapping() {
    }

    public static String eventType(String nativeCode) {
        return nativeCode == null ? "UNKNOWN" : EVENT_TYPES.getOrDefault(nativeCode, "UNKNOWN");
    }

    /** Blank/unknown is not evidence of no tsunami; JMA does not define a false code here. */
    public static Boolean tsunamiFlag(String nativeCode) {
        return nativeCode != null && TSUNAMI_CODES.contains(nativeCode) ? Boolean.TRUE : null;
    }
}
