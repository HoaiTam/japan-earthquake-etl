package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Deterministic comparator for JMA catalog releases (CON-03, SLV-06).
 * Orders catalog releases by normalized inventory order / version / timestamp
 * without arbitrary string sorting.
 */
public final class JmaReleaseComparator implements Comparator<String>, Serializable {

    private static final Pattern RELEASE_V_PATTERN = Pattern.compile("^(?:release[-_]?)?v?(\\d+)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("(?:lm|retrieved)[-_](\\d{8}T\\d{6}Z)", Pattern.CASE_INSENSITIVE);
    private static final Pattern DOTTED_VERSION_PATTERN = Pattern.compile("^v?(\\d+(?:\\.\\d+)+)$", Pattern.CASE_INSENSITIVE);

    private final Map<String, Integer> inventoryOrder;

    public JmaReleaseComparator() {
        this.inventoryOrder = Map.of();
    }

    public JmaReleaseComparator(List<String> orderedReleases) {
        Objects.requireNonNull(orderedReleases, "orderedReleases");
        Map<String, Integer> order = new HashMap<>();
        for (int i = 0; i < orderedReleases.size(); i++) {
            String release = orderedReleases.get(i);
            if (release != null && !release.isBlank()) {
                order.put(release.trim().toLowerCase(), i);
            }
        }
        this.inventoryOrder = Map.copyOf(order);
    }

    @Override
    public int compare(String r1, String r2) {
        if (Objects.equals(r1, r2)) {
            return 0;
        }
        if (r1 == null) {
            return -1;
        }
        if (r2 == null) {
            return 1;
        }

        String s1 = r1.trim();
        String s2 = r2.trim();
        if (s1.equalsIgnoreCase(s2)) {
            return 0;
        }

        // 1. Explicit inventory order
        if (!inventoryOrder.isEmpty()) {
            Integer idx1 = inventoryOrder.get(s1.toLowerCase());
            Integer idx2 = inventoryOrder.get(s2.toLowerCase());
            if (idx1 != null && idx2 != null) {
                return Integer.compare(idx1, idx2);
            }
            if (idx1 != null) {
                return 1;
            }
            if (idx2 != null) {
                return -1;
            }
        }

        // 2. Timestamp in release name (e.g. jma-lm-20160819T034858Z-... vs jma-lm-20230901T120000Z-...)
        Matcher ts1 = TIMESTAMP_PATTERN.matcher(s1);
        Matcher ts2 = TIMESTAMP_PATTERN.matcher(s2);
        if (ts1.find() && ts2.find()) {
            int tsCompare = ts1.group(1).compareTo(ts2.group(1));
            if (tsCompare != 0) {
                return tsCompare;
            }
        }

        // 3. Simple version number (e.g. release-v1 vs release-v2, v1 vs v2, 1 vs 2)
        Matcher v1 = RELEASE_V_PATTERN.matcher(s1);
        Matcher v2 = RELEASE_V_PATTERN.matcher(s2);
        if (v1.matches() && v2.matches()) {
            try {
                long num1 = Long.parseLong(v1.group(1));
                long num2 = Long.parseLong(v2.group(1));
                int numCompare = Long.compare(num1, num2);
                if (numCompare != 0) {
                    return numCompare;
                }
                return 0;
            } catch (NumberFormatException ignored) {
                // fallback to dotted/string
            }
        }

        // 4. Dotted version (e.g. 1.0 vs 2.1)
        Matcher dot1 = DOTTED_VERSION_PATTERN.matcher(s1);
        Matcher dot2 = DOTTED_VERSION_PATTERN.matcher(s2);
        if (dot1.matches() && dot2.matches()) {
            int dotCompare = compareDottedVersions(dot1.group(1), dot2.group(1));
            if (dotCompare != 0) {
                return dotCompare;
            }
            return 0;
        }

        // 5. Stable fallback
        int caseInsensitive = s1.compareToIgnoreCase(s2);
        if (caseInsensitive != 0) {
            return caseInsensitive;
        }
        return s1.compareTo(s2);
    }

    private static int compareDottedVersions(String v1, String v2) {
        String[] parts1 = v1.split("\\.");
        String[] parts2 = v2.split("\\.");
        int length = Math.max(parts1.length, parts2.length);
        for (int i = 0; i < length; i++) {
            long p1 = (i < parts1.length) ? parsePart(parts1[i]) : 0;
            long p2 = (i < parts2.length) ? parsePart(parts2[i]) : 0;
            if (p1 != p2) {
                return Long.compare(p1, p2);
            }
        }
        return 0;
    }

    private static long parsePart(String part) {
        try {
            return Long.parseLong(part);
        } catch (NumberFormatException ex) {
            return 0;
        }
    }
}
