package ie212.earthquake.spark.jma;

public record JmaDownloadResult(
        int year,
        String segment,
        String status,
        String archivePath,
        String statePath,
        String catalogRelease,
        String sha256,
        long contentLengthBytes,
        boolean preflightChanged,
        boolean idempotentReuse,
        String error,
        JmaHttpMetadata http,
        java.time.Instant retrievedAtUtc) {

    /** Compatibility constructor for callers that only consume download outcomes. */
    public JmaDownloadResult(int year, String segment, String status, String archivePath,
            String statePath, String catalogRelease, String sha256, long contentLengthBytes,
            boolean preflightChanged, boolean idempotentReuse, String error) {
        this(year, segment, status, archivePath, statePath, catalogRelease, sha256,
                contentLengthBytes, preflightChanged, idempotentReuse, error, null, null);
    }

    public boolean succeeded() {
        return "DOWNLOADED".equals(status) || "REUSED".equals(status);
    }
}
