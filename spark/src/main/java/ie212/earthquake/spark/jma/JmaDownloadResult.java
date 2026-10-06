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
        String error) {

    public boolean succeeded() {
        return "DOWNLOADED".equals(status) || "REUSED".equals(status);
    }
}
