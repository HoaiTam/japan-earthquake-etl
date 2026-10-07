package ie212.earthquake.spark.silver;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata for a single published file within a Silver partition.
 */
public record SilverFileMetadata(
        String fileName,
        String relativePath,
        String sha256,
        long byteSize,
        int recordCount) implements Serializable {

    public SilverFileMetadata {
        Objects.requireNonNull(fileName, "fileName");
        Objects.requireNonNull(relativePath, "relativePath");
        Objects.requireNonNull(sha256, "sha256");
        if (byteSize < 0) {
            throw new IllegalArgumentException("byteSize cannot be negative: " + byteSize);
        }
        if (recordCount < 0) {
            throw new IllegalArgumentException("recordCount cannot be negative: " + recordCount);
        }
    }
}
