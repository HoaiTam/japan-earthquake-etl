package ie212.earthquake.spark.silver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/** Offline/local adapter used by Spark tests and local staging. */
public final class PathBronzeInputReader implements BronzeInputReader {
    private final Path bronzeRoot;

    public PathBronzeInputReader(Path bronzeRoot) {
        this.bronzeRoot = Objects.requireNonNull(bronzeRoot, "bronzeRoot").toAbsolutePath().normalize();
    }

    @Override
    public byte[] read(String rawObjectKey, String rawObjectUri) throws IOException {
        if (rawObjectKey == null || rawObjectKey.isBlank()) {
            throw new IOException("manifest raw_object_key is blank");
        }
        Path candidate = bronzeRoot.resolve(rawObjectKey).normalize();
        if (!candidate.startsWith(bronzeRoot) || !Files.isRegularFile(candidate)) {
            throw new IOException("Bronze object is not available under the configured root");
        }
        return Files.readAllBytes(candidate);
    }
}
