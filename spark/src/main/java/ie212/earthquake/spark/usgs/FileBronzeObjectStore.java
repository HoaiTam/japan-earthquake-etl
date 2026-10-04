package ie212.earthquake.spark.usgs;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** File-backed immutable object store used by local runs and deterministic tests. */
public final class FileBronzeObjectStore implements BronzeObjectStore {
    private final Path root;

    public FileBronzeObjectStore(Path root) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
    }

    @Override
    public void putIfAbsent(String key, byte[] payload, String contentType) throws IOException {
        Path target = resolve(key);
        Files.createDirectories(Objects.requireNonNull(target.getParent(), "target parent"));
        try {
            Files.write(
                    target,
                    Objects.requireNonNull(payload, "payload"),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
        } catch (FileAlreadyExistsException exception) {
            throw exception;
        }
    }

    @Override
    public byte[] read(String key) throws IOException {
        return Files.readAllBytes(resolve(key));
    }

    @Override
    public boolean exists(String key) throws IOException {
        return Files.exists(resolve(key));
    }

    @Override
    public String uriForKey(String key) {
        return resolve(key).toUri().toString();
    }

    private Path resolve(String key) {
        if (key == null || key.isBlank() || key.startsWith("/") || key.contains("..")) {
            throw new IllegalArgumentException("Bronze object key must be a safe relative path");
        }
        Path resolved = root.resolve(key).normalize();
        if (!resolved.startsWith(root)) {
            throw new IllegalArgumentException("Bronze object key escapes the store root");
        }
        return resolved;
    }
}
