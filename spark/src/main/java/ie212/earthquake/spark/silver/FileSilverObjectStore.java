package ie212.earthquake.spark.silver;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * File-backed implementation of SilverObjectStore for local execution, tests, and debugging.
 */
public final class FileSilverObjectStore implements SilverObjectStore {

    private final Path root;

    public FileSilverObjectStore(Path root) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
    }

    public Path root() {
        return root;
    }

    @Override
    public void put(String key, byte[] content, String contentType) throws IOException {
        Path target = resolve(key);
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tempFile = Files.createTempFile(parent, ".tmp_", ".part");
        try {
            Files.write(tempFile, Objects.requireNonNull(content, "content"));
            try {
                Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(tempFile);
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
    public List<String> list(String prefix) throws IOException {
        Path start = resolve(prefix != null ? prefix : "");
        if (!Files.exists(start)) {
            return List.of();
        }
        List<String> results = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(start)) {
            stream.filter(Files::isRegularFile).forEach(p -> {
                String rel = root.relativize(p).toString().replace('\\', '/');
                results.add(rel);
            });
        }
        Collections.sort(results);
        return results;
    }

    @Override
    public void delete(String key) throws IOException {
        Files.deleteIfExists(resolve(key));
    }

    @Override
    public void deletePrefix(String prefix) throws IOException {
        Path target = resolve(prefix);
        if (!Files.exists(target)) {
            return;
        }
        Files.walkFileTree(target, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) throw exc;
                Files.deleteIfExists(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @Override
    public void move(String sourceKey, String targetKey) throws IOException {
        Path source = resolve(sourceKey);
        Path target = resolve(targetKey);
        Path parent = target.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ex) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    @Override
    public String uriForKey(String key) {
        return resolve(key).toUri().toString();
    }

    private Path resolve(String key) {
        if (key == null) {
            return root;
        }
        String clean = key.trim().replace('\\', '/');
        while (clean.startsWith("/")) {
            clean = clean.substring(1);
        }
        if (clean.contains("..")) {
            throw new IllegalArgumentException("Silver object key contains invalid '..' path segment: " + key);
        }
        Path resolved = root.resolve(clean).normalize();
        if (!resolved.startsWith(root)) {
            throw new IllegalArgumentException("Silver object key escapes store root: " + key);
        }
        return resolved;
    }
}
