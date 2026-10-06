package ie212.earthquake.spark.silver;

import ie212.earthquake.spark.usgs.BronzeObjectStore;
import java.io.IOException;
import java.util.Objects;

/** Adapter for the configured MinIO/S3 Bronze object store. */
public final class ObjectStoreBronzeInputReader implements BronzeInputReader {
    private final BronzeObjectStore objectStore;

    public ObjectStoreBronzeInputReader(BronzeObjectStore objectStore) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    }

    @Override
    public byte[] read(String rawObjectKey, String rawObjectUri) throws IOException {
        return objectStore.read(rawObjectKey);
    }
}
