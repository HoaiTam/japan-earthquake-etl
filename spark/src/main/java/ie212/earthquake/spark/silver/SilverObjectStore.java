package ie212.earthquake.spark.silver;

import java.io.IOException;
import java.util.List;

/**
 * Storage contract for Silver dataset partitions and markers (CON-03 1.0, SLV-08).
 * Supports atomic file operations, partition promotion, and clean prefix cleanup.
 */
public interface SilverObjectStore {

    /**
     * Writes or overwrites an object at the specified relative key.
     */
    void put(String key, byte[] content, String contentType) throws IOException;

    /**
     * Reads the entire byte content of the specified key.
     */
    byte[] read(String key) throws IOException;

    /**
     * Checks whether an object exists at the specified relative key.
     */
    boolean exists(String key) throws IOException;

    /**
     * Lists relative keys under the given prefix.
     */
    List<String> list(String prefix) throws IOException;

    /**
     * Deletes a single object at the specified relative key.
     */
    void delete(String key) throws IOException;

    /**
     * Recursively deletes all objects under the specified prefix.
     */
    void deletePrefix(String prefix) throws IOException;

    /**
     * Moves an object from sourceKey to targetKey (used for atomic staging promotion).
     */
    void move(String sourceKey, String targetKey) throws IOException;

    /**
     * Resolves a relative key to its full storage URI.
     */
    String uriForKey(String key);
}
