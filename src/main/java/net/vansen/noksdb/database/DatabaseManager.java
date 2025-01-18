package net.vansen.noksdb.database;

import net.vansen.noksdb.compression.Compression;
import org.apache.fury.ThreadSafeFury;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Map;

/**
 * The DatabaseManager interface provides methods for loading and saving a database from/to a file.
 */
public interface DatabaseManager {

    /**
     * Loads a database from a file into a ConcurrentHashMap.
     *
     * @param file        the file to load the database from
     * @param compression the compression algorithm to use
     * @param serializer  the serializer to use
     * @param store       the ConcurrentHashMap to store the loaded data in
     */
    void load(@NotNull File file, @NotNull Compression compression, @NotNull ThreadSafeFury serializer, @NotNull Map<String, Map<String, Object>> store);

    /**
     * Saves a database from a ConcurrentHashMap to a file.
     *
     * @param file        the file to save the database to
     * @param compression the compression algorithm to use
     * @param serializer  the serializer to use
     * @param store       the ConcurrentHashMap containing the data to save
     */
    void save(@NotNull File file, @NotNull Compression compression, @NotNull ThreadSafeFury serializer, @NotNull Map<String, Map<String, Object>> store);
}