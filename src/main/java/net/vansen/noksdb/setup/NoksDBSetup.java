package net.vansen.noksdb.setup;

import net.vansen.noksdb.NoksDB;
import net.vansen.noksdb.compression.Compression;
import net.vansen.noksdb.compression.CompressionBased;
import net.vansen.noksdb.database.DatabaseManager;
import net.vansen.noksdb.database.impl.DefaultDatabaseManager;
import net.vansen.noksdb.maps.MapType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 * A builder class for configuring and creating a new {@link NoksDB} instance.
 */
@SuppressWarnings("unused")
public class NoksDBSetup {

    private @NotNull File storageFile = new File("noksdb.dat");
    private boolean autoSave = true;
    private boolean autoSaveAsync = false;
    private @NotNull Compression compressor = CompressionBased.snappyOrNone();
    private boolean compressionBySerializer = false;
    private ExecutorService executor;
    private DatabaseManager databaseManager = new DefaultDatabaseManager();
    private MapType mapType = MapType.CONCURRENT_HASH_MAP;

    /**
     * Sets the file used to store the database.
     *
     * @param file The storage file.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup storageFile(@NotNull File file) {
        this.storageFile = file;
        return this;
    }

    /**
     * Enables or disables auto-saving.
     * <p>
     * By default, auto-saving is enabled.
     *
     * @param autoSave True to enable auto-saving, false to disable it.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup autoSave(boolean autoSave) {
        this.autoSave = autoSave;
        return this;
    }

    /**
     * Enables asynchronous auto-saving.
     * <p>
     * By default, asynchronous auto-saving is disabled.
     *
     * @param autoSaveAsync True to enable asynchronous auto-saving, false to disable it.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup autoSaveAsync(boolean autoSaveAsync) {
        this.autoSaveAsync = autoSaveAsync;
        return this;
    }

    /**
     * Sets whether serializer (Fury) should compress strings and numbers.
     * <p>
     * By default, compression by serializer is disabled.
     *
     * @param compressionBySerializer True if compression by serializer is enabled, false otherwise.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup compressionBySerializer(boolean compressionBySerializer) {
        this.compressionBySerializer = compressionBySerializer;
        return this;
    }

    /**
     * Sets the executor service for handling asynchronous tasks.
     * <p>
     * By default, a fixed thread pool with 4 threads is used (made in the constructor of {@link NoksDB}).
     *
     * @param executor The executor service to use.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup executor(@NotNull ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    /**
     * Sets the compression method for saving and loading the database.
     * <p>
     * By default, {@link CompressionBased#snappyOrNone()} is used.
     *
     * @param compressor The compression method to use.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup compression(@NotNull Compression compressor) {
        this.compressor = compressor;
        return this;
    }

    /**
     * Sets the database manager for managing the database.
     * <p>
     * By default, {@link DefaultDatabaseManager} is used.
     *
     * @param databaseManager The database manager to use.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup databaseManager(@NotNull DatabaseManager databaseManager) {
        this.databaseManager = databaseManager;
        return this;
    }

    /**
     * Sets the map type for the database.
     * <p>
     * By default, {@link MapType#NOKS_MAP} is used.
     *
     * @param mapType The map type to use.
     * @return This {@link NoksDBSetup} instance.
     */
    public NoksDBSetup mapType(@NotNull MapType mapType) {
        this.mapType = mapType;
        return this;
    }

    /**
     * Gets the compression method used for saving and loading the database.
     *
     * @return The compression method.
     */
    public Compression compression() {
        return compressor;
    }

    /**
     * Gets the executor service used for handling asynchronous tasks.
     *
     * @return The executor service.
     */
    public ExecutorService executor() {
        return executor;
    }

    /**
     * Gets the database manager used to manage the database.
     *
     * @return The database manager.
     */
    public DatabaseManager databaseManager() {
        return databaseManager;
    }

    /**
     * Gets the file used to store the database.
     *
     * @return The storage file.
     */
    public File storageFile() {
        return storageFile;
    }

    /**
     * Gets whether auto-saving is enabled.
     *
     * @return True if auto-saving is enabled, false otherwise.
     */
    public boolean autoSave() {
        return autoSave;
    }

    /**
     * Gets whether asynchronous auto-saving is enabled.
     *
     * @return True if asynchronous auto-saving is enabled, false otherwise.
     */
    public boolean autoSaveAsync() {
        return autoSaveAsync;
    }

    /**
     * Gets whether serializer (Fury) should compress strings and numbers.
     *
     * @return True if compression by serializer is enabled, false otherwise.
     */
    public boolean compressionBySerializer() {
        return compressionBySerializer;
    }

    /**
     * Gets the map type.
     *
     * @return The map type.
     */
    public MapType mapType() {
        return mapType;
    }

    /**
     * Builds a new {@link NoksDB} instance with the configured settings.
     *
     * @return A new {@link NoksDB} instance.
     */
    public NoksDB build() {
        return new NoksDB(this);
    }
}