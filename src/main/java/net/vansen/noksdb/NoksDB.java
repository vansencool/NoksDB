package net.vansen.noksdb;

import net.vansen.noksdb.bulk.BulkBuilder;
import net.vansen.noksdb.compression.Compression;
import net.vansen.noksdb.database.DatabaseManager;
import net.vansen.noksdb.entry.RowBuilder;
import net.vansen.noksdb.entry.UpdateBuilder;
import net.vansen.noksdb.fetch.FetchBuilder;
import net.vansen.noksdb.language.NQLInterpreter;
import net.vansen.noksdb.maps.NoksMap;
import net.vansen.noksdb.setup.NoksDBSetup;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.logging.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * A lightweight, thread-safe key-value database with support for compression and auto-saving.
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
public class NoksDB {

    private final File storageFile;
    private Map<String, Map<String, Object>> store;
    private final ThreadSafeFury serializer;
    private final Compression compressor;
    private final boolean autoSave;
    private final boolean autoSaveAsync;
    private final ExecutorService executor;
    private final DatabaseManager databaseManager;

    /**
     * Creates a new instance of the NoksDB database.
     *
     * @param builder A {@link NoksDBSetup} object containing the configuration for the database.
     */
    @SuppressWarnings("all")
    public NoksDB(@NotNull NoksDBSetup builder) {
        this.storageFile = builder.storageFile();
        this.autoSave = builder.autoSave();
        this.autoSaveAsync = builder.autoSaveAsync();
        this.executor = builder.executor() != null ? builder.executor() : Executors.newFixedThreadPool(4);
        LoggerFactory.disableLogging();
        FuryBuilder fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .withAsyncCompilation(true)
                .requireClassRegistration(builder.requireClassRegistration())
                .suppressClassRegistrationWarnings(true)
                .withNumberCompressed(builder.compressionBySerializer())
                .withStringCompressed(builder.compressionBySerializer())
                .withCompatibleMode(CompatibleMode.COMPATIBLE);
        this.serializer = fury.buildThreadSafeFury();
        this.compressor = builder.compression();
        this.databaseManager = builder.databaseManager();
        switch (builder.mapType()) {
            case CONCURRENT_HASH_MAP -> this.store = new ConcurrentHashMap<>();
            case NOKS_MAP -> this.store = new NoksMap<>();
            case HASH_MAP -> this.store = new HashMap<>();
        }
        databaseManager.load(storageFile, compressor, serializer, store);
    }

    /**
     * Starts building a new NoksDB database instance with the default setup.
     *
     * @return A new {@link NoksDBSetup} object for configuring the database.
     */
    public static NoksDBSetup builder() {
        return new NoksDBSetup();
    }

    /**
     * Creates a new row for the specified key.
     *
     * @param key The key to associate with the row.
     * @return A {@link RowBuilder} object to add values to the row.
     */
    public RowBuilder rowOf(@NotNull String key) {
        return new RowBuilder(this, key);
    }

    /**
     * Fetches an existing row by its key.
     *
     * @param key The key of the row to fetch.
     * @return A {@link FetchBuilder} object to retrieve data from the row.
     */
    public FetchBuilder fetch(@NotNull String key) {
        return new FetchBuilder(this, key);
    }

    /**
     * Starts a bulk operation for adding or removing multiple rows at once.
     *
     * @return A {@link BulkBuilder} object for performing bulk operations.
     */
    public BulkBuilder bulk() {
        return new BulkBuilder(this);
    }

    /**
     * Updates an existing row by its key.
     *
     * @param key The key of the row to update.
     * @return An {@link UpdateBuilder} object to modify the row's values.
     */
    public UpdateBuilder update(@NotNull String key) {
        return new UpdateBuilder(this, key);
    }

    /**
     * Deletes a row from the database.
     *
     * @param key The key of the row to delete.
     */
    public void delete(@NotNull String key) {
        store.remove(key);
        triggerSave();
    }

    /**
     * Deletes a row asynchronously.
     *
     * @param key The key of the row to delete.
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> deleteAsync(@NotNull String key) {
        return CompletableFuture.runAsync(() -> delete(key), executor);
    }

    /**
     * Returns an interpreter for NQL queries.
     * NQL is a query language for NoksDB. Allowing SQL-like queries.
     *
     * @return An interpreter for NQL queries.
     * @see NQLInterpreter
     */
    public NQLInterpreter nql() {
        return new NQLInterpreter(this);
    }

    /**
     * Deletes a specific field within a row.
     *
     * @param key   The key of the row containing the field.
     * @param field The name of the field to delete.
     */
    public void deleteField(@NotNull String key, @NotNull String field) {
        store.get(key).remove(field);
        triggerSave();
    }

    /**
     * Checks if a field exists within a row.
     *
     * @param key   The key of the row containing the field.
     * @param field The name of the field to check.
     * @return True if the field exists, false otherwise.
     */
    public boolean existsField(@NotNull String key, @NotNull String field) {
        return store.get(key).containsKey(field);
    }

    /**
     * Deletes a specific field asynchronously.
     *
     * @param key   The key of the row containing the field.
     * @param field The name of the field to delete.
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> deleteFieldAsync(@NotNull String key, @NotNull String field) {
        return CompletableFuture.runAsync(() -> deleteField(key, field), executor);
    }

    /**
     * Saves the database to the file.
     */
    public void save() {
        databaseManager.save(storageFile, compressor, serializer, store);
    }

    /**
     * Saves the database asynchronously.
     *
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> saveAsync() {
        return CompletableFuture.runAsync(this::save, executor);
    }

    /**
     * Checks if a row exists in the database.
     *
     * @param key The key of the row to check.
     * @return True if the row exists, otherwise false.
     */
    public boolean exists(@NotNull String key) {
        return store.containsKey(key);
    }

    /**
     * Counts the total number of rows in the database.
     *
     * @return The total number of rows.
     */
    public long count() {
        return store.size();
    }

    /**
     * Clears all rows from the database.
     */
    public void clear() {
        store.clear();
    }

    /**
     * Clears the database and immediately saves it to the file.
     */
    public void clearAndSave() {
        clear();
        save();
    }

    /**
     * Adds multiple rows to the database in bulk.
     *
     * @param entries An object of keys and their associated row data to add.
     */
    public void bulkAdd(Map<String, Map<String, Object>> entries) {
        entries.entrySet().parallelStream().forEach(entry -> store.put(entry.getKey(), entry.getValue()));
        triggerSave();
    }

    /**
     * Removes multiple rows from the database in bulk.
     *
     * @param keys A set of keys to remove.
     */
    public void bulkRemove(Set<String> keys) {
        keys.parallelStream().forEach(store::remove);
        triggerSave();
    }

    /**
     * Retrieves the internal store of the database.
     *
     * @return The object representing the database's rows and their data.
     */
    public Map<String, Map<String, Object>> store() {
        return store;
    }

    /**
     * Retrieves the executor service used for asynchronous tasks.
     *
     * @return The {@link ExecutorService} used by the database.
     */
    public Executor executor() {
        return executor;
    }

    /**
     * Adds multiple rows to the database asynchronously in bulk.
     *
     * @param entries An object of keys and their associated row data to add.
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> bulkAddAsync(Map<String, Map<String, Object>> entries) {
        return CompletableFuture.runAsync(() -> bulkAdd(entries), executor);
    }

    /**
     * Removes multiple rows from the database asynchronously in bulk.
     *
     * @param keys A set of keys to remove.
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> bulkRemoveAsync(Set<String> keys) {
        return CompletableFuture.runAsync(() -> bulkRemove(keys), executor);
    }

    /**
     * Closes the database and shuts down the executor.
     */
    public void close() {
        executor.shutdown();
        setNullAndGc();
    }

    /**
     * A set of all keys in the database.
     *
     * @return A set of keys.
     */
    public Set<String> keys() {
        return store.keySet();
    }

    /**
     * All values in the database.
     *
     * @return A collection of values in the database.
     */
    public Collection<Map<String, Object>> values() {
        return store.values();
    }

    /**
     * All entries in the database.
     *
     * @return A collection of entries in the database.
     */
    public Set<Map.Entry<String, Map<String, Object>>> entrySet() {
        return store.entrySet();
    }

    /**
     * Triggers a save operation if auto-save is enabled.
     */
    public void triggerSave() {
        if (!autoSave) return;
        if (autoSaveAsync) {
            saveAsync();
        } else {
            save();
        }
    }

    /**
     * Clears the internal store and triggers garbage collection.
     */
    public void setNullAndGc() {
        store.clear();
        store = null;
        System.gc();
    }
}