package net.vansen.noksdb.entry;

import net.vansen.noksdb.NoksDB;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A builder class for creating new rows in the database.
 * Allows adding fields and values to the row before inserting it into the database.
 */
@SuppressWarnings("unused")
public class RowBuilder {
    private final NoksDB db;
    private final String key;
    private final Map<String, Object> values = new ConcurrentHashMap<>();

    /**
     * Creates a new instance of the {@link RowBuilder}.
     *
     * @param db  The database instance.
     * @param key The key of the row to create.
     */
    public RowBuilder(@NotNull NoksDB db, @NotNull String key) {
        this.db = db;
        this.key = key;
    }

    /**
     * Adds a field and its value to the row.
     *
     * @param field The name of the field to add.
     * @param value The value to associate with the field.
     * @return This {@link RowBuilder} instance.
     */
    public RowBuilder value(@NotNull String field, @NotNull Object value) {
        values.put(field, value);
        return this;
    }

    /**
     * Inserts the row into the database.
     * <p>
     * Overwrites the row if it already exists.
     */
    public void insert() {
        db.store().put(key, values);
        db.triggerSave();
    }

    /**
     * Inserts the row into the database asynchronously.
     * <p>
     * Overwrites the row if it already exists.
     *
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> insertAsync() {
        return CompletableFuture.runAsync(this::insert, db.executor());
    }

    /**
     * Inserts the row into the database if it doesn't already exist.
     */
    public void insertIfNotExists() {
        db.store().putIfAbsent(key, values);
        db.triggerSave();
    }

    /**
     * Inserts the row into the database asynchronously if it doesn't already exist.
     *
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> insertIfNotExistsAsync() {
        return CompletableFuture.runAsync(this::insertIfNotExists, db.executor());
    }
}