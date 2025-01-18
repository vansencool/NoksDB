package net.vansen.noksdb.bulk;

import net.vansen.noksdb.NoksDB;
import net.vansen.noksdb.collection.DynamicObjectArrayList;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A builder class for performing multiple database operations in bulk.
 * Supports adding, updating, and deleting rows or fields, and executes all operations as a batch.
 */
@SuppressWarnings("unused")
public class BulkBuilder {
    private final NoksDB db;
    private final DynamicObjectArrayList<Runnable> operations = new DynamicObjectArrayList<>();
    private int count = -1;

    /**
     * Creates a new instance of the {@link BulkBuilder}.
     *
     * @param db The database instance.
     */
    public BulkBuilder(@NotNull NoksDB db) {
        this.db = db;
    }

    /**
     * Adds a new row to the database.
     *
     * @param key    The key of the row to add.
     * @param values An object of field names and values to associate with the row.
     * @return This {@link BulkBuilder} instance.
     */
    public BulkBuilder add(@NotNull String key, @NotNull Map<String, Object> values) {
        operations.add(count++, () -> db.store().put(key, new ConcurrentHashMap<>(values)));
        return this;
    }

    /**
     * Updates a specific field in an existing row.
     *
     * @param key   The key of the row to update.
     * @param field The field to update.
     * @param value The new value for the field.
     * @return This {@link BulkBuilder} instance.
     */
    public BulkBuilder update(@NotNull String key, @NotNull String field, @NotNull Object value) {
        operations.add(count++, () -> db.store().merge(key, Map.of(field, value), (existing, incoming) -> {
            existing.put(field, value);
            return existing;
        }));
        return this;
    }

    /**
     * Updates multiple fields in an existing row.
     *
     * @param key       The key of the row to update.
     * @param newValues An object of new field names and values to update in the row.
     * @return This {@link BulkBuilder} instance.
     */
    public BulkBuilder update(@NotNull String key, @NotNull Map<String, Object> newValues) {
        operations.add(count++, () -> db.store().merge(key, newValues, (existing, incoming) -> {
            existing.putAll(incoming);
            return existing;
        }));
        return this;
    }

    /**
     * Deletes a row from the database.
     *
     * @param key The key of the row to delete.
     * @return This {@link BulkBuilder} instance.
     */
    public BulkBuilder delete(@NotNull String key) {
        operations.add(count++, () -> db.store().remove(key));
        return this;
    }

    /**
     * Deletes a specific field from an existing row.
     *
     * @param key   The key of the row.
     * @param field The field to delete.
     * @return This {@link BulkBuilder} instance.
     */
    public BulkBuilder deleteField(@NotNull String key, @NotNull String field) {
        operations.add(count++, () -> db.store().get(key).remove(field));
        return this;
    }

    /**
     * Executes all queued operations in parallel.
     * After execution, all operations are cleared from the queue.
     */
    public void execute() {
        operations.parallelStream()
                .forEach(Runnable::run);
        db.triggerSave();
    }

    /**
     * Executes all queued operations asynchronously in parallel.
     *
     * @return A {@link CompletableFuture} representing the asynchronous execution of all operations.
     */
    public CompletableFuture<Void> executeAsync() {
        return CompletableFuture.runAsync(this::execute, db.executor());
    }
}