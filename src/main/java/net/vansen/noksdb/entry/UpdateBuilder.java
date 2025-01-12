package net.vansen.noksdb.entry;

import net.vansen.noksdb.NoksDB;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A builder class for updating data in the database.
 * Allows modifying or adding fields in an existing row.
 */
@SuppressWarnings("unused")
public class UpdateBuilder {
    private final NoksDB db;
    private final String key;
    private final Map<String, Object> values = new ConcurrentHashMap<>();

    /**
     * Creates a new instance of the {@link UpdateBuilder}.
     *
     * @param db  The database instance.
     * @param key The key of the row to update.
     */
    public UpdateBuilder(@NotNull NoksDB db, @NotNull String key) {
        this.db = db;
        this.key = key;
    }

    /**
     * Adds or updates a field in the row with the specified value.
     *
     * @param field The name of the field to update.
     * @param value The value to set for the field.
     * @return This {@link UpdateBuilder} instance.
     */
    public UpdateBuilder value(@NotNull String field, @NotNull Object value) {
        values.put(field, value);
        return this;
    }

    /**
     * Applies the updates to the database.
     * Updates are merged with the existing row, and new fields are added if they don't exist.
     */
    public void apply() {
        db.store().merge(key, values, (existing, newValues) -> {
            existing.putAll(newValues);
            return existing;
        });
        db.triggerSave();
    }

    /**
     * Applies the updates to the database asynchronously.
     * Updates are merged with the existing row, and new fields are added if they don't exist.
     *
     * @return A {@link CompletableFuture} representing the asynchronous operation.
     */
    public CompletableFuture<Void> applyAsync() {
        return CompletableFuture.runAsync(this::apply, db.executor());
    }
}