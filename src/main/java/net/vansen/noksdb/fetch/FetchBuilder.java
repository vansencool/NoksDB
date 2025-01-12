package net.vansen.noksdb.fetch;

import net.vansen.noksdb.NoksDB;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A builder class for fetching data from the database.
 * Allows retrieving entire rows or specific fields from a row.
 */
@SuppressWarnings("unused")
public class FetchBuilder {
    private final NoksDB db;
    private final String key;
    private String field;

    /**
     * Creates a new instance of the {@link FetchBuilder}.
     *
     * @param db  The database instance.
     * @param key The key of the row to fetch.
     */
    public FetchBuilder(@NotNull NoksDB db, @NotNull String key) {
        this.db = db;
        this.key = key;
    }

    /**
     * Specifies the field to retrieve from the row.
     *
     * @param field The name of the field to fetch.
     * @return This {@link FetchBuilder} instance.
     */
    public FetchBuilder field(@NotNull String field) {
        this.field = field;
        return this;
    }

    /**
     * Retrieves the data from the database.
     * If no field is specified, the entire row is returned as a map.
     *
     * @return The value of the specified field, the entire row as a map,
     * or null if the key does not exist.
     */
    public Object get() {
        Map<String, Object> record = db.store().get(key);
        if (record == null) return null;
        return field != null ? record.get(field) : record;
    }

    /**
     * Retrieves the data from the database asynchronously.
     * If no field is specified, the entire row is returned as a map.
     *
     * @return A {@link CompletableFuture} containing the value of the specified field,
     * the entire row as a map, or null if the key does not exist.
     */
    public CompletableFuture<Object> getAsync() {
        return CompletableFuture.supplyAsync(this::get, db.executor());
    }
}