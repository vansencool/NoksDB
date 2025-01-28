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
     * If no field is specified, the entire row is returned as a object.
     *
     * @return The value of the specified field, the entire row as a object,
     * or null if the key does not exist.
     */
    public Object get() {
        Map<String, Object> record = db.store().get(key);
        if (record == null) return null;
        return field != null ? record.get(field) : record;
    }

    /**
     * Retrieves the data from the database asynchronously.
     * If no field is specified, the entire row is returned as a object.
     *
     * @return A {@link CompletableFuture} containing the value of the specified field,
     * the entire row as a object, or null if the key does not exist.
     */
    public CompletableFuture<Object> getAsync() {
        return CompletableFuture.supplyAsync(this::get, db.executor());
    }

    /**
     * Casts the retrieved data to the specified class.
     *
     * @param clazz The class to cast the retrieved data to.
     * @param <T>   The type of the class.
     * @return The instance of the specified class.
     */
    public <T> T get(@NotNull Class<T> clazz) {
        return clazz.cast(get());
    }

    /**
     * Casts the retrieved data to the specified class asynchronously.
     *
     * @param clazz The class to cast the retrieved data to.
     * @param <T>   The type of the class.
     * @return A {@link CompletableFuture} containing the instance of the specified class.
     */
    public <T> CompletableFuture<T> getAsync(@NotNull Class<T> clazz) {
        return getAsync().thenApply(clazz::cast);
    }

    /**
     * Retrieves the data from the database.
     * If no field is specified, the entire row is returned as a object.
     * If the key does not exist, the defaultValue is returned instead.
     *
     * @param defaultValue The default value to return if the key does not exist.
     * @return The value of the specified field, the entire row as a object,
     * or the defaultValue if the key does not exist.
     */
    public Object getOrDefault(@NotNull Object defaultValue) {
        Map<String, Object> record = db.store().get(key);
        if (record == null) return defaultValue;
        return field != null ? record.get(field) : record;
    }

    /**
     * Retrieves the data from the database asynchronously.
     * If no field is specified, the entire row is returned as a object.
     * If the key does not exist, the defaultValue is returned instead.
     *
     * @param defaultValue The default value to return if the key does not exist.
     * @return A {@link CompletableFuture} containing the value of the specified field,
     * the entire row as a object, or the defaultValue if the key does not exist.
     */
    public CompletableFuture<Object> getOrDefaultAsync(@NotNull Object defaultValue) {
        return CompletableFuture.supplyAsync(() -> getOrDefault(defaultValue), db.executor());
    }

    /**
     * Casts the retrieved data to the specified class.
     * If the key does not exist, the defaultValue is returned instead.
     *
     * @param clazz        The class to cast the retrieved data to.
     * @param defaultValue The default value to return if the key does not exist.
     * @param <T>          The type of the class.
     * @return The instance of the specified class.
     */
    public <T> T getOrDefaultCast(@NotNull Class<T> clazz, @NotNull T defaultValue) {
        return clazz.cast(getOrDefault(defaultValue));
    }

    /**
     * Casts the retrieved data to the specified class asynchronously.
     * If the key does not exist, the defaultValue is returned instead.
     *
     * @param clazz        The class to cast the retrieved data to.
     * @param defaultValue The default value to return if the key does not exist.
     * @param <T>          The type of the class.
     * @return A {@link CompletableFuture} containing the instance of the specified class.
     */
    public <T> CompletableFuture<T> getOrDefaultCastAsync(@NotNull Class<T> clazz, @NotNull T defaultValue) {
        return getOrDefaultAsync(defaultValue).thenApply(clazz::cast);
    }
}