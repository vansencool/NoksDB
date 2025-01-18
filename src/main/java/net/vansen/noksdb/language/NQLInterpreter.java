package net.vansen.noksdb.language;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import net.vansen.noksdb.NoksDB;
import net.vansen.noksdb.entry.UpdateBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An interpreter for SQL-like queries.
 * <p>
 * Allows very similar syntax to SQL.
 */
@SuppressWarnings("unused")
public class NQLInterpreter {

    private final NoksDB db;

    /**
     * Constructs an instance of the interpreter for the provided NoksDB database.
     *
     * @param db The database instance.
     */
    public NQLInterpreter(@NotNull NoksDB db) {
        this.db = db;
    }

    /**
     * Executes a SQL-like query.
     *
     * @param query The query string.
     * @return The result of the query, or "-" if the query doesn't return anything.
     */
    @Nullable
    @CanIgnoreReturnValue
    public Object execute(@NotNull String query) {
        String[] tokens = query.trim().split("\\s+", 2);
        String command = tokens[0].toUpperCase();

        return switch (command) {
            case "INSERT" -> handleInsert(tokens[1]);
            case "SELECT" -> handleSelect(tokens[1]);
            case "UPDATE" -> handleUpdate(tokens[1]);
            case "DELETE" -> handleDelete(tokens[1]);
            default -> throw new IllegalArgumentException("Unknown command: " + command);
        };
    }

    private Object handleInsert(@NotNull String query) {
        String[] parts = query.split("\\s+", 3);
        if (!"INTO".equalsIgnoreCase(parts[0])) {
            throw new IllegalArgumentException("Invalid INSERT syntax.");
        }

        db.store().put(parts[1], parseFields(parts[2].replaceAll("[()]", "")));
        return null;
    }

    private Object handleSelect(@NotNull String query) {
        String[] parts = query.split("\\s+");
        if (parts.length < 3 || !"FROM".equalsIgnoreCase(parts[1])) {
            throw new IllegalArgumentException("Invalid SELECT syntax.");
        }

        return db.fetch(parts[2]).field(parts[0]).get();
    }

    private Object handleUpdate(@NotNull String query) {
        query = query.replaceAll("[()]", "");
        String[] parts = query.split("\\s+");
        if (!"SET".equalsIgnoreCase(parts[1])) {
            throw new IllegalArgumentException("Invalid UPDATE syntax.");
        }

        StringBuilder fields = new StringBuilder();
        for (int i = 2; i < parts.length; i++) {
            fields.append(parts[i]).append(" ");
        }
        fields = new StringBuilder(fields.toString().trim());

        UpdateBuilder updateBuilder = db.update(parts[0]);
        parseFields(fields.toString()).forEach(updateBuilder::value);
        updateBuilder.apply();
        return "-";
    }

    private Object handleDelete(@NotNull String query) {
        String[] parts = query.split("\\s+", 3);

        if ("FROM".equalsIgnoreCase(parts[0])) {
            db.delete(parts[1]);
        } else {
            db.deleteField(parts[2], parts[0]);
        }

        return null;
    }

    private Map<String, Object> parseFields(@NotNull String fields) {
        Map<String, Object> result = new ConcurrentHashMap<>();
        for (String pair : fields.replaceAll("\\s+", "").split(",")) {
            String[] keyValue = pair.split("=");
            result.put(keyValue[0], keyValue[1]);
        }
        return result;
    }
}