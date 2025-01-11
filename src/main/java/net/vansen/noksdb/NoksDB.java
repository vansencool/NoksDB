package net.vansen.noksdb;

import net.vansen.noksdb.compression.Compression;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings({"unused", "unchecked"})
public class NoksDB {
    private final File storageFile;
    private Map<String, Map<String, Object>> store = new ConcurrentHashMap<>();
    private final ThreadSafeFury serializer;
    private final Compression compressor;
    private final boolean autoSave;
    private final boolean autoSaveAsync;
    private final ExecutorService executor;

    private NoksDB(NoksDB.Builder builder) {
        this.storageFile = builder.storageFile;
        this.autoSave = builder.autoSave;
        this.autoSaveAsync = builder.autoSaveAsync;
        this.executor = builder.executor != null ? builder.executor : Executors.newFixedThreadPool(4);
        FuryBuilder fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .withCompatibleMode(CompatibleMode.COMPATIBLE);
        this.compressor = builder.compressor;
        this.serializer = fury.buildThreadSafeFury();
        loadDatabase();
    }

    public static NoksDB.Builder builder() {
        return new NoksDB.Builder();
    }

    public NoksDB.RowBuilder rowOf(@NotNull String key) {
        return new RowBuilder(this, key);
    }

    public NoksDB.FetchBuilder fetch(@NotNull String key) {
        return new FetchBuilder(this, key);
    }

    public NoksDB.BulkBuilder bulk() {
        return new BulkBuilder(this);
    }

    public NoksDB.UpdateBuilder update(@NotNull String key) {
        return new UpdateBuilder(this, key);
    }

    public void delete(@NotNull String key) {
        store.remove(key);
        if (autoSave) triggerSave();
    }

    public CompletableFuture<Void> deleteAsync(@NotNull String key) {
        return CompletableFuture.runAsync(() -> delete(key), executor);
    }

    public void save() {
        saveDatabase();
    }

    public CompletableFuture<Void> saveAsync() {
        return CompletableFuture.runAsync(this::saveDatabase, executor);
    }

    public boolean exists(@NotNull String key) {
        return store.containsKey(key);
    }

    public long count() {
        return store.size();
    }

    public void bulkAdd(Map<String, Map<String, Object>> entries) {
        entries.entrySet().parallelStream().forEach(entry -> store.put(entry.getKey(), entry.getValue()));
        if (autoSave) triggerSave();
    }

    public void bulkRemove(Set<String> keys) {
        keys.parallelStream().forEach(store::remove);
        if (autoSave) triggerSave();
    }

    public CompletableFuture<Void> bulkAddAsync(Map<String, Map<String, Object>> entries) {
        return CompletableFuture.runAsync(() -> bulkAdd(entries), executor);
    }

    public CompletableFuture<Void> bulkRemoveAsync(Set<String> keys) {
        return CompletableFuture.runAsync(() -> bulkRemove(keys), executor);
    }

    public void close() {
        executor.shutdown();
        setNullAndGc();
    }

    private void triggerSave() {
        if (autoSaveAsync) {
            saveAsync();
        } else {
            save();
        }
    }

    private void saveDatabase() {
        try {
            if (compressor != null) {
                try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(storageFile))) {
                    byte[] compressedData = compressor.compress(serializer.serialize(store));
                    if (compressor.writeLength()) dos.writeInt(compressedData.length);
                    dos.write(compressedData);
                }
                return;
            }
            try (FileOutputStream fos = new FileOutputStream(storageFile)) {
                byte[] data = serializer.serialize(store);
                fos.write(data);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to save database", e);
        }
    }

    private void loadDatabase() {
        if (!storageFile.exists()) return;
        try {
            if (compressor != null) {
                if (!compressor.writeLength()) {
                    try (FileInputStream fis = new FileInputStream(storageFile)) {
                        byte[] data = fis.readAllBytes();
                        if (data.length > 0) {
                            byte[] decompressedData = compressor.decompress(data, data.length);
                            store.putAll((Map<String, Map<String, Object>>) serializer.deserialize(decompressedData));
                        }
                    }
                } else {
                    try (DataInputStream dis = new DataInputStream(new FileInputStream(storageFile))) {
                        int compressedLength = dis.readInt();
                        byte[] compressedData = new byte[compressedLength];
                        dis.readFully(compressedData);
                        store.putAll((Map<String, Map<String, Object>>) serializer.deserialize(compressor.decompress(compressedData, compressedLength)));
                    }
                }
            } else {
                try (FileInputStream fis = new FileInputStream(storageFile)) {
                    byte[] data = fis.readAllBytes();
                    if (data.length > 0) store.putAll((Map<String, Map<String, Object>>) serializer.deserialize(data));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load database", e);
        }
    }

    public void setNullAndGc() {
        store.clear();
        store = null;
        System.gc();
    }

    public static class Builder {
        private File storageFile = new File("noksdb.dat");
        private boolean autoSave = true;
        private boolean autoSaveAsync = false;
        private Compression compressor;
        private ExecutorService executor;

        public NoksDB.Builder storageFile(@NotNull File file) {
            this.storageFile = file;
            return this;
        }

        public NoksDB.Builder autoSave(boolean autoSave) {
            this.autoSave = autoSave;
            return this;
        }

        public NoksDB.Builder autoSaveAsync(boolean autoSaveAsync) {
            this.autoSaveAsync = autoSaveAsync;
            return this;
        }

        public NoksDB.Builder executor(@NotNull ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public NoksDB.Builder compression(Compression compressor) {
            this.compressor = compressor;
            return this;
        }

        public NoksDB build() {
            return new NoksDB(this);
        }
    }

    public static class RowBuilder {
        private final NoksDB db;
        private final String key;
        private final Map<String, Object> values = new ConcurrentHashMap<>();

        public RowBuilder(@NotNull NoksDB db, @NotNull String key) {
            this.db = db;
            this.key = key;
        }

        public NoksDB.RowBuilder value(@NotNull String field, @NotNull Object value) {
            values.put(field, value);
            return this;
        }

        public void insert() {
            db.store.put(key, values);
            if (db.autoSave) db.triggerSave();
        }

        public CompletableFuture<Void> insertAsync() {
            return CompletableFuture.runAsync(this::insert, db.executor);
        }
    }

    public static class FetchBuilder {
        private final NoksDB db;
        private final String key;
        private String field;

        public FetchBuilder(@NotNull NoksDB db, @NotNull String key) {
            this.db = db;
            this.key = key;
        }

        public NoksDB.FetchBuilder field(@NotNull String field) {
            this.field = field;
            return this;
        }

        public Object get() {
            Map<String, Object> record = db.store.get(key);
            if (record == null) return null;
            return field != null ? record.get(field) : record;
        }

        public CompletableFuture<Object> getAsync() {
            return CompletableFuture.supplyAsync(this::get, db.executor);
        }
    }

    public static class UpdateBuilder {
        private final NoksDB db;
        private final String key;
        private final Map<String, Object> values = new ConcurrentHashMap<>();

        public UpdateBuilder(@NotNull NoksDB db, @NotNull String key) {
            this.db = db;
            this.key = key;
        }

        public NoksDB.UpdateBuilder value(@NotNull String field, @NotNull Object value) {
            values.put(field, value);
            return this;
        }

        public void apply() {
            db.store.merge(key, values, (existing, newValues) -> {
                existing.putAll(newValues);
                return existing;
            });
            if (db.autoSave) db.triggerSave();
        }

        public CompletableFuture<Void> applyAsync() {
            return CompletableFuture.runAsync(this::apply, db.executor);
        }
    }

    public static class BulkBuilder {
        private final NoksDB db;
        private final List<Runnable> operations = new ArrayList<>();

        public BulkBuilder(@NotNull NoksDB db) {
            this.db = db;
        }

        public BulkBuilder add(@NotNull String key, @NotNull Map<String, Object> values) {
            operations.add(() -> db.store.put(key, new ConcurrentHashMap<>(values)));
            return this;
        }

        public BulkBuilder update(@NotNull String key, @NotNull String field, @NotNull Object value) {
            operations.add(() -> db.store.merge(key, Map.of(field, value), (existing, incoming) -> {
                existing.put(field, value);
                return existing;
            }));
            return this;
        }

        public BulkBuilder update(@NotNull String key, @NotNull Map<String, Object> newValues) {
            operations.add(() -> db.store.merge(key, newValues, (existing, incoming) -> {
                existing.putAll(incoming);
                return existing;
            }));
            return this;
        }

        public BulkBuilder delete(@NotNull String key) {
            operations.add(() -> db.store.remove(key));
            return this;
        }

        public BulkBuilder deleteWhere(@NotNull String field, @NotNull Object value) {
            operations.add(() -> db.store.entrySet().removeIf(entry -> {
                Map<String, Object> record = entry.getValue();
                return record != null && value.equals(record.get(field));
            }));
            return this;
        }

        public void execute() {
            operations.parallelStream()
                    .forEach(Runnable::run);
            if (db.autoSave) db.triggerSave();
        }

        public CompletableFuture<Void> executeAsync() {
            return CompletableFuture.runAsync(this::execute, db.executor);
        }
    }
}