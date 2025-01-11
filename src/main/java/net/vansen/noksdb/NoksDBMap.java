package net.vansen.noksdb;

import net.vansen.noksdb.compression.Compression;
import net.vansen.noksdb.types.MapType;
import org.apache.commons.collections.FastHashMap;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NoksDBMap {
    private final File storageFile;
    private Map<String, Map<String, Object>> store;
    private final ThreadSafeFury serializer;
    private Compression compressor;
    private final boolean autoSave;
    private final boolean autoSaveAsync;
    private final ExecutorService executor;

    private NoksDBMap(NoksDBMap.Builder builder) {
        this.storageFile = builder.storageFile;
        this.autoSave = builder.autoSave;
        this.autoSaveAsync = builder.autoSaveAsync;
        this.executor = builder.executor != null ? builder.executor : Executors.newFixedThreadPool(4);
        FuryBuilder fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .withCompatibleMode(CompatibleMode.COMPATIBLE);
        this.compressor = builder.compresser;
        this.serializer = fury.buildThreadSafeFury();

        switch (builder.mapType) {
            case HASHMAP -> store = new HashMap<>();
            case LINKEDHASHMAP, LINKED_MAP -> store = new LinkedHashMap<>();
            case CONCURRENTHASHMAP -> store = new ConcurrentHashMap<>();
            case TREE_MAP -> store = new TreeMap<>();
            case IDENTITY_HASH_MAP -> store = new IdentityHashMap<>();
            case WEAK_HASH_MAP -> store = new WeakHashMap<>();
            case FAST_HASHMAP -> store = new FastHashMap();
            case FAST_MODE_FAST_HASHMAP -> {
                FastHashMap fastHashMap = new FastHashMap();
                fastHashMap.setFast(true);
                store = fastHashMap;
            }
        }
        loadDatabase();
    }

    public static NoksDBMap.Builder builder() {
        return new NoksDBMap.Builder();
    }

    public NoksDBMap.RowBuilder createRow(@NotNull String key) {
        return new NoksDBMap.RowBuilder(this, key);
    }

    public NoksDBMap.FetchBuilder fetch() {
        return new NoksDBMap.FetchBuilder(this);
    }

    public void setNullAndGc() {
        store.clear();
        store = null;
        System.gc();
    }

    public void clear() {
        store.clear();
    }

    public NoksDBMap.UpdateBuilder update(@NotNull String key) {
        return new NoksDBMap.UpdateBuilder(this, key);
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
                    int compressedLength = compressedData.length;
                    if (compressor.writeLength()) dos.writeInt(compressedLength);
                    dos.write(compressedData);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to save database", e);
                }
                return;
            }
            try (FileOutputStream fos = new FileOutputStream(storageFile)) {
                byte[] data = serializer.serialize(store);
                fos.write(data);
            } catch (IOException e) {
                throw new RuntimeException("Failed to save database", e);
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
                        byte[] decompressedData = compressor.decompress(compressedData, compressedLength);
                        store.putAll((Map<String, Map<String, Object>>) serializer.deserialize(decompressedData));
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

    public static class Builder {
        private File storageFile = new File("noksdb.dat");
        private boolean autoSave = true;
        private boolean autoSaveAsync = false;
        private Compression compresser;
        private ExecutorService executor;
        private MapType mapType = MapType.CONCURRENTHASHMAP;

        public NoksDBMap.Builder storageFile(@NotNull File file) {
            this.storageFile = file;
            return this;
        }

        public NoksDBMap.Builder autoSave(boolean autoSave) {
            this.autoSave = autoSave;
            return this;
        }

        public NoksDBMap.Builder autoSaveAsync(boolean autoSaveAsync) {
            this.autoSaveAsync = autoSaveAsync;
            return this;
        }

        public NoksDBMap.Builder executor(@NotNull ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public NoksDBMap.Builder compression(Compression compressor) {
            this.compresser = compressor;
            return this;
        }

        public NoksDBMap.Builder mapType(MapType mapType) {
            this.mapType = mapType;
            return this;
        }

        public NoksDBMap build() {
            return new NoksDBMap(this);
        }
    }

    public class RowBuilder {
        private final NoksDBMap db;
        private final String key;
        private final Map<String, Object> values = new ConcurrentHashMap<>();

        public RowBuilder(@NotNull NoksDBMap db, @NotNull String key) {
            this.db = db;
            this.key = key;
        }

        public NoksDBMap.RowBuilder value(@NotNull String field, @NotNull Object value) {
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

    public class FetchBuilder {
        private final NoksDBMap db;
        private String where;
        private String field;

        public FetchBuilder(@NotNull NoksDBMap db) {
            this.db = db;
        }

        public NoksDBMap.FetchBuilder where(@NotNull String where) {
            this.where = where;
            return this;
        }

        public NoksDBMap.FetchBuilder key(@NotNull String field) {
            this.field = field;
            return this;
        }

        public Object get() {
            Map<String, Object> record = db.store.get(where);
            if (record == null) return null;
            return field != null ? record.get(field) : record;
        }

        /**
         * Can throw exception if record is null, so its really recommended to use get instead, but for the sake of some nano benchmarks
         *
         * @return object, or a map if field is null, or even an exception if record is null
         */
        public Object getFaster() {
            Map<String, Object> record = db.store.get(where);
            return field != null ? record.get(field) : record;
        }

        public Object getOrDefault(Object defaultValue) {
            return get() != null ? get() : defaultValue;
        }

        public Object getOrDefaultFaster(Object defaultValue) {
            return getFaster() != null ? getFaster() : defaultValue;
        }

        public CompletableFuture<Object> getAsync() {
            return CompletableFuture.supplyAsync(this::get, db.executor);
        }

        public CompletableFuture<Object> getFasterAsync() {
            return CompletableFuture.supplyAsync(this::getFaster, db.executor);
        }

        public CompletableFuture<Object> getOrDefaultAsync(Object defaultValue) {
            return CompletableFuture.supplyAsync(() -> getOrDefault(defaultValue), db.executor);
        }

        public CompletableFuture<Object> getOrDefaultFasterAsync(Object defaultValue) {
            return CompletableFuture.supplyAsync(() -> getOrDefaultFaster(defaultValue), db.executor);
        }
    }

    public class UpdateBuilder {
        private final NoksDBMap db;
        private final String key;
        private final Map<String, Object> values = new ConcurrentHashMap<>();

        public UpdateBuilder(@NotNull NoksDBMap db, @NotNull String key) {
            this.db = db;
            this.key = key;
        }

        public NoksDBMap.UpdateBuilder value(@NotNull String field, @NotNull Object value) {
            values.put(field, value);
            return this;
        }

        public void apply() {
            db.store.put(key, values);
            if (db.autoSave) db.triggerSave();
        }

        public CompletableFuture<Void> applyAsync() {
            return CompletableFuture.runAsync(this::apply, db.executor);
        }
    }
}