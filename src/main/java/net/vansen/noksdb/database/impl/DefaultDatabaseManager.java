package net.vansen.noksdb.database.impl;

import net.vansen.noksdb.compression.Compression;
import net.vansen.noksdb.database.DatabaseManager;
import org.apache.fury.ThreadSafeFury;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A default implementation of the DatabaseManager interface.
 */
@SuppressWarnings({"unchecked", "ResultOfMethodCallIgnored"})
public class DefaultDatabaseManager implements DatabaseManager {

    @Override
    public void load(@NotNull File file, @NotNull Compression compression, @NotNull ThreadSafeFury serializer, @NotNull ConcurrentHashMap<String, Map<String, Object>> store) {
        if (!file.exists()) return;
        try {
            if (!compression.writeLength()) {
                try (FileInputStream fis = new FileInputStream(file)) {
                    byte[] data = fis.readAllBytes();
                    if (data.length > 0) {
                        byte[] decompressedData = compression.decompress(data, data.length);
                        store.putAll((Map<String, Map<String, Object>>) serializer.deserialize(decompressedData));
                    }
                }
            } else {
                try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
                    int compressedLength = dis.readInt();
                    byte[] compressedData = new byte[compressedLength];
                    dis.readFully(compressedData);
                    store.putAll((Map<String, Map<String, Object>>) serializer.deserialize(compression.decompress(compressedData, compressedLength)));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load database", e);
        }
    }

    @Override
    public void save(@NotNull File file, @NotNull Compression compression, @NotNull ThreadSafeFury serializer, @NotNull ConcurrentHashMap<String, Map<String, Object>> store) {
        if (!file.exists()) file.getParentFile().mkdirs();
        try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(file))) {
            byte[] compressedData = compression.compress(serializer.serialize(store));
            if (compression.writeLength()) dos.writeInt(compressedData.length);
            dos.write(compressedData);
        } catch (Exception e) {
            throw new RuntimeException("Failed to save database", e);
        }
    }
}
