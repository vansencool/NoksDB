package net.vansen.noksdb.tests.comparsions;

import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.NotNull;

public class ListTest {

    private final int capacity = 10000000;
    private final XXHashFactory hashFactory = XXHashFactory.fastestInstance();

    public int indexOf(@NotNull String key) {
        long seed = 0x6C078965170E983BL;
        byte[] keyBytes = key.getBytes();
        long hash = hashFactory.hash64().hash(keyBytes, 0, keyBytes.length, seed);
        return Math.abs((int) (hash % capacity));
    }
}