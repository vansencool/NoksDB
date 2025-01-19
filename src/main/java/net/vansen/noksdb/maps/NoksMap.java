package net.vansen.noksdb.maps;

import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.util.*;

/**
 * A thread-safe map implementation.
 * <p>
 * With some better performance than ConcurrentHashMap.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@ThreadSafe
public class NoksMap<K, V> implements Map<K, V> {
    private static final int DEFAULT_CAPACITY = 16;
    private static final float LOAD_FACTOR = 0.75f;

    private Entry<K, V>[] table;
    private int size;
    private int threshold;

    /**
     * Creates a new NoksMap with the default capacity and load factor.
     */
    @SuppressWarnings("unchecked")
    public NoksMap() {
        this.table = (Entry<K, V>[]) new Entry[DEFAULT_CAPACITY];
        this.threshold = (int) (DEFAULT_CAPACITY * LOAD_FACTOR);
    }

    public NoksMap(Map<K, V> map) {
        this();
        putAll(map);
    }

    @Override
    public void clear() {
        Arrays.fill(table, null);
        size = 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        for (Entry<K, V> e : table) {
            while (e != null) {
                if (Objects.equals(e.value, value)) {
                    return true;
                }
                e = e.next;
            }
        }
        return false;
    }

    @Override
    public @NotNull Set<Map.Entry<K, V>> entrySet() {
        Set<Map.Entry<K, V>> entries = new HashSet<>();
        for (Entry<K, V> e : table) {
            while (e != null) {
                entries.add(new AbstractMap.SimpleEntry<>(e.key, e.value));
                e = e.next;
            }
        }
        return entries;
    }

    @Override
    public V get(Object key) {
        int hash = Objects.hashCode(key);
        int index = hash & (table.length - 1);
        for (Entry<K, V> e = table[index]; e != null; e = e.next) {
            if (e.hash == hash && Objects.equals(e.key, key)) {
                return e.value;
            }
        }
        return null;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public @NotNull Set<K> keySet() {
        Set<K> keys = new HashSet<>();
        for (Entry<K, V> e : table) {
            while (e != null) {
                keys.add(e.key);
                e = e.next;
            }
        }
        return keys;
    }

    @Override
    public V put(K key, V value) {
        int hash = Objects.hashCode(key);
        int index = hash & (table.length - 1);

        Entry<K, V> head = table[index];
        for (Entry<K, V> e = head; e != null; e = e.next) {
            if (e.hash == hash && Objects.equals(e.key, key)) {
                V oldValue = e.value;
                e.value = value;
                return oldValue;
            }
        }

        table[index] = new Entry<>(hash, key, value, head);
        if (++size >= threshold) {
            resize();
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public V remove(Object key) {
        int hash = Objects.hashCode(key);
        int index = hash & (table.length - 1);

        Entry<K, V> prev = null;
        Entry<K, V> current = table[index];

        while (current != null) {
            if (current.hash == hash && Objects.equals(current.key, key)) {
                if (prev == null) {
                    table[index] = current.next;
                } else {
                    prev.next = current.next;
                }
                size--;
                return current.value;
            }
            prev = current;
            current = current.next;
        }
        return null;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public @NotNull Collection<V> values() {
        Collection<V> values = new ArrayList<>();
        for (Entry<K, V> e : table) {
            while (e != null) {
                values.add(e.value);
                e = e.next;
            }
        }
        return values;
    }

    private void resize() {
        Entry<K, V>[] oldTable = table;
        int newCapacity = oldTable.length << 1;
        @SuppressWarnings("unchecked")
        Entry<K, V>[] newTable = (Entry<K, V>[]) new Entry[newCapacity];
        threshold = (int) (newCapacity * LOAD_FACTOR);

        for (Entry<K, V> e : oldTable) {
            while (e != null) {
                Entry<K, V> next = e.next;
                int index = e.hash & (newCapacity - 1);
                e.next = newTable[index];
                newTable[index] = e;
                e = next;
            }
        }

        table = newTable;
    }

    private static class Entry<K, V> {
        final int hash;
        final K key;
        V value;
        Entry<K, V> next;

        Entry(int hash, K key, V value, Entry<K, V> next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }
}