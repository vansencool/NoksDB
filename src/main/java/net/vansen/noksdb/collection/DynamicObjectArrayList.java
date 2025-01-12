package net.vansen.noksdb.collection;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * An ArrayList that can grow dynamically.
 * Dynamically growing means that the array can grow as needed, at any index.
 * <p>
 * But be warned of it, having a high index can cause the array to grow very quickly, which means many null elements, which is bad for performance.
 * <p>
 * This class can possibly throw many exceptions without a clear reason.
 * So be careful when using it.
 *
 * @param <T> the type of the elements
 */
@SuppressWarnings({"unused", "unchecked"})
public class DynamicObjectArrayList<T> implements Iterable<T> {
    private Object[] elements;
    private int size;

    public DynamicObjectArrayList() {
        elements = new Object[10];
        size = 0;
    }

    public DynamicObjectArrayList(int capacity) {
        elements = new Object[capacity];
        size = 0;
    }

    private void ensure(int index) {
        if (index >= elements.length) {
            int newCapacity = Math.max(index + 1, elements.length * 2);
            elements = Arrays.copyOf(elements, newCapacity);
        }
        size = Math.max(size, index + 1);
    }

    public T get(int index) {
        return (T) elements[index];
    }

    public void add(int index, T value) {
        ensure(index);
        elements[index] = value;
    }

    /**
     * Adds an element at the given index.
     * <p>
     * This is faster than the other add method, but is highly NOT recommended to use, as it does not check for capacity.
     *
     * @param index the index to add the element at
     * @param value the element to add
     */
    public void addFaster(int index, T value) {
        elements[index] = value;
        size = Math.max(size, index + 1);
    }

    public void set(int index, T value) {
        elements[index] = value;
    }

    public void remove(int index) {
        System.arraycopy(elements, index + 1, elements, index, size - index - 1);
        elements[--size] = null;
    }

    public void clear() {
        Arrays.fill(elements, 0, size, null);
        size = 0;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public T[] toArray() {
        return (T[]) elements;
    }

    public Stream<T> stream() {
        return Arrays.stream(toArray());
    }

    public Stream<T> parallelStream() {
        return Arrays.stream(toArray()).parallel();
    }

    @Override
    public String toString() {
        return Arrays.toString(Arrays.copyOf(elements, size));
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return Iterators.forArray(toArray());
    }
}