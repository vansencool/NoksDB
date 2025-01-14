package net.vansen.noksdb.collection;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * A dynamically growing ArrayList implementation.
 * <p>
 * This array list grows as needed when elements are added at high indices, which may lead to performance issues due to a large number of null elements, be sure to have a counter to add elements one-by-one to avoid null elements.
 * <p>
 * Be cautious when using this class, as improper usage may result in unexpected exceptions.
 *
 * @param <T> The type of elements in this list.
 */
@SuppressWarnings({"unused", "unchecked"})
public class DynamicObjectArrayList<T> implements Iterable<T>, Cloneable {

    private Object[] elements;
    private int size;

    /**
     * Constructs a new empty {@code DynamicObjectArrayList} with an initial capacity of 10.
     */
    public DynamicObjectArrayList() {
        elements = new Object[10];
        size = 0;
    }

    /**
     * Constructs a new empty {@code DynamicObjectArrayList} with the specified initial capacity.
     *
     * @param capacity The initial capacity of the list.
     */
    public DynamicObjectArrayList(int capacity) {
        elements = new Object[capacity];
        size = 0;
    }

    /**
     * Ensures that the internal array has sufficient capacity to accommodate the specified index.
     * If the index is greater than the current capacity, the internal array is resized.
     *
     * @param index The index to ensure capacity for.
     */
    private void ensure(int index) {
        if (index >= elements.length) {
            int newCapacity = Math.max(index + 1, elements.length * 2);
            elements = Arrays.copyOf(elements, newCapacity);
        }
        size = Math.max(size, index + 1);
    }

    /**
     * Retrieves the element at the specified index.
     *
     * @param index The index of the element to retrieve.
     * @return The element at the specified index.
     * @throws ArrayIndexOutOfBoundsException If the index is out of bounds.
     */
    public T get(int index) {
        return (T) elements[index];
    }

    /**
     * Adds a new element at the specified index.
     * The internal array grows if necessary to accommodate the new element.
     *
     * @param index The index at which to add the element.
     * @param value The value to add.
     */
    public void add(int index, T value) {
        ensure(index);
        elements[index] = value;
    }

    /**
     * Sets the element at the specified index to the given value.
     * This does not affect the size of the list.
     *
     * @param index The index to set.
     * @param value The value to set.
     */
    public void set(int index, T value) {
        elements[index] = value;
    }

    /**
     * Removes the element at the specified index.
     * Shifts subsequent elements to the left to fill the gap.
     *
     * @param index The index of the element to remove.
     */
    public void remove(int index) {
        System.arraycopy(elements, index + 1, elements, index, size - index - 1);
        elements[--size] = null;
    }

    /**
     * Removes all elements from the list.
     */
    public void clear() {
        Arrays.fill(elements, 0, size, null);
        size = 0;
    }

    /**
     * Returns the number of elements in the list.
     *
     * @return The size of the list.
     */
    public int size() {
        return size;
    }

    /**
     * Checks whether the list is empty.
     *
     * @return {@code true} if the list is empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * Converts the list to an array.
     *
     * @return An array containing all elements in the list.
     */
    public T[] toArray() {
        return (T[]) Arrays.copyOf(elements, size);
    }

    /**
     * Returns a stream of elements in the list.
     *
     * @return A sequential stream of the elements in the list.
     */
    public Stream<T> stream() {
        return Arrays.stream(toArray());
    }

    /**
     * Returns a parallel stream of elements in the list.
     *
     * @return A parallel stream of the elements in the list.
     */
    public Stream<T> parallelStream() {
        return Arrays.stream(toArray()).parallel();
    }

    /**
     * Returns a string representation of the list.
     *
     * @return A string representation of the list.
     */
    @Override
    public String toString() {
        return Arrays.toString(Arrays.copyOf(elements, size));
    }

    /**
     * Returns an iterator over the elements in the list.
     *
     * @return An iterator over the elements in the list.
     */
    @NotNull
    @Override
    public Iterator<T> iterator() {
        return Iterators.forArray(toArray());
    }

    /**
     * Clones the list.
     *
     * @return A clone of the list.
     * @throws CloneNotSupportedException If the list cannot be cloned.
     */
    @Override
    protected DynamicObjectArrayList<T> clone() throws CloneNotSupportedException {
        DynamicObjectArrayList<T> clone = (DynamicObjectArrayList<T>) super.clone();
        clone.elements = Arrays.copyOf(this.elements, this.elements.length);
        return clone;
    }
}