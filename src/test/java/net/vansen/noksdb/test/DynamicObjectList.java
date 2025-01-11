package net.vansen.noksdb.test;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.text.DecimalFormat;
import java.util.Arrays;

@SuppressWarnings({"unused", "unchecked"})
public class DynamicObjectList<T> {
    private Object[] elements;
    private int size;

    public DynamicObjectList() {
        elements = new Object[10];
        size = 0;
    }

    private void ensureCapacity(int index) {
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
        ensureCapacity(index);
        elements[index] = value;
    }

    public void update(int index, T value) {
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

    @Override
    public String toString() {
        return Arrays.toString(Arrays.copyOf(elements, size));
    }

    public static void main(String[] args) {
        int[] iterationCounts = {1, 10, 100, 1000, 10000, 100000};
        int numRepeats = 1000;

        System.out.println("DynamicObjectList vs ObjectArrayList:");
        System.out.println("Time repeats: " + numRepeats);
        for (int iteration : iterationCounts) {
            System.out.println("Iteration count: " + iteration);
            long dynamicTimeTotal = 0;
            long objectTimeTotal = 0;

            for (int repeat = 0; repeat < numRepeats; repeat++) {
                DynamicObjectList<String> dynamicList = new DynamicObjectList<>();
                long startTime = System.nanoTime();
                for (int i = 1; i < iteration; i++) {
                    dynamicList.add(i, "Hello");
                    dynamicList.get(i);
                    dynamicList.remove(i);
                }
                dynamicTimeTotal += System.nanoTime() - startTime;

                ObjectArrayList<String> objectList = new ObjectArrayList<>();
                startTime = System.nanoTime();
                for (int i = 1; i < iteration; i++) {
                    while (objectList.size() <= i) {
                        objectList.add(null);
                    }
                    objectList.set(i, "Hello");
                    objectList.get(i - 1);
                    objectList.remove(i - 1);
                }
                objectTimeTotal += System.nanoTime() - startTime;
            }

            long dynamicTimeAverage = dynamicTimeTotal / numRepeats;
            long objectTimeAverage = objectTimeTotal / numRepeats;

            double speedup = (objectTimeAverage / (double) dynamicTimeAverage);
            String comparison = speedup > 1 ? "faster" : "slower";
            double percentage = Math.abs((speedup - 1) * 100);

            DecimalFormat df = new DecimalFormat("#.##");
            System.out.println("DynamicObjectList average time: " + dynamicTimeAverage + " ns");
            System.out.println("ObjectArrayList average time: " + objectTimeAverage + " ns");
            System.out.println("DynamicObjectList is " + df.format(speedup) + "x " + comparison + " than ObjectArrayList (" + df.format(percentage) + "%)");
            System.out.println();
        }
    }
}
