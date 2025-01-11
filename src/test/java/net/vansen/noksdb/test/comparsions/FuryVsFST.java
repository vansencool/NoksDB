package net.vansen.noksdb.test.comparsions;

import org.apache.fury.Fury;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.serializers.FSTMapSerializer;

import java.util.HashMap;
import java.util.Map;

public class FuryVsFST {

    public static void main(String[] args) {
        Map<String, String> data = new HashMap<>();
        data.put("key1", "value1");
        data.put("key2", "value2");

        // Fury initialization
        long startTime = System.nanoTime();
        Fury serializer = Fury.builder()
                .build();
        long endTime = System.nanoTime();
        System.out.println("Fury initialization time: " + (endTime - startTime) + " nanoseconds");

        // FST initialization
        startTime = System.nanoTime();
        FSTConfiguration fstConfig = FSTConfiguration.createDefaultConfiguration();
        fstConfig.registerSerializer(HashMap.class, new FSTMapSerializer(), true);
        fstConfig.registerClass(Map.class);
        fstConfig.registerClass(HashMap.class);
        endTime = System.nanoTime();
        System.out.println("FST initialization time: " + (endTime - startTime) + " nanoseconds");

        // Fury serialization
        startTime = System.nanoTime();
        byte[] furySerializedData = serializer.serialize(data);
        endTime = System.nanoTime();
        System.out.println("Fury serialization time: " + (endTime - startTime) + " nanoseconds");

        // FST serialization
        startTime = System.nanoTime();
        byte[] fstSerializedData = fstConfig.asByteArray(data);
        endTime = System.nanoTime();
        System.out.println("FST serialization time: " + (endTime - startTime) + " nanoseconds");

        // Fury deserialization
        startTime = System.nanoTime();
        Map<String, String> furyDeserializedData = (Map<String, String>) serializer.deserialize(furySerializedData);
        endTime = System.nanoTime();
        System.out.println("Fury deserialization time: " + (endTime - startTime) + " nanoseconds");

        // FST deserialization
        startTime = System.nanoTime();
        Map<String, String> fstDeserializedData = (Map<String, String>) fstConfig.asObject(fstSerializedData);
        endTime = System.nanoTime();
        System.out.println("FST deserialization time: " + (endTime - startTime) + " nanoseconds");
    }
}
