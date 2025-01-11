package net.vansen.noksdb.tests.comparsions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;

import java.util.HashMap;
import java.util.Map;

public class FuryVsKryo {

    public static void main(String[] args) {
        HashMap<String, String> data = new HashMap<>();
        for (int i = 0; i < 500000; i++) {
            data.put("key" + i, "value" + i);
        }

        // Fury initialization
        long startTime = System.nanoTime();
        Fury serializer = Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(false)
                .withClassVersionCheck(false)
                .build();
        long endTime = System.nanoTime();
        System.out.println("Fury initialization time: " + (endTime - startTime) + " nanoseconds");

        // Kryo initialization
        startTime = System.nanoTime();
        Kryo kryo = new Kryo();
        kryo.register(HashMap.class);
        kryo.register(Map.class);
        endTime = System.nanoTime();
        System.out.println("Kryo initialization time: " + (endTime - startTime) + " nanoseconds");

        // Fury serialization
        startTime = System.nanoTime();
        byte[] furySerializedData = serializer.serializeJavaObjectAndClass(data);
        endTime = System.nanoTime();
        System.out.println("Fury serialization time: " + (endTime - startTime) + " nanoseconds");

        // Kryo serialization
        startTime = System.nanoTime();
        Output output = new Output(new FastByteArrayOutputStream(), Integer.MAX_VALUE / 2);
        kryo.writeClassAndObject(output, data);
        byte[] kyroSerializedData = output.toBytes();
        endTime = System.nanoTime();
        System.out.println("Kryo serialization time: " + (endTime - startTime) + " nanoseconds");

        // Fury deserialization
        startTime = System.nanoTime();
        HashMap<String, String> furyDeserializedData = (HashMap<String, String>) serializer.deserializeJavaObjectAndClass(furySerializedData);
        endTime = System.nanoTime();
        System.out.println("Fury deserialization time: " + (endTime - startTime) + " nanoseconds");

        // Kryo deserialization
        startTime = System.nanoTime();
        Input serializedData = new Input(kyroSerializedData);
        HashMap<String, String> kryoDeserializedData = (HashMap<String, String>) kryo.readClassAndObject(serializedData);
        endTime = System.nanoTime();
        System.out.println("Kryo deserialization time: " + (endTime - startTime) + " nanoseconds");
    }
}
