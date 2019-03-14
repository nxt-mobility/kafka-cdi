package org.aerogear.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ProtoDeserializer<T extends GeneratedMessageV3> implements Deserializer<T> {
    private Class<T> type;
    private Method parseFromMethod;


    public ProtoDeserializer(Class<T> type) {
        this.type = type;
        try {
            parseFromMethod = type.getMethod("parseFrom", byte[].class);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(type.toString() + " does not have required method parseFrom(byte[])", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return type.cast(parseFromMethod.invoke(null, (Object)data));
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new SerializationException("Unable to deserialize object", e);
        }
    }

    @Override
    public void close() {
    }

}
