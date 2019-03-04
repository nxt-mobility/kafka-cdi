package org.aerogear.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ProtoDeserializer<T> implements Deserializer<T> {
    private Class<T> type;
    private ObjectMapper mapper = new ObjectMapper();


    public ProtoDeserializer(Class<T> type) {
        this.type = type;
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
            Method parseFromMethod = type.getMethod("parseFrom", byte[].class);
            return (T) parseFromMethod.invoke(null, data);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new SerializationException("Unable to deserialize object", e);
        }
    }

    @Override
    public void close() {
    }

}
