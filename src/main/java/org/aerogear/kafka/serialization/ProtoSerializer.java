package org.aerogear.kafka.serialization;

import com.google.protobuf.GeneratedMessageV3;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ProtoSerializer<T extends  GeneratedMessageV3> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, GeneratedMessageV3 msg) {
        return msg.toByteArray();
    }

    @Override
    public void close() {

    }
}
