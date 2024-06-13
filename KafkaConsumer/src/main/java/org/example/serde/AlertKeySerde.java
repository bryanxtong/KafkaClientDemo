package org.example.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class AlertKeySerde implements Serializer<Alert>, Deserializer<Alert> {

    @Override
    public Alert deserialize(String topic, byte[] data) {
        if(data == null) return null;
        Alert alert = new Alert();
        alert.setStageId(new String(data));
        //other fields empty
        return alert;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Alert data) {
        if (data == null) return null;
        return data.getStageId().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
