package org.example.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Customize the serializer and deserializer
 */
public class AlertSerde implements Serializer<Alert>, Deserializer<Alert> {
    @Override
    public byte[] serialize(String topic, Alert data) {
        byte[] serializedName1, serializedName2, serializedName3;
        int stringSize1, stringSize2, stringSize3;
        if (data == null) return null;
        else {
            if (data.getStageId() != null) {
                serializedName1 = data.getStageId().getBytes(StandardCharsets.UTF_8);
                stringSize1 = serializedName1.length;
            } else {
                serializedName1 = new byte[0];
                stringSize1 = 0;
            }

            if (data.getAlertLevel() != null) {
                serializedName2 = data.getAlertLevel().getBytes(StandardCharsets.UTF_8);
                stringSize2 = serializedName2.length;
            } else {
                serializedName2 = new byte[0];
                stringSize2 = 0;
            }

            if (data.getAlertMessage() != null) {
                serializedName3 = data.getAlertMessage().getBytes(StandardCharsets.UTF_8);
                stringSize3 = serializedName3.length;
            } else {
                serializedName3 = new byte[0];
                stringSize3 = 0;
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize1 + 4 + stringSize2 + 4 + stringSize3);
            buffer.putInt(data.getAlertId());
            buffer.putInt(stringSize1);
            buffer.put(serializedName1);
            buffer.putInt(stringSize2);
            buffer.put(serializedName2);
            buffer.putInt(stringSize3);
            buffer.put(serializedName3);
            return buffer.array();
        }
    }

    @Override
    public Alert deserialize(String topic, byte[] data) {
        if (data == null) return null;
        if (data.length < 8)
            throw new SerializationException("Size of data received by deserializer is shorter than expected");
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int alertId = buffer.getInt();
        int fieldSize = buffer.getInt();
        byte[] fieldBytes = new byte[fieldSize];
        buffer.get(fieldBytes);
        String alertStageId = new String(fieldBytes, StandardCharsets.UTF_8);

        fieldSize = buffer.getInt();
        fieldBytes = new byte[fieldSize];
        buffer.get(fieldBytes);
        String alertLevel = new String(fieldBytes, StandardCharsets.UTF_8);

        fieldSize = buffer.getInt();
        fieldBytes = new byte[fieldSize];
        buffer.get(fieldBytes);
        String alertMessage = new String(fieldBytes, StandardCharsets.UTF_8);
        return new Alert(alertId, alertStageId, alertLevel, alertMessage);
    }

    @Override
    public Alert deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public Alert deserialize(String topic, Headers headers, ByteBuffer data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }


    @Override
    public void close() {
        Serializer.super.close();
    }
}
