package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Generic Json Consumer
 */
public class KafkaGenricJsonSchemaConsumer {
    private volatile boolean keepConsuming = true;

    public Properties buildKafkaConfig() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put("json.fail.invalid.schema", true);
        return kafkaProps;
    }

    public void consumeMessages() {
        Properties kafkaProps = this.buildKafkaConfig();
        Consumer<String, JsonNode> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("JsonSchemaGeneric"));
        ObjectMapper mapper = new ObjectMapper();
        while (keepConsuming) {
            ConsumerRecords<String, JsonNode> poll = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + poll.count());
            poll.forEach(record -> {
                JsonNode product = mapper.convertValue(record.value(), new TypeReference<>() {
                });
                System.out.println("key: " + record.key() + " value: " + product.toString() + " offset: " + record.offset());
            });
        }
        consumer.close();
    }

    private void shutdown() {
        keepConsuming = false;
    }

    public static void main(String[] args) {
        KafkaGenricJsonSchemaConsumer consumer = new KafkaGenricJsonSchemaConsumer();
        consumer.consumeMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }
}
