package org.example;

import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * Consume Json record of a specific format
 */
public class KafkaSpecificJsonConsumer {
    private volatile boolean keepConsuming = true;

    public Properties buildKafkaConfig() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Product.class.getName());
        return kafkaProps;
    }

    public void consumeMessages() {
        Properties kafkaProps = this.buildKafkaConfig();
        Consumer<String, Product> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("JsonSpecific"));
        while (keepConsuming) {
            ConsumerRecords<String, Product> poll = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + poll.count());
            poll.forEach(record -> {
                System.out.println("key: " + record.key() + " value: " + record.value().toString() + " offset: " + record.offset());
            });
        }
        consumer.close();
    }

    private void shutdown() {
        keepConsuming = false;
    }

    public static void main(String[] args) {
        KafkaSpecificJsonConsumer consumer = new KafkaSpecificJsonConsumer();
        consumer.consumeMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));

    }
}
