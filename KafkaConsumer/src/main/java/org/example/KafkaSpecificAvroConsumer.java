package org.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaSpecificAvroConsumer {

    public static void main(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        kafkaProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        Consumer<String, Customer> consumer = new KafkaConsumer<String, Customer>(kafkaProps);
        consumer.subscribe(Collections.singleton("bRequests"));
        AtomicInteger c = new AtomicInteger(0);
        while (true) {
            int i = c.incrementAndGet();
            if (i == 50) break;
            ConsumerRecords<String, Customer> poll = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + poll.count());
            poll.forEach(record -> {
                System.out.println("key " + record.key() + " value " + record.value().toString() + " offset " + record.offset());
            });
            //consumer.commitAsync();

        }
    }
}
