package org.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
public class KafkaGenericAvroConsumer {
    private volatile boolean keepConsuming = true;

    public Properties buildKafkaConfig() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return kafkaProps;
    }

    public void consumeMessages() {
        Properties kafkaProps = this.buildKafkaConfig();
        Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("AvroRequests"));
        while (keepConsuming) {
            ConsumerRecords<String, GenericRecord> consumerRecords = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + consumerRecords.count());
            consumerRecords.forEach(record -> {
                GenericRecord genericRecord = record.value();
                System.out.println("key " + record.key() + " value " + genericRecord.toString() + " offset " + record.offset());
            });
        }
        consumer.close();
    }

    public static void main(String[] args) {
        KafkaGenericAvroConsumer consumer = new KafkaGenericAvroConsumer();
        consumer.consumeMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }

    private void shutdown() {
        keepConsuming = false;
    }

}
