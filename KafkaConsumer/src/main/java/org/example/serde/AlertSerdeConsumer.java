package org.example.serde;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AlertSerdeConsumer {
    private volatile boolean keepConsuming = true;

    public Properties buildKafkaConfig() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertKeySerde.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AlertSerde.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return kafkaProps;
    }

    public void consumeMessages() {
        Properties kafkaProps = this.buildKafkaConfig();
        Consumer<Alert, Alert> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("Alert1"));
        while (keepConsuming) {
            ConsumerRecords<Alert, Alert> consumerRecords = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + consumerRecords.count());
            consumerRecords.forEach(record -> {
                System.out.println("key " + record.key().getStageId() + " value " + record.value() + " offset " + record.offset() + " partition " + record.partition());
            });
        }
        consumer.close();
    }

    private void shutdown() {
        keepConsuming = false;
    }

    public static void main(String[] args) {
        AlertSerdeConsumer consumer = new AlertSerdeConsumer();
        consumer.consumeMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }
}
