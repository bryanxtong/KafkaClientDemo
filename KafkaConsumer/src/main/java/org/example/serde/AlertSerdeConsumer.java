package org.example.serde;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class AlertSerdeConsumer {

    public static void main(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AlertKeySerde.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AlertSerde.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        Consumer<Alert, Alert> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("Alert1"));
        AtomicInteger c = new AtomicInteger(0);
        while (true) {
            int i = c.incrementAndGet();
            if (i == 50) break;
            ConsumerRecords<Alert, Alert> consumerRecords = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + consumerRecords.count());
            consumerRecords.forEach(record -> {
                System.out.println("key " + record.key().getStageId() + " value " + record.value() + " offset " + record.offset() + " partition " + record.partition());
            });
            //consumer.commitAsync();
        }
        consumer.close();
    }
}
