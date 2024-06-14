package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaClientConsumer {
    private volatile boolean keepConsuming = true;

    public Properties buildKafkaConfig() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return kafkaProps;
    }

    public void consumeMessages() {
        Properties kafkaProps = this.buildKafkaConfig();
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
        consumer.subscribe(Collections.singleton("bRequests"));
        while (keepConsuming) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + poll.count());
            poll.forEach(record -> System.out.println("key " + record.key() + " value " + record.value() + " offset " + record.offset()));
        }
        consumer.close();
    }

    public static void main(String[] args) {
        KafkaClientConsumer consumer = new KafkaClientConsumer();
        consumer.consumeMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }

    private void shutdown() {
        keepConsuming = false;
    }

}
