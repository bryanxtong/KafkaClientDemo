package org.example.serde;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AlertGsonSerdeConsumer {
    private volatile boolean keepConsuming = true;
    public Properties buildKafkaConfig(){
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonSerde.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put(GsonSerde.CONFIG_VALUE_CLASS, Alert.class.getName());
        return kafkaProps;
    }

    public void consumeMessages(){
        Properties kafkaProps = this.buildKafkaConfig();
        Consumer<String, Alert> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("Alert2"));
        while (keepConsuming) {
            ConsumerRecords<String, Alert> consumerRecords = consumer.poll(Duration.ofMillis(250));
            System.out.println("polling records...." + consumerRecords.count());
            consumerRecords.forEach(record -> {
                System.out.println("key \"" + record.key() + "\" value " + record.value() + " offset " + record.offset() + " partition " + record.partition());
            });
        }
        consumer.close();
    }

    public static void main(String[] args) {
        AlertGsonSerdeConsumer consumer = new AlertGsonSerdeConsumer();
        consumer.consumeMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }

    private void shutdown() {
        keepConsuming = false;
    }
}
