package org.example.serde;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AlertJacksonSerdeConsumer {
    private static final Logger log = LoggerFactory.getLogger(AlertJacksonSerdeConsumer.class);

    private volatile boolean keepConsuming = true;

    public static void main(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonSerde.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaProps.put(JacksonSerde.CONFIG_VALUE_CLASS, Alert.class.getName());

        AlertJacksonSerdeConsumer consumer = new AlertJacksonSerdeConsumer();
        consumer.consume(kafkaProps);
        Runtime.getRuntime().addShutdownHook(new Thread(consumer::shutdown));
    }

    private void shutdown() {
        keepConsuming = false;
    }

    private void consume(Properties kafkaProps) {
        KafkaConsumer<String, Alert> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("Alert2"));
        while (keepConsuming) {
            ConsumerRecords<String, Alert> consumerRecords = consumer.poll(Duration.ofMillis(250));
            System.out.println("polling records...." + consumerRecords.count());
            consumerRecords.forEach(record -> {
                commitOffset(record, consumer);
            });
        }
        consumer.close();
    }

    private void commitOffset(ConsumerRecord<String, Alert> record, KafkaConsumer<String, Alert> consumer) {

        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap();
        long offset = record.offset();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(++offset, "");
        offsetAndMetadataMap.put(new TopicPartition(record.topic(), record.partition()), offsetAndMetadata);
        consumer.commitAsync(offsetAndMetadataMap, (map, e) -> {
            if (e != null) {
                for (TopicPartition key : map.keySet()) {
                    log.info("topic {}, partition {}, offset {}, key \"{}\", value {}",
                            key.topic(),
                            key.partition(),
                            map.get(key).offset(),
                            record.key(),
                            record.value());
                }
            } else {
                for (TopicPartition key : map.keySet()) {
                    log.info("topic {}, partition {}, offset {}, key \"{}\", value {}",
                            key.topic(),
                            key.partition(),
                            map.get(key).offset(),
                            record.key(),
                            record.value());
                }
            }
        });
    }
}
