package org.example.serde;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class AlertSerdeProducer {
    private static final Logger log = LoggerFactory.getLogger(AlertSerdeProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AlertKeySerde.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AlertSerde.class.getName());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, "3");
        kafkaProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AlertLevelPartitioner.class.getName());
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        String alertStatus[] = new String[]{"Critical", "Major", "Minor", "Warning"};
        Random random = new Random();
        try (Producer<Alert, Alert> producer = new KafkaProducer<>(kafkaProps)) {
            for (int i = 0; i < 11; i++) {
                Alert alert = new Alert(i, "Stage " + i, alertStatus[random.nextInt(alertStatus.length)], "Stage " + i + " stopped");
                ProducerRecord<Alert, Alert> producerRecord = new ProducerRecord<>("Alert1", alert, alert);
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (null == e) {
                        System.out.println("topic " + recordMetadata.topic() + " partition " + recordMetadata.partition() + " key \"" + producerRecord.key().getStageId() + "\" value " + producerRecord.value());
                    } else {
                        e.printStackTrace();
                    }
                });
            }
        }
    }
}
