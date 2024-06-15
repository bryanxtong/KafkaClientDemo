package org.example.sticky;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.serde.Alert;
import org.example.serde.JacksonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Serialize the value with Jackson library and keys are not provided
 * and It will use official BuiltInPartitioner
 */
public class AlertJacksonSerdeStickyProducer {
    private static final Logger log = LoggerFactory.getLogger(AlertJacksonSerdeStickyProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonSerde.class.getName());
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
        kafkaProps.put(ProducerConfig.RETRIES_CONFIG, "3");
        kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        String alertStatus[] = new String[]{"Critical", "Major", "Minor", "Warning"};
        Random random = new Random();
        try (Producer<String, Alert> producer = new KafkaProducer<>(kafkaProps)) {
            for (int i = 0; i < 1000; i++) {
                Alert alert = new Alert(i, "Stage " + i, alertStatus[random.nextInt(alertStatus.length)], "Stage " + i + " stopped");
                ProducerRecord<String, Alert> producerRecord = new ProducerRecord<>("Alert2", alert);
                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (null == e) {
                        System.out.println("topic " + recordMetadata.topic() + " partition " + recordMetadata.partition() + " key \"" + producerRecord.key() + "\" value " + producerRecord.value());
                    } else {
                        e.printStackTrace();
                    }
                });
            }
        }
    }
}
