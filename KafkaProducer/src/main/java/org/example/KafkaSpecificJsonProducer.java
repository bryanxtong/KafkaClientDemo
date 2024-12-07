package org.example;

import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * serialize json of a specific format
 */
public class KafkaSpecificJsonProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");

        Producer<String, Product> producer = new KafkaProducer<>(kafkaProps);
        Product product = new Product();
        product.setProductId(100000);
        product.setProductName("phone");
        product.setPrice(8000);
        Product.Dimentions dimentions = new Product.Dimentions();
        String[] tags = new String[]{"1"};
        product.setTags(tags);
        dimentions.setWidth(10);
        dimentions.setHeight(10);
        dimentions.setLength(10);
        product.setDimentions(dimentions);
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("JsonSpecific", product), (recordMetadata, e) -> {
            if (null == e) {
                System.out.println(recordMetadata.topic() + " " + recordMetadata.partition());
            }else{
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();

        Thread.sleep(10000);
    }
}
