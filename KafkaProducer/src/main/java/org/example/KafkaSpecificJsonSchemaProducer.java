package org.example;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * Produce record with json schema validation which Using @Schema annotation on the Java object
 */
public class KafkaSpecificJsonSchemaProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        kafkaProps.put("json.fail.invalid.schema", true);

        Producer<String, ProductWithSchema> producer = new KafkaProducer<>(kafkaProps);
        ProductWithSchema product = new ProductWithSchema();
        product.setProductId(100000);
        product.setProductName("phone");
        product.setPrice(8000);
        ProductWithSchema.Dimentions dimentions = new ProductWithSchema.Dimentions();
        String[] tags = new String[]{"1"};
        product.setTags(tags);
        dimentions.setWidth(10);
        dimentions.setHeight(10);
        dimentions.setLength(10);
        product.setDimentions(dimentions);
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("JsonSchema5", product), (recordMetadata, e) -> {
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
