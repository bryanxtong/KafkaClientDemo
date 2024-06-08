package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class KafkaSpecificAvroProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        kafkaProps.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        Producer<String, Customer> producer = new KafkaProducer<>(kafkaProps);
        Customer customer = new Customer();
        customer.setId(10000L);
        customer.setName("Bryan");
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("bRequests", String.valueOf(customer.getId()), customer), (recordMetadata, e) -> {
            if(null == e){
                System.out.println(recordMetadata.topic() +" "+ recordMetadata.partition());
            }else{
                e.printStackTrace();
            }
        });
/*        while (future.isDone()) {
            RecordMetadata recordMetadata = future.get(10, TimeUnit.SECONDS);
            System.out.println(recordMetadata);
        }*/
        producer.flush();
        producer.close();
        //Thread.sleep(10000);
    }
}
