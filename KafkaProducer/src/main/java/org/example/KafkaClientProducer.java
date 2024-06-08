package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaClientProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        Producer<String, String> producer = new KafkaProducer<String, String>(kafkaProps);
        Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("bRequests", "hi", "Bryan"), (recordMetadata, e) -> {
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
