package org.example;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaSpecificProtoConsumer {

    public static void main(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group");
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaProps.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        kafkaProps.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, SimpleMessageProto.SimpleMessage.class.getName());
        Consumer<String, SimpleMessageProto.SimpleMessage> consumer = new KafkaConsumer<>(kafkaProps);
        consumer.subscribe(Collections.singleton("ProtoRequests"));
        AtomicInteger c = new AtomicInteger(0);
        while (true) {
            int i = c.incrementAndGet();
            if (i == 50) break;
            ConsumerRecords<String, SimpleMessageProto.SimpleMessage> poll = consumer.poll(Duration.ofMillis(300));
            System.out.println("polling records...." + poll.count());
            poll.forEach(record -> {
                System.out.println("key: " + record.key() + " value: " + record.value().toString()+ " offset: " + record.offset());
            });
            //consumer.commitAsync();

        }
        consumer.close();
    }
}
