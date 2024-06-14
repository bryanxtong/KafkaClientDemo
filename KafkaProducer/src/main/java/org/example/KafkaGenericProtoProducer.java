package org.example;

import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class KafkaGenericProtoProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        kafkaProps.put(KafkaProtobufSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        Producer<String, DynamicMessage> producer = new KafkaProducer<>(kafkaProps);

        SimpleMessageProto.SimpleMessage message = SimpleMessageProto.SimpleMessage.newBuilder()
                .setContent("Bryan")
                .setDateTime(Instant.now().toString())
                .build();
        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(message).build();
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("ProtoRequests", dynamicMessage), (recordMetadata, e) -> {
            if (null == e) {
                System.out.println(recordMetadata.topic() + " " + recordMetadata.partition());
            } else {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }
}
