package org.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * Send Avro Generic data
 */
public class KafkaGenericAvroProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        Producer<String, GenericRecord> producer = new KafkaProducer<>(kafkaProps);
        String schemaStr = """
                        {
                          "type": "record",
                          "name": "Customer",
                          "namespace": "org.example",
                          "fields": [
                            {
                              "name": "id",
                              "type": "long"
                            },
                            {
                              "name": "name",
                              "type": "string"
                            }
                          ]
                        }
                """;
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaStr);
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 10000L);
        genericRecord.put("name", "Bryan");

        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("AvroRequests", genericRecord.hasField("id") ? String.valueOf(genericRecord.get("id")) : null, genericRecord);
        Future<RecordMetadata> future = producer.send(record, (recordMetadata, e) -> {
            if (null == e) {
                System.out.println(recordMetadata.topic() + " " + recordMetadata.partition());
            }else{
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }
}
