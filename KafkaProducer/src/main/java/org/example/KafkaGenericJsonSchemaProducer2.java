package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Write Generic Json to Kafka with JsonSchema validation
 * For Genetic Json(JsonNode) , both schema and payload has to be provided
 *
 * Embedded the schema in java
 */
public class KafkaGenericJsonSchemaProducer2 {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put("json.fail.invalid.schema", true);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        Producer<String, JsonNode> producer = new KafkaProducer<>(kafkaProps);

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
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            //convert java object to JsonNode
            JsonNode jsonNode = objectMapper.valueToTree(product);
            JsonNode jsonNodeSchema = objectMapper.readTree(ProductWithSchema.SCHEMA_AS_STRING);
            JsonSchema jsonSchema = new JsonSchema(jsonNodeSchema);
            ObjectNode envelope = JsonSchemaUtils.envelope(jsonSchema, jsonNode);
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>("JsonSchema3", envelope), (recordMetadata, e) -> {
                if (null == e) {
                    System.out.println(recordMetadata.topic() + " " + recordMetadata.partition());
                } else {
                    e.printStackTrace();
                }
            });

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        producer.flush();
        producer.close();
    }
}
