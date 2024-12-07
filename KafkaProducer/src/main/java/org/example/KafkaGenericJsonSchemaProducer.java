package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Write Generic Json to Kafka with JsonSchema validation
 * For Genetic Json(JsonNode) , both schema and payload has to be provided
 * Embedded the schema in files
 */
public class KafkaGenericJsonSchemaProducer {

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        kafkaProps.put("json.fail.invalid.schema", true);
        kafkaProps.put("schema.registry.url", "http://localhost:8081");
        Producer<String, JsonNode> producer = new KafkaProducer<>(kafkaProps);

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

        ObjectMapper objectMapper = new ObjectMapper();
        JsonSchema jsonSchema = readJsonSchema(objectMapper, "json/Product.json");
        //convert java object to JsonNode
        JsonNode jsonNode = objectMapper.valueToTree(product);
        ObjectNode envelope = JsonSchemaUtils.envelope(jsonSchema, jsonNode);
        //System.out.println(envelope.toString());
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>("JsonSchemaGeneric", envelope), (recordMetadata, e) -> {
            if (null == e) {
                System.out.println(recordMetadata.topic() + " " + recordMetadata.partition());
            } else {
                e.printStackTrace();
            }
        });
        producer.flush();
        producer.close();
    }

    /**
     * Read Json string to JsonNode
     *
     * @param mapper
     * @param fileName
     * @return
     */
    public static JsonSchema readJsonSchema(ObjectMapper mapper, String fileName) {
        try {
            URI uri = KafkaGenericJsonSchemaProducer.class.getClassLoader().getResource(fileName).toURI();
            String schemaStr = Files.readString(Paths.get(uri));
            JsonNode jsonNode = mapper.readTree(schemaStr);
            JsonSchema jsonSchema = new JsonSchema(jsonNode);
            return jsonSchema;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
