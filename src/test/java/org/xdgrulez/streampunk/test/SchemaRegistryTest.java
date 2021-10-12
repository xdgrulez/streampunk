package org.xdgrulez.streampunk.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class SchemaRegistryTest {
    @BeforeEach
    public void setup() {
    }

    @AfterEach
    public void tearDown() {
    }

        //    @Test
    public void testProduceJSONSchema() throws JsonProcessingException {
        var properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        properties.put("schema.registry.url", "http://localhost:8081");

//        var topicString = "SNACKS_JSONSCHEMA1";
//        var kafkaProducer = new KafkaProducer<String, Snack>(properties);
//        var licoriceSnack = new Snack("licorice", 400, "black");
//        kafkaProducer.send(new ProducerRecord<String, Snack>(topicString, licoriceSnack));
    }

//    @Test
    public void testConsumeJSONSchema() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var topicString = "SNACKS_JSONSCHEMA";
        var kafkaConsumer = new KafkaConsumer<String, JsonNode>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topicString), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> topicPartitionCollection) {
            }

            public void onPartitionsAssigned(Collection<TopicPartition> topicPartitionCollection) {
                kafkaConsumer.seek(new TopicPartition(topicString, 0), 0L);
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, JsonNode> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}
