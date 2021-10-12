package org.xdgrulez.streampunk.producer;

import org.xdgrulez.streampunk.helper.Helpers;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerStringProtobuf extends Producer {
    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaProducer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaProducer<String, DynamicMessage> getKafkaProducer(String clusterString) {
        return (KafkaProducer<String, DynamicMessage>)
                KafkaProducerEnum.INSTANCE.get(clusterString, StringSerializer.class, KafkaProtobufSerializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Produce
    ////////////////////////////////////////////////////////////////////////////////

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            DynamicMessage valueDynamicMessage) {
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueDynamicMessage));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            DynamicMessage valueDynamicMessage,
            Iterable<Header> headers) {
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueDynamicMessage, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyString,
            DynamicMessage valueDynamicMessage) {
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueDynamicMessage));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyString,
            DynamicMessage valueDynamicMessage,
            Iterable<Header> headers) {
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueDynamicMessage, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String keyString,
            DynamicMessage valueDynamicMessage) {
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyString, valueDynamicMessage));
    }

    public static void produce(
            String clusterString,
            String topicString,
            DynamicMessage valueDynamicMessage) {
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueDynamicMessage));
    }

    //

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            String valueJsonString,
            String schemaString) {
        var valueDynamicMessage = Helpers.jsonStringToDynamicMessage(valueJsonString, schemaString);
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueDynamicMessage));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            String valueJsonString,
            String schemaString,
            Iterable<Header> headers) {
        var valueDynamicMessage = Helpers.jsonStringToDynamicMessage(valueJsonString, schemaString);
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueDynamicMessage, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyString,
            String valueJsonString,
            String schemaString) {
        var valueDynamicMessage = Helpers.jsonStringToDynamicMessage(valueJsonString, schemaString);
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueDynamicMessage));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyString,
            String valueJsonString,
            String schemaString,
            Iterable<Header> headers) {
        var valueDynamicMessage = Helpers.jsonStringToDynamicMessage(valueJsonString, schemaString);
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueDynamicMessage, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String keyString,
            String valueJsonString,
            String schemaString) {
        var valueDynamicMessage = Helpers.jsonStringToDynamicMessage(valueJsonString, schemaString);
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyString, valueDynamicMessage));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String valueJsonString,
            String schemaString) {
        var valueDynamicMessage = Helpers.jsonStringToDynamicMessage(valueJsonString, schemaString);
        ProducerStringProtobuf.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueDynamicMessage));
    }
}
