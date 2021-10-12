package org.xdgrulez.streampunk.producer;

import org.xdgrulez.streampunk.helper.Helpers;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerStringAvro extends Producer {
    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaProducer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaProducer<String, GenericRecord> getKafkaProducer(String clusterString) {
        return (KafkaProducer<String, GenericRecord>)
                KafkaProducerEnum.INSTANCE.get(clusterString, StringSerializer.class, KafkaAvroSerializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Produce
    ////////////////////////////////////////////////////////////////////////////////

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            GenericRecord valueGenericRecord) {
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            GenericRecord valueGenericRecord,
            Iterable<Header> headers) {
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyString,
            GenericRecord valueGenericRecord) {
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyString,
            GenericRecord valueGenericRecord,
            Iterable<Header> headers) {
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String keyString,
            GenericRecord valueGenericRecord) {
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyString, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            GenericRecord valueGenericRecord) {
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueGenericRecord));
    }

    //

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            String valueJsonString,
            String schemaString) {
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, schemaString);
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyString,
            String valueJsonString,
            String schemaString,
            Iterable<Header> headers) {
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, schemaString);
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyString,
            String valueJsonString,
            String schemaString) {
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, schemaString);
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueGenericRecord));
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
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, schemaString);
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String keyString,
            String valueJsonString,
            String schemaString) {
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, schemaString);
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyString, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String valueJsonString,
            String schemaString) {
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, schemaString);
        ProducerStringAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueGenericRecord));
    }
}
