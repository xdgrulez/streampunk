package org.xdgrulez.streampunk.producer;

import org.xdgrulez.streampunk.helper.Helpers;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

public class ProducerAvro extends Producer {
    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaProducer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaProducer<GenericRecord, GenericRecord> getKafkaProducer(String clusterString) {
        return (KafkaProducer<GenericRecord, GenericRecord>) KafkaProducerEnum.INSTANCE.get(clusterString, KafkaAvroSerializer.class, KafkaAvroSerializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Produce
    ////////////////////////////////////////////////////////////////////////////////

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            GenericRecord keyGenericRecord,
            GenericRecord valueGenericRecord) {
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyGenericRecord, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            GenericRecord keyGenericRecord,
            GenericRecord valueGenericRecord,
            Iterable<Header> headers) {
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyGenericRecord, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            GenericRecord keyGenericRecord,
            GenericRecord valueGenericRecord) {
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyGenericRecord, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            GenericRecord keyGenericRecord,
            GenericRecord valueGenericRecord,
            Iterable<Header> headers) {
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyGenericRecord, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            GenericRecord keyGenericRecord,
            GenericRecord valueGenericRecord) {
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyGenericRecord, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            GenericRecord valueGenericRecord) {
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueGenericRecord));
    }

    //

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyJsonString,
            String keySchemaString,
            String valueJsonString,
            String valueSchemaString) {
        var keyGenericRecord = Helpers.jsonStringToGenericRecord(keyJsonString, keySchemaString);
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, valueSchemaString);
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyGenericRecord, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            String keyJsonString,
            String keySchemaString,
            String valueJsonString,
            String valueSchemaString,
            Iterable<Header> headers) {
        var keyGenericRecord = Helpers.jsonStringToGenericRecord(keyJsonString, keySchemaString);
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, valueSchemaString);
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyGenericRecord, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyJsonString,
            String keySchemaString,
            String valueJsonString,
            String valueSchemaString) {
        var keyGenericRecord = Helpers.jsonStringToGenericRecord(keyJsonString, keySchemaString);
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, valueSchemaString);
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyGenericRecord, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            int partitionInt,
            long timestamp,
            String keyJsonString,
            String keySchemaString,
            String valueJsonString,
            String valueSchemaString,
            Iterable<Header> headers) {
        var keyGenericRecord = Helpers.jsonStringToGenericRecord(keyJsonString, keySchemaString);
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, valueSchemaString);
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyGenericRecord, valueGenericRecord, headers));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String keyJsonString,
            String keySchemaString,
            String valueJsonString,
            String valueSchemaString) {
        var keyGenericRecord = Helpers.jsonStringToGenericRecord(keyJsonString, keySchemaString);
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, valueSchemaString);
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyGenericRecord, valueGenericRecord));
    }

    public static void produce(
            String clusterString,
            String topicString,
            String valueJsonString,
            String valueSchemaString) {
        var valueGenericRecord = Helpers.jsonStringToGenericRecord(valueJsonString, valueSchemaString);
        ProducerAvro.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueGenericRecord));
    }
}
