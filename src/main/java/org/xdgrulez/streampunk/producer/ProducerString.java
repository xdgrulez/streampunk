package org.xdgrulez.streampunk.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerString extends Producer {
    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaProducer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaProducer<String, String> getKafkaProducer(String clusterString) {
        return (KafkaProducer<String, String>)
                KafkaProducerEnum.INSTANCE.get(clusterString, StringSerializer.class, StringSerializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Produce
    ////////////////////////////////////////////////////////////////////////////////

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, String keyString, String valueString) {
        ProducerString.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueString));
    }

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, String keyString, String valueString, Iterable<Header> headers) {
        ProducerString.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyString, valueString, headers));
    }

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, long timestamp, String keyString, String valueString) {
        ProducerString.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueString));
    }

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, long timestamp, String keyString, String valueString, Iterable<Header> headers) {
        ProducerString.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyString, valueString, headers));
    }

    public static void produce(
            String clusterString,
            String topicString, String keyString, String valueString) {
        ProducerString.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyString, valueString));
    }

    public static void produce(
            String clusterString,
            String topicString, String valueString) {
        ProducerString.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueString));
    }
}
