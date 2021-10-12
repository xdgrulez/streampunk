package org.xdgrulez.streampunk.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProducerByteArray extends Producer {

    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaProducer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaProducer<byte[], byte[]> getKafkaProducer(String clusterString) {
        return (KafkaProducer<byte[], byte[]>)
                KafkaProducerEnum.INSTANCE.get(clusterString, ByteArraySerializer.class, ByteArraySerializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Produce
    ////////////////////////////////////////////////////////////////////////////////

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, byte[] keyBytes, byte[] valueBytes) {
        ProducerByteArray.getKafkaProducer(clusterString)
                .send(new ProducerRecord<byte[], byte[]>(topicString, partitionInt, keyBytes, valueBytes));
    }

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, byte[] keyBytes, byte[] valueBytes, Iterable<Header> headers) {
        ProducerByteArray.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, keyBytes, valueBytes, headers));
    }

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, long timestamp, byte[] keyBytes, byte[] valueBytes) {
        ProducerByteArray.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyBytes, valueBytes));
    }

    public static void produce(
            String clusterString,
            String topicString, int partitionInt, long timestamp, byte[] keyBytes, byte[] valueBytes, Iterable<Header> headers) {
        ProducerByteArray.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, partitionInt, timestamp, keyBytes, valueBytes, headers));
    }

    public static void produce(
            String clusterString,
            String topicString, byte[] keyBytes, byte[] valueBytes) {
        ProducerByteArray.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, keyBytes, valueBytes));
    }

    public static void produce(
            String clusterString,
            String topicString, byte[] valueBytes) {
        ProducerByteArray.getKafkaProducer(clusterString)
                .send(new ProducerRecord<>(topicString, valueBytes));
    }
}
