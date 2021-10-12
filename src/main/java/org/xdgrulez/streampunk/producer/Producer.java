package org.xdgrulez.streampunk.producer;

import org.xdgrulez.streampunk.helper.Helpers;
import org.apache.kafka.clients.producer.KafkaProducer;

public class Producer<Key, Value> {

    protected String clusterString;
    protected Class<?> keySerializerClass;
    protected Class<?> valueSerializerClass;

    protected KafkaProducer<Key, Value> kafkaProducer = null;

    ////////////////////////////////////////////////////////////////////////////////
    // Reset KafkaProducer
    ////////////////////////////////////////////////////////////////////////////////

    protected void resetKafkaProducer() {
        this.kafkaProducer = null;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaProducer
    ////////////////////////////////////////////////////////////////////////////////

    protected KafkaProducer<Key, Value> getKafkaProducer(String clusterString,
                                                         Class<?> keySerializerClass,
                                                         Class<?> valueSerializerClass) {
        if (this.kafkaProducer == null ||
                !clusterString.equals(this.clusterString) ||
                !keySerializerClass.equals(this.keySerializerClass) ||
                !valueSerializerClass.equals(this.valueSerializerClass)) {
            var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
            properties.put("key.serializer", keySerializerClass);
            properties.put("value.serializer", valueSerializerClass);
            this.kafkaProducer = new KafkaProducer<>(properties);
        }
        return this.kafkaProducer;
    }
}
