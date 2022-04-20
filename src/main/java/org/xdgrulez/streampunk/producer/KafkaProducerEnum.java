package org.xdgrulez.streampunk.producer;

import org.xdgrulez.streampunk.helper.Helpers;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.HashMap;
import java.util.Map;

// Java Singletons Using Enum (https://dzone.com/articles/java-singletons-using-enum)
public enum KafkaProducerEnum {
    INSTANCE;

    private final Map<String, KafkaProducer<?, ?>> stringKafkaProducerMap = new HashMap<>();

    public KafkaProducer<?, ?> get(String clusterString,
                                   Class<?> keySerializerClass,
                                   Class<?> valueSerializerClass) {
        var string = clusterString + "_" + keySerializerClass.toString() + "_" + valueSerializerClass.toString();
        KafkaProducer<?, ?> kafkaProducer;
        if (stringKafkaProducerMap.containsKey(string)) {
            kafkaProducer = stringKafkaProducerMap.get(string);
        } else {
            var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
            properties.put("key.serializer", keySerializerClass);
            properties.put("value.serializer", valueSerializerClass);
            kafkaProducer = new KafkaProducer<>(properties);
            this.stringKafkaProducerMap.put(string, kafkaProducer);
        }
        return kafkaProducer;
    }
}
