package org.xdgrulez.streampunk.consumer;

import org.xdgrulez.streampunk.helper.fun.Pred;
import org.xdgrulez.streampunk.helper.fun.Proc;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Map;

public class ConsumerStringAvro extends Consumer {

    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaConsumer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaConsumer<String, GenericRecord> getKafkaConsumer(
            String clusterString, String groupString, Integer maxPollRecordsInt) {
        return getKafkaConsumer(clusterString, groupString, maxPollRecordsInt,
                StringDeserializer.class, KafkaAvroDeserializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Subscribe
    ////////////////////////////////////////////////////////////////////////////////

    public static void subscribe(String clusterString,
                                 String topicString,
                                 String groupString,
                                 Map<Integer, Long> offsets) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, null);
        subscribe(kafkaConsumer, topicString, offsets);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume
    ////////////////////////////////////////////////////////////////////////////////

    public static void consume(String clusterString,
                               String topicString,
                               String groupString,
                               Map<Integer, Long> startOffsets,
                               Map<Integer, Long> endOffsets,
                               Proc<ConsumerRecord<String, GenericRecord>> doConsumerRecordProc,
                               Pred<ConsumerRecord<String, GenericRecord>> untilConsumerRecordPred,
                               Integer maxPollRecordsInt,
                               boolean interactiveBoolean,
                               long interactiveBatchSizeLong) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPollRecordsInt);
        //
        subscribe(kafkaConsumer, topicString, startOffsets);
        //
        poll(kafkaConsumer, topicString, endOffsets,
                doConsumerRecordProc, untilConsumerRecordPred, interactiveBoolean, interactiveBatchSizeLong);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume Partition
    ////////////////////////////////////////////////////////////////////////////////

    public static void consumePartition(String clusterString,
                                        String topicString,
                                        String groupString,
                                        int partitionInt,
                                        Long startOffsetLong,
                                        Long endOffsetLong,
                                        Proc<ConsumerRecord<String, GenericRecord>> doConsumerRecordProc,
                                        Pred<ConsumerRecord<String, GenericRecord>> untilConsumerRecordPred,
                                        Integer maxPollRecordsInt,
                                        boolean interactiveBoolean,
                                        long interactiveBatchSizeLong) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPollRecordsInt);
        //
        Consumer.consumePartition(kafkaConsumer, topicString, partitionInt, startOffsetLong, endOffsetLong,
                doConsumerRecordProc, untilConsumerRecordPred, interactiveBoolean, interactiveBatchSizeLong);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume Parallel
    ////////////////////////////////////////////////////////////////////////////////

    public static void consumeParallel(
            String clusterString,
            String groupString,
            List<String> topicStringList,
            List<Map<Integer, Long>> startOffsetsList,
            List<Map<Integer, Long>> endOffsetsList,
            List<Proc<ConsumerRecord<String, GenericRecord>>> doConsumerRecordProcList,
            List<Pred<ConsumerRecord<String, GenericRecord>>> untilConsumerRecordPredList,
            Integer maxPollRecordsInt) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPollRecordsInt);
        //
        Consumer.consumeParallel(kafkaConsumer, topicStringList, startOffsetsList, endOffsetsList, doConsumerRecordProcList, untilConsumerRecordPredList);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume N
    ////////////////////////////////////////////////////////////////////////////////

    public static List<ConsumerRecord<String, GenericRecord>> consumeN(
            String clusterString,
            String topicString,
            String groupString,
            int nInt,
            int partitionInt,
            Long offsetLong,
            boolean interactiveBoolean,
            long interactiveBatchSizeLong) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, nInt);
        //
        return Consumer.consumeN(kafkaConsumer, topicString, nInt, partitionInt, offsetLong,
                interactiveBoolean, interactiveBatchSizeLong);
    }
}