package org.xdgrulez.streampunk.consumer;

import org.xdgrulez.streampunk.helper.fun.Pred;
import org.xdgrulez.streampunk.helper.fun.Proc;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Map;

public class ConsumerAvro extends Consumer {

    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaConsumer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaConsumer<GenericRecord, GenericRecord> getKafkaConsumer(
            String clusterString, String groupString, int maxPollRecordsInt) {
        return getKafkaConsumer(clusterString, groupString, maxPollRecordsInt,
                KafkaAvroDeserializer.class, KafkaAvroDeserializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Subscribe
    ////////////////////////////////////////////////////////////////////////////////

    public static void subscribe(String clusterString,
                                 String topicString,
                                 String groupString,
                                 Map<Integer, Long> offsets,
                                 int maxPullRecordsInt) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPullRecordsInt);
        subscribe(kafkaConsumer, topicString, offsets);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume
    ////////////////////////////////////////////////////////////////////////////////

    public static void consume(String clusterString,
                               String topicString,
                               String groupString,
                               Map<Integer, Long> startOffsets) {
        consume(clusterString, topicString, groupString, startOffsets, null,
                null, null,
                INTERACTIVE_MAX_POLL_RECORDS, INTERACTIVE_MAX_RETRIES, true, INTERACTIVE_BATCH_SIZE);
    }

    public static void consume(String clusterString,
                               String topicString,
                               String groupString,
                               Map<Integer, Long> startOffsets,
                               Map<Integer, Long> endOffsets,
                               Proc<ConsumerRecord<GenericRecord, GenericRecord>> doConsumerRecordProc,
                               Pred<ConsumerRecord<GenericRecord, GenericRecord>> untilConsumerRecordPred,
                               int maxPollRecordsInt,
                               int maxRetriesInt,
                               boolean interactiveBoolean,
                               long interactiveBatchSizeLong) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPollRecordsInt);
        //
        subscribe(kafkaConsumer, topicString, startOffsets);
        //
        poll(kafkaConsumer, topicString, endOffsets,
                doConsumerRecordProc, untilConsumerRecordPred,
                maxRetriesInt, interactiveBoolean, interactiveBatchSizeLong);
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
                                        Proc<ConsumerRecord<GenericRecord, GenericRecord>> doConsumerRecordProc,
                                        Pred<ConsumerRecord<GenericRecord, GenericRecord>> untilConsumerRecordPred,
                                        int maxPollRecordsInt,
                                        int maxRetriesInt,
                                        boolean interactiveBoolean,
                                        long interactiveBatchSizeLong) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPollRecordsInt);
        //
        Consumer.consumePartition(kafkaConsumer, topicString, partitionInt, startOffsetLong, endOffsetLong,
                doConsumerRecordProc, untilConsumerRecordPred,
                maxRetriesInt, interactiveBoolean, interactiveBatchSizeLong);
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
            List<Proc<ConsumerRecord<GenericRecord, GenericRecord>>> doConsumerRecordProcList,
            List<Pred<ConsumerRecord<GenericRecord, GenericRecord>>> untilConsumerRecordPredList,
            int maxPollRecordsInt,
            int maxRetriesInt) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPollRecordsInt);
        //
        Consumer.consumeParallel(kafkaConsumer, topicStringList, startOffsetsList, endOffsetsList,
                doConsumerRecordProcList, untilConsumerRecordPredList,
                maxRetriesInt);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume N
    ////////////////////////////////////////////////////////////////////////////////

    public static List<ConsumerRecord<GenericRecord, GenericRecord>> consumeN(
            String clusterString,
            String topicString,
            String groupString,
            int nInt,
            int partitionInt,
            Long offsetLong,
            int maxRetriesInt,
            boolean interactiveBoolean,
            long interactiveBatchSizeLong) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, nInt);
        //
        return Consumer.consumeN(kafkaConsumer, topicString, nInt, partitionInt, offsetLong,
                maxRetriesInt, interactiveBoolean, interactiveBatchSizeLong);
    }
}
