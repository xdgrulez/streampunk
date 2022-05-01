package org.xdgrulez.streampunk.consumer;

import org.xdgrulez.streampunk.helper.fun.Pred;
import org.xdgrulez.streampunk.helper.fun.Proc;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.List;
import java.util.Map;

public class ConsumerByteArray extends Consumer {

    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaConsumer
    ////////////////////////////////////////////////////////////////////////////////

    protected static KafkaConsumer<byte[], byte[]> getKafkaConsumer(
            String clusterString, String groupString, int maxPollRecordsInt) {
        return getKafkaConsumer(clusterString, groupString, maxPollRecordsInt,
                ByteArrayDeserializer.class, ByteArrayDeserializer.class);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Subscribe
    ////////////////////////////////////////////////////////////////////////////////

    public static void subscribe(String clusterString,
                                 String topicString,
                                 String groupString,
                                 Map<Integer, Long> offsets,
                                 int maxPollRecordsInt) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, maxPollRecordsInt);
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
                               Proc<ConsumerRecord<byte[], byte[]>> doConsumerRecordProc,
                               Pred<ConsumerRecord<byte[], byte[]>> untilConsumerRecordPred,
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
                                        Proc<ConsumerRecord<byte[], byte[]>> doConsumerRecordProc,
                                        Pred<ConsumerRecord<byte[], byte[]>> untilConsumerRecordPred,
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
            List<Proc<ConsumerRecord<byte[], byte[]>>> doConsumerRecordProcList,
            List<Pred<ConsumerRecord<byte[], byte[]>>> untilConsumerRecordPredList,
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

    public static List<ConsumerRecord<byte[], byte[]>> consumeN(
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
