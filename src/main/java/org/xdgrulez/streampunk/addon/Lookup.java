package org.xdgrulez.streampunk.addon;

import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.consumer.ConsumerString;
import org.xdgrulez.streampunk.consumer.ConsumerStringAvro;
import org.xdgrulez.streampunk.helper.fun.Pred;
import org.xdgrulez.streampunk.helper.fun.Proc;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

public class Lookup {
    public static ConsumerRecord<String, GenericRecord> lookupStringAvro(
            String clusterString,
            String topicString,
            Pred<ConsumerRecord<String, GenericRecord>> lookupFun) {
        var partitionsInt = Topic.getPartitions(clusterString, topicString);
        //
        var startOffsets = new HashMap<Integer, Long>();
        for (int i = 0; i < partitionsInt; i++) {
            startOffsets.put(i, 0L);
        }
        //
        var latestOffsets = Topic.getOffsets(clusterString, topicString).getLatest();
        var endOffsets = new HashMap<Integer, Long>();
        for (int i = 0; i < partitionsInt; i++) {
            endOffsets.put(i, latestOffsets.get(i));
        }
        //
        var atomicReference = new AtomicReference<ConsumerRecord<String, GenericRecord>>();
        var doConsumerRecordProc =
                (Proc<ConsumerRecord<String, GenericRecord>>) consumerRecord -> {
                    if (consumerRecord.offset() % 10000 == 0) {
                        System.out.printf("Topic %s, partition %d, offset: %d/%d\n",
                                consumerRecord.topic(), consumerRecord.partition(),
                                consumerRecord.offset(), endOffsets.get(consumerRecord.partition()));
                    }
                    //
                    if (lookupFun.apply(consumerRecord)) {
                        atomicReference.set(consumerRecord);
                    }
                };
        //
        var untilConsumerRecordPred =
                (Pred<ConsumerRecord<String, GenericRecord>>) lookupFun::apply;
        //
        ConsumerStringAvro.consume(clusterString, topicString, "test", startOffsets, endOffsets, doConsumerRecordProc, untilConsumerRecordPred, 500, false, 1);
        //
        return atomicReference.get();
    }

    public static ConsumerRecord<String, String> lookupString(
            String clusterString,
            String topicString,
            Pred<ConsumerRecord<String, String>> lookupFun) {
        return lookupString(clusterString, topicString, null, null, lookupFun);
    }

    public static ConsumerRecord<String, String> lookupString(
            String clusterString,
            String topicString,
            HashMap<Integer, Long> startOffsets,
            Pred<ConsumerRecord<String, String>> lookupFun) {
        return lookupString(clusterString, topicString, startOffsets, null, lookupFun);
    }

    public static ConsumerRecord<String, String> lookupString(
            String clusterString,
            String topicString,
            HashMap<Integer, Long> startOffsets,
            HashMap<Integer, Long> endOffsets,
            Pred<ConsumerRecord<String, String>> lookupFun) {
        var partitionsInt = Topic.getPartitions(clusterString, topicString);
        //
        if (startOffsets == null) {
            startOffsets = new HashMap<>();
            for (int i = 0; i < partitionsInt; i++) {
                startOffsets.put(i, 0L);
            }
        }
        //
        if (endOffsets == null) {
            endOffsets = new HashMap<>();
            var latestOffsets = Topic.getOffsets(clusterString, topicString).getLatest();
            for (int i = 0; i < partitionsInt; i++) {
                endOffsets.put(i, latestOffsets.get(i));
            }
        }
        //
        final var endOffsets1 = endOffsets;
        //
        var atomicReference = new AtomicReference<ConsumerRecord<String, String>>();
        var doConsumerRecordProc =
                (Proc<ConsumerRecord<String, String>>) consumerRecord -> {
                    if (consumerRecord.offset() % 10000 == 0) {
                        System.out.printf("Topic %s, partition %d, offset: %d/%d\n",
                                consumerRecord.topic(), consumerRecord.partition(),
                                consumerRecord.offset(), endOffsets1.get(consumerRecord.partition()));
                    }
                    //
                    if (lookupFun.apply(consumerRecord)) {
                        atomicReference.set(consumerRecord);
                    }
                };
        //
        var untilConsumerRecordPred =
                (Pred<ConsumerRecord<String, String>>) lookupFun::apply;
        //
        ConsumerString.consume(clusterString, topicString, "test", startOffsets, endOffsets, doConsumerRecordProc, untilConsumerRecordPred, 500, false, 1);
        //
        return atomicReference.get();
    }
}
