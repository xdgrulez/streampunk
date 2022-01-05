package org.xdgrulez.streampunk.consumer;

import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.exception.IORuntimeException;
import org.xdgrulez.streampunk.helper.Helpers;
import org.xdgrulez.streampunk.helper.fun.Pred;
import org.xdgrulez.streampunk.helper.fun.Proc;
import org.xdgrulez.streampunk.record.OffsetTimestampRec;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class Consumer {

    ////////////////////////////////////////////////////////////////////////////////
    // Get KafkaConsumer
    ////////////////////////////////////////////////////////////////////////////////

    protected static <Key, Value> KafkaConsumer<Key, Value> getKafkaConsumer(
            String clusterString, String groupString, Integer maxPollRecordsInt,
            Class<?> keyDeserializerClass, Class<?> valueDeserializerClass) {
        var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
        properties.put("auto.offset.reset", "earliest");
        //
        System.out.printf("Group: %s\n", groupString);
        properties.put("group.id", groupString);
        //
        properties.put("enable.auto.commit", "false");
        //
        if (maxPollRecordsInt != null && maxPollRecordsInt > 0) {
            properties.put("max.poll.records", String.valueOf(maxPollRecordsInt));
        }
        //
        properties.put("key.deserializer", keyDeserializerClass);
        properties.put("value.deserializer", valueDeserializerClass);
        //
        return new KafkaConsumer<>(properties);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Subscribe
    ////////////////////////////////////////////////////////////////////////////////

    protected static <Key, Value> void subscribe(KafkaConsumer<Key, Value> kafkaConsumer,
                                                 String topicString,
                                                 Map<Integer, Long> offsets) {
        kafkaConsumer.subscribe(Collections.singletonList(topicString), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> topicPartitionCollection) {
            }

            public void onPartitionsAssigned(Collection<TopicPartition> topicPartitionCollection) {
                if (offsets != null) {
                    topicPartitionCollection.forEach(topicPartition -> {
                        var partitionInt = topicPartition.partition();
                        if (offsets.containsKey(partitionInt)) {
                            var offsetLong = Helpers.getLong(offsets.get(partitionInt));
                            kafkaConsumer.seek(topicPartition, offsetLong);
                        }
                    });
                }
            }
        });
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Poll
    ////////////////////////////////////////////////////////////////////////////////

    protected static <Key, Value> void poll(KafkaConsumer<Key, Value> kafkaConsumer,
                                            String topicString,
                                            Map<Integer, Long> endOffsets,
                                            Proc<ConsumerRecord<Key, Value>> doConsumerRecordProc,
                                            Pred<ConsumerRecord<Key, Value>> untilConsumerRecordPred,
                                            boolean interactiveBoolean,
                                            long interactiveBatchSizeLong) {
        var breakBoolean = false;
        InputStreamReader fileInputStream;
        BufferedReader bufferedReader = null;
        if (interactiveBoolean) {
            fileInputStream = new InputStreamReader(System.in);
            bufferedReader = new BufferedReader(fileInputStream);
        }
        var currentOffsets = new HashMap<Integer, Long>();
        if (endOffsets != null) {
            for (Integer partitionInt: endOffsets.keySet()) {
                currentOffsets.put(partitionInt, 0L);
            }
        }
        var retriesInt = 0;
        var maxRetriesInt = 200;
        do {
            var consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            if (consumerRecords.isEmpty()) {
                retriesInt++;
                if (retriesInt == maxRetriesInt) {
                    breakBoolean = true;
                    System.out.printf("poll(): Stopping after %d retries.\n", maxRetriesInt);
                    continue;
                }
                if (retriesInt % 20 == 0) {
                    System.out.printf("poll(): Retrying... %d/%d\n", retriesInt, maxRetriesInt);
                }
            } else {
                retriesInt = 0;
            }
            var i = 1L;
            for (var consumerRecord : consumerRecords) {
//                System.out.printf("\nTopic: %s, Partition: %d, Offset: %d, Timestamp: %d%n",
//                        topicString,
//                        consumerRecord.partition(),
//                        consumerRecord.offset(),
//                        consumerRecord.timestamp());
                if (!interactiveBoolean && consumerRecord.offset() % 10000 == 0) {
                    var partitionInt = consumerRecord.partition();
                    var offsetLong = consumerRecord.offset();
                    System.out.printf("Topic: %s, partition: %d, offset: %d\n",
                            consumerRecord.topic(), partitionInt, consumerRecord.offset());
                }
                if (interactiveBoolean) {
                    System.out.printf("\nTopic: %s, Partition: %d, Offset: %d, Timestamp: %s%n",
                            topicString,
                            consumerRecord.partition(),
                            consumerRecord.offset(),
                            Helpers.epochToTs(consumerRecord.timestamp()));
                    var headers = consumerRecord.headers();
                    System.out.println("Headers:");
                    for (var header : headers)
                        System.out.printf("Key: %s, Value: %s%n", header.key(), Arrays.toString(header.value()));
                    System.out.printf("Key: %s\n", consumerRecord.key());
                    System.out.printf("Value: %s\n", consumerRecord.value());
                }
                // do doConsumerRecordProc.apply()...
                if (doConsumerRecordProc != null) {
                    doConsumerRecordProc.apply(consumerRecord);
                }
                // ...until either endOffsets is reached...
                if (endOffsets != null) {
                    currentOffsets.put(consumerRecord.partition(), consumerRecord.offset());
                    if (endOffsets.entrySet().stream().allMatch(entry -> {
                        var partitionInt = entry.getKey();
                        var latestOffsetLong = entry.getValue();
                        //
                        var currentOffsetLong = currentOffsets.get(partitionInt);
                        //
//                        if (currentOffsetLong != null && currentOffsetLong > 80000) {
//                            System.out.printf("partition: %d, currentOffsetLong: %d, latestOffsetLong: %d\n", partitionInt, currentOffsetLong, latestOffsetLong);
//                        }
                        return currentOffsetLong != null && currentOffsetLong >= latestOffsetLong - 1;
                    })) {
                        breakBoolean = true;
                        break;
                    }
                }
                // ...or untilConsumerRecordPred.apply() == true
                if (untilConsumerRecordPred != null && untilConsumerRecordPred.apply(consumerRecord)) {
                    breakBoolean = true;
                    break;
                }
                //
                if (interactiveBoolean && i % interactiveBatchSizeLong == 0) {
                    if (!Helpers.yesNoPrompt("\nContinue (Y/n)?", true)) {
                        breakBoolean = true;
                        break;
                    }
                }
                i++;
            }
            //
            kafkaConsumer.commitSync();
            //
            if (interactiveBoolean) {
                try {
                    if (bufferedReader.ready()) {
                        var charInt = bufferedReader.read();
                        if (charInt == 27 || charInt == 32) {
                            breakBoolean = true;
                        }
                    }
                } catch (IOException e) {
                    throw new IORuntimeException(e);
                }
            }
        } while (!breakBoolean);
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume Partition
    ////////////////////////////////////////////////////////////////////////////////

    protected static <Key, Value> void consumePartition(KafkaConsumer<Key, Value> kafkaConsumer,
                                                        String topicString,
                                                        int partitionInt,
                                                        Long startOffsetLong,
                                                        Long endOffsetLong,
                                                        Proc<ConsumerRecord<Key, Value>> doConsumerRecordProc,
                                                        Pred<ConsumerRecord<Key, Value>> untilConsumerRecordPred,
                                                        boolean interactiveBoolean,
                                                        long interactiveBatchSizeLong) {
        var topicPartition = new TopicPartition(topicString, partitionInt);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        if (startOffsetLong != null) {
            kafkaConsumer.seek(topicPartition, startOffsetLong);
        }
        var endOffsets = new HashMap<Integer, Long>();
        if (endOffsetLong != null) {
            endOffsets.put(partitionInt, endOffsetLong);
        }
        //
        poll(kafkaConsumer, topicString, endOffsets, doConsumerRecordProc, untilConsumerRecordPred, interactiveBoolean, interactiveBatchSizeLong);
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume Parallel
    ////////////////////////////////////////////////////////////////////////////////

    protected static <Key, Value> void consumeParallel(KafkaConsumer<Key, Value> kafkaConsumer,
                                                       List<String> topicStringList,
                                                       List<Map<Integer, Long>> startOffsetsList,
                                                       List<Map<Integer, Long>> endOffsetsList,
                                                       List<Proc<ConsumerRecord<Key, Value>>> doConsumerRecordProcList,
                                                       List<Pred<ConsumerRecord<Key, Value>>> untilConsumerRecordPredList) {
        List<Runnable> runnableList = new ArrayList<>();
        for (int i = 0; i < topicStringList.size(); i++) {
            var topicString = topicStringList.get(i);
            var startOffsets = startOffsetsList.get(i);
            var endOffsets = endOffsetsList.get(i);
            var doConsumerRecordProc = doConsumerRecordProcList.get(i);
            var untilConsumerRecordPred = untilConsumerRecordPredList.get(i);
            //
            Runnable runnable = () -> {
                subscribe(kafkaConsumer, topicString, startOffsets);
                //
                poll(kafkaConsumer, topicString, endOffsets, doConsumerRecordProc, untilConsumerRecordPred, false, 0);
            };
            runnableList.add(runnable);
        }
        for (Runnable runnable : runnableList) {
            var thread = new Thread(runnable);
            thread.start();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume N
    ////////////////////////////////////////////////////////////////////////////////

    public static void commitOffset(
            String clusterString,
            String topicString,
            String groupString,
            int partitionInt,
            Long offsetLong) {
        var kafkaConsumer =
                getKafkaConsumer(clusterString, groupString, 0,
                        ByteArrayDeserializer.class, ByteArrayDeserializer.class);
        //
        var topicPartition = new TopicPartition(topicString, partitionInt);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        if (offsetLong != null) {
            kafkaConsumer.seek(topicPartition, offsetLong);
        }
        //
        kafkaConsumer.commitSync();
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Consume N
    ////////////////////////////////////////////////////////////////////////////////

    protected static <Key, Value> List<ConsumerRecord<Key, Value>> consumeN(
            KafkaConsumer<Key, Value> kafkaConsumer,
            String topicString,
            long nLong,
            int partitionInt,
            Long offsetLong,
            boolean interactiveBoolean,
            long interactiveBatchSizeLong) {
        var topicPartition = new TopicPartition(topicString, partitionInt);
        kafkaConsumer.assign(Collections.singletonList(topicPartition));
        if (offsetLong != null) {
            kafkaConsumer.seek(topicPartition, offsetLong);
        }
        //
        var consumerRecordList = new ArrayList<ConsumerRecord<Key, Value>>();
        Proc<ConsumerRecord<Key, Value>> doConsumerRecordProc =
                consumerRecordList::add;
        Pred<ConsumerRecord<Key, Value>> untilConsumerRecordPred =
                value -> consumerRecordList.size() >= nLong;
        //
        poll(kafkaConsumer, topicString, null, doConsumerRecordProc, untilConsumerRecordPred, interactiveBoolean, interactiveBatchSizeLong);
        //
        return consumerRecordList;
    }

    ////////////////////////////////////////////////////////////////////////////////
    // Get offset for timestamp
    ////////////////////////////////////////////////////////////////////////////////

    public static Map<Integer, OffsetTimestampRec> getOffsetForTimestamp(
            String clusterString,
            String topicString,
            long timestampLong) {
        var partitionIntTimestampLongMap = new HashMap<Integer, Long>();
        var partitionsInt = Topic.getPartitions(clusterString, topicString);
        for (var partitionInt = 0; partitionInt < partitionsInt; partitionInt++) {
            partitionIntTimestampLongMap.put(partitionInt, timestampLong);
        }
        return getOffsetForTimestamp(clusterString, topicString, partitionIntTimestampLongMap);
    }

    public static Map<Integer, OffsetTimestampRec> getOffsetForTimestamp(
            String clusterString,
            String topicString,
            Map<Integer, Long> partitionIntTimestampLongMap) {
        var properties = Helpers.loadProperties(String.format("./clusters/%s.properties", clusterString));
        properties.put("key.deserializer", ByteArrayDeserializer.class);
        properties.put("value.deserializer", ByteArrayDeserializer.class);
        var kafkaConsumer = new KafkaConsumer<>(properties);
        //
        var topicPartitionTimestampLongMap =
                partitionIntTimestampLongMap.entrySet().stream()
                .map(partitionIntTimestampLongEntry ->
                {
                    var partitionInt = partitionIntTimestampLongEntry.getKey();
                    var timestampLong = partitionIntTimestampLongEntry.getValue();
                    var topicPartition = new TopicPartition(topicString, partitionInt);
                    return Map.entry(topicPartition, timestampLong);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        var topicPartitionOffsetAndTimestampMap =
                kafkaConsumer.offsetsForTimes(topicPartitionTimestampLongMap);
        return topicPartitionOffsetAndTimestampMap.entrySet().stream()
                .map(topicPartitionOffsetAndTimestampEntry -> {
                    var topicPartition = topicPartitionOffsetAndTimestampEntry.getKey();
                    var offsetAndTimestamp = topicPartitionOffsetAndTimestampEntry.getValue();
                    var partitionInt = topicPartition.partition();
                    var offsetTimestampRec = new OffsetTimestampRec(offsetAndTimestamp);
                    return Map.entry(partitionInt, offsetTimestampRec);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
