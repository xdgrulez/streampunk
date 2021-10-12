package org.xdgrulez.streampunk.addon;

import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.consumer.Consumer;
import org.xdgrulez.streampunk.consumer.ConsumerString;
import org.xdgrulez.streampunk.consumer.ConsumerStringAvro;
import org.xdgrulez.streampunk.exception.IORuntimeException;
import org.xdgrulez.streampunk.helper.fun.Proc;
import org.xdgrulez.streampunk.record.OffsetTimestampRec;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class Download {
    public static void downloadStringAvroToFile(String clusterString,
                                                String topicString,
                                                String fileString) {
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
        var doConsumerRecordProc =
                (Proc<ConsumerRecord<String, GenericRecord>>) consumerRecord -> {
                    System.out.printf("Topic %s, partition %d, offset: %d\n",
                            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                    //
                    try {
                        var string = String.format("%d:%d,%s:%s\n",
                                consumerRecord.partition(),
                                consumerRecord.offset(),
                                consumerRecord.key(),
                                consumerRecord.value());
                        //
                        System.out.println(string);
                        //
                        Files.writeString(Path.of(fileString),
                                string,
                                StandardCharsets.UTF_8,
                                StandardOpenOption.APPEND);
                    } catch (IOException e) {
                        throw new IORuntimeException(e);
                    }
                };
        //
        try {
            Files.deleteIfExists(Path.of(fileString));
            Files.createFile(Path.of(fileString));
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        //
        ConsumerStringAvro.consume(clusterString, topicString, "test", startOffsets, endOffsets, doConsumerRecordProc, null, 500, false, 1);
    }

    public static void downloadStringToFile(String clusterString,
                                            String topicString,
                                            String fileString) {
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
        downloadStringToFile(clusterString, topicString, fileString, startOffsets, endOffsets);
    }

    public static void downloadStringToFile(String clusterString,
                                            String topicString,
                                            String fileString,
                                            String startTimestampString,
                                            String endTimestampString) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss O");
        //
        var startZonedDateTime = ZonedDateTime.parse(startTimestampString, dateTimeFormatter);
        var startEpochMilliLong = startZonedDateTime.toInstant().toEpochMilli();
        var startPartitionIntOffsetTimestampRecMap = Consumer.getOffsetForTimestamp(clusterString, topicString, startEpochMilliLong);
        System.out.println("Start");
        var startOffsets = getOffsets(startPartitionIntOffsetTimestampRecMap);
        //
        var endZonedDateTime = ZonedDateTime.parse(endTimestampString, dateTimeFormatter);
        var endEpochMilliLong = endZonedDateTime.toInstant().toEpochMilli();
        var endPartitionIntOffsetTimestampRecMap = Consumer.getOffsetForTimestamp(clusterString, topicString, endEpochMilliLong);
        System.out.println("End");
        var endOffsets = getOffsets(endPartitionIntOffsetTimestampRecMap);
        //
        downloadStringToFile(clusterString, topicString, fileString, startOffsets, endOffsets);
    }

    private static Map<Integer, Long> getOffsets(Map<Integer, OffsetTimestampRec> partitionIntOffsetTimestampRecMap) {
        return partitionIntOffsetTimestampRecMap.entrySet().stream()
                        .map(partitionIntOffsetTimestampRecEntry -> {
                            var partitionInt = partitionIntOffsetTimestampRecEntry.getKey();
                            var offsetTimestampRec = partitionIntOffsetTimestampRecEntry.getValue();
                            System.out.printf("Partition: %d, Offset: %d, Timestamp: %d\n",
                                    partitionInt, offsetTimestampRec.getOffset(), offsetTimestampRec.getTimestamp());
                            return Map.entry(partitionInt, offsetTimestampRec.getOffset());
                        })
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static void downloadStringToFile(String clusterString,
                                            String topicString,
                                            String fileString,
                                            Map<Integer, Long> startOffsets,
                                            Map<Integer, Long> endOffsets) {
        var doConsumerRecordProc =
                (Proc<ConsumerRecord<String, String>>) consumerRecord -> {
                    if (consumerRecord.offset() % 1000 == 0) {
                        var partitionInt = consumerRecord.partition();
                        var offsetLong = consumerRecord.offset();
                        var endOffsetLong = endOffsets.get(partitionInt);
                        var progressDouble = offsetLong / (double)endOffsetLong * 100;
                        System.out.printf("Topic %s, partition %d, offset: %d/%d (%.8f%%)\n",
                                consumerRecord.topic(), partitionInt,
                                consumerRecord.offset(), endOffsetLong, progressDouble);
                    }
                    //
                    try {
                        var string = String.format("%d:%d,%s:%s\n",
                                consumerRecord.partition(),
                                consumerRecord.offset(),
                                consumerRecord.key(),
                                consumerRecord.value());
                        //
//                        System.out.println(string);
                        //
                        Files.writeString(Path.of(fileString),
                                string,
                                StandardCharsets.UTF_8,
                                StandardOpenOption.APPEND);
                    } catch (IOException e) {
                        throw new IORuntimeException(e);
                    }
                };
        //
        try {
            Files.deleteIfExists(Path.of(fileString));
            Files.createFile(Path.of(fileString));
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        //
        System.out.println("Starting download...");
        ConsumerString.consume(clusterString, topicString, "test", startOffsets, endOffsets, doConsumerRecordProc, null, 500, false, 1);
        System.out.println("...download done.");
    }
}
