package org.xdgrulez.streampunk.addon;

import org.xdgrulez.streampunk.admin.Cluster;
import org.xdgrulez.streampunk.admin.Group;
import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.consumer.ConsumerByteArray;
import org.xdgrulez.streampunk.exception.InterruptedRuntimeException;
import org.xdgrulez.streampunk.helper.fun.Fun;
import org.xdgrulez.streampunk.helper.fun.Pred;
import org.xdgrulez.streampunk.helper.fun.Proc;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.xdgrulez.streampunk.producer.ProducerByteArray;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Replicate {
    public static void copy(String sourceClusterString,
                            String targetClusterString,
                            String sourceTopicString,
                            String targetTopicString,
                            Fun<ConsumerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> smtFun,
                            boolean deleteExistingTopicBoolean,
                            boolean fromBeginningBoolean,
                            boolean skipConsumerGroupsBoolean) {
        Replicate.replicateTopic(sourceClusterString,
                targetClusterString,
                sourceTopicString,
                targetTopicString,
                null,
                null,
                null,
                deleteExistingTopicBoolean);
        //
        Replicate.replicateTopicContent(sourceClusterString,
                targetClusterString,
                sourceTopicString,
                targetTopicString,
                null,
                null,
                null,
                smtFun,
                false,
                fromBeginningBoolean,
                true);
        //
        if (!skipConsumerGroupsBoolean) {
            Replicate.replicateConsumerGroup(sourceClusterString,
                    targetClusterString,
                    sourceTopicString,
                    targetTopicString,
                    null,
                    null,
                    null,
                    null,
                    null);
        }
    }

    public static void copy(String sourceClusterString,
                            String targetClusterString,
                            String sourceTopicString,
                            String targetTopicString,
                            Fun<byte[], byte[]> bytesBytesSmtFun,
                            boolean deleteExistingTopicBoolean,
                            boolean fromBeginningBoolean) {
        Fun<ConsumerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> smtFun =
                consumerRecord -> new ProducerRecord<>(
                        targetTopicString, consumerRecord.partition(), consumerRecord.timestamp(),
                        consumerRecord.key(), consumerRecord.value(), consumerRecord.headers());
        Replicate.copy(sourceClusterString, targetClusterString, sourceTopicString, targetTopicString,
                smtFun, deleteExistingTopicBoolean, fromBeginningBoolean, false);
    }

    public static void copy(String sourceClusterString,
                            String targetClusterString,
                            String sourceTopicString,
                            String targetTopicString) {
        copy(sourceClusterString, targetClusterString, sourceTopicString, targetTopicString, null, true, true);
    }

    public static void copy(String sourceClusterString,
                            String targetClusterString,
                            String sourceTopicString) {
        copy(sourceClusterString, targetClusterString, sourceTopicString, sourceTopicString, null, true, true);
    }

    // Get current replication lag for source cluster clusterString, consumer group groupString
    // for all topics matching topicRegexString
    public static void replicationLag(String clusterString,
                                      String groupString,
                                      String topicRegexString,
                                      List<String> topicStringList,
                                      Pred<String> topicPred) {
        // Get all topics matching TopicRegexString
        var filteredTopicStringList = Topic.list(clusterString)
                .stream()
                .filter(topicString -> {
                    if (topicRegexString != null) {
                        return topicString.matches(topicRegexString);
                    } else if (topicStringList != null) {
                        return topicStringList.contains(topicString);
                    } else if (topicPred != null) {
                        return topicPred.apply(topicString);
                    } else {
                        return false;
                    }
                })
                .collect(Collectors.toList());
        // Get combined topic size and combined lag for each of the topics and
        // a) print it out, b) calculate total sums of the combined topic size and the combined lag.
        final var totalTopicSizeAllTopicsLong = new AtomicLong(0);
        final var totalTotalLagLong = new AtomicLong(0);
        filteredTopicStringList
                .forEach(topicString -> {
                    var totalTopicSizeLong = Topic.getTotalSize(clusterString, topicString);
                    var totalLagLong = Topic.groupGetTotalLag(clusterString, topicString, groupString);
                    System.out.printf("Topic: %s, Total topic size: %d, Total lag: %d\n",
                            topicString, totalTopicSizeLong, totalLagLong);
                    totalTopicSizeAllTopicsLong.set(totalTopicSizeAllTopicsLong.get() + totalTopicSizeLong);
                    totalTotalLagLong.set(totalTotalLagLong.get() + totalLagLong);
                });
        // Print out the number of topics,
        // the total sums of the combined topic size and the combined lag, and the average combined lag.
        var zoneDateTime = ZonedDateTime.now();
        System.out.printf("Date: %s, Topics: %d, Total topic size (all topics): %d, Total lag (all topics): %d, Percentage read: %.2f%%%n",
                zoneDateTime.toLocalDateTime(),
                filteredTopicStringList.size(),
                totalTopicSizeAllTopicsLong.get(),
                totalTotalLagLong.get(),
                (1 - (float) totalTotalLagLong.get() / totalTopicSizeAllTopicsLong.get()) * 100);
    }

    public static void replicateTopics(String sourceClusterString,
                                       String targetClusterString,
                                       Map<String, String> sourceTopicStringTargetTopicStringMap,
                                       Map<String, Map<String, String>> sourceTopicStringTargetStringStringMapMap,
                                       Map<String, Integer> sourceTopicStringTargetPartitionsIntMap,
                                       Map<String, Integer> sourceTopicStringTargetReplicationFactorIntMap,
                                       boolean deleteExistingTopicBoolean) {
        System.out.println("replicateTopics()");
        //
        sourceTopicStringTargetTopicStringMap
                .keySet()
                .stream()
                .sorted()
                .forEach(sourceTopicString -> {
                    // Get the configuration of the source topic
                    var sourceStringStringMap = Topic.getConfig(sourceClusterString, sourceTopicString);
                    var sourcePartitionsInt = Topic.getPartitions(sourceClusterString, sourceTopicString);
                    var sourceReplicationFactorInt = Topic.getReplicationFactor(sourceClusterString, sourceTopicString);
                    // Combine the source topic configuration with the corresponding target configuration
                    // (using sourceTopicStringTargetStringStringMapMap)
                    var targetStringStringMap = new HashMap<>(sourceStringStringMap);
                    if (sourceTopicStringTargetStringStringMapMap != null &&
                            sourceTopicStringTargetStringStringMapMap.get(sourceTopicString) != null) {
                        sourceTopicStringTargetStringStringMapMap.get(sourceTopicString)
                                .forEach(targetStringStringMap::put);
                    }

                    // Get the number of partitions (using sourceTopicStringTargetPartitionsIntMap)
                    var targetPartitionsInt = sourcePartitionsInt;
                    if (sourceTopicStringTargetPartitionsIntMap != null &&
                            sourceTopicStringTargetPartitionsIntMap.get(sourceTopicString) != null) {
                        targetPartitionsInt = sourceTopicStringTargetPartitionsIntMap.get(sourceTopicString);
                    }
                    // Get the replication factor (using sourceTopicStringTargetReplicationFactorIntMap)
                    var targetReplicationFactorString = Cluster.getConfig(targetClusterString).get("default.replication.factor");
                    var targetReplicationFactorInt = Integer.valueOf(targetReplicationFactorString);
                    //
                    var describeClusterResultRec = Cluster.describe(targetClusterString);
                    var targetClusterNodesInt = describeClusterResultRec.getNodes().size();
                    if (targetClusterNodesInt < targetReplicationFactorInt) {
                        targetReplicationFactorInt = targetClusterNodesInt;
                    }
                    //
                    if (sourceTopicStringTargetReplicationFactorIntMap != null &&
                            sourceTopicStringTargetReplicationFactorIntMap.get(sourceTopicString) != null) {
                        targetReplicationFactorInt = sourceTopicStringTargetReplicationFactorIntMap.get(sourceTopicString);
                    }
                    // Delete the corresponding target topic if it already exists
                    // (using sourceTopicStringTargetTopicStringMap)
                    var targetTopicString = sourceTopicStringTargetTopicStringMap.get(sourceTopicString);

                    System.out.printf("replicateTopics() %s/%s, %s/%s\n",
                            sourceClusterString, targetClusterString, sourceTopicString, targetTopicString);

                    if (Topic.exists(targetClusterString, targetTopicString)) {
                        // If the target topic already exists...
                        if (deleteExistingTopicBoolean) {
                            // ...delete it if deleteExistingTopicBoolean == true
                            do {
                                Topic.delete(targetClusterString, targetTopicString, false);
                                try {
                                    Thread.sleep(5000);
                                } catch (InterruptedException e) {
                                    throw new InterruptedRuntimeException(e);
                                }
                            } while (Topic.exists(targetClusterString, targetTopicString));
                            // Create the corresponding target topic
                            Topic.create(targetClusterString, targetTopicString,
                                    targetPartitionsInt, targetReplicationFactorInt, targetStringStringMap);
                        }
                    } else {
                        // Create the corresponding target topic
                        Topic.create(targetClusterString, targetTopicString,
                                targetPartitionsInt, targetReplicationFactorInt, targetStringStringMap);
                    }

                });
    }

    public static void replicateTopic(String sourceClusterString,
                                      String targetClusterString,
                                      String sourceTopicString,
                                      String targetTopicString,
                                      Map<String, String> targetStringStringMapMap,
                                      Integer targetPartitionsInt,
                                      Integer targetReplicationFactorInt,
                                      boolean deleteExistingTopicBoolean) {
        System.out.printf("sourceClusterString: %s\n", sourceClusterString);
        System.out.printf("targetClusterString: %s\n", targetClusterString);
        System.out.printf("sourceTopicString: %s\n", sourceTopicString);
        System.out.printf("targetTopicString: %s\n", targetTopicString);
        System.out.printf("targetStringStringMapMap: %s\n", targetStringStringMapMap.toString());
        System.out.printf("targetPartitionsInt: %s\n", targetPartitionsInt);
        System.out.printf("targetReplicationFactorInt: %s\n", targetReplicationFactorInt);
        System.out.printf("deleteExistingTopicBoolean: %s\n", deleteExistingTopicBoolean);
        //
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put(sourceTopicString, targetTopicString);
        //
        var sourceTopicStringTargetStringStringMapMap = new HashMap<String, Map<String, String>>();
        sourceTopicStringTargetStringStringMapMap.put(sourceTopicString, targetStringStringMapMap);
        //
        var sourceTopicStringTargetPartitionsIntMap = new HashMap<String, Integer>();
        sourceTopicStringTargetPartitionsIntMap.put(sourceTopicString, targetPartitionsInt);
        //
        var sourceTopicStringTargetReplicationFactorIntMap = new HashMap<String, Integer>();
        sourceTopicStringTargetReplicationFactorIntMap.put(sourceTopicString, targetReplicationFactorInt);
        //
        replicateTopics(sourceClusterString,
                targetClusterString,
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringTargetStringStringMapMap,
                sourceTopicStringTargetPartitionsIntMap,
                sourceTopicStringTargetReplicationFactorIntMap,
                deleteExistingTopicBoolean);
    }

    public static void replicateTopicContents(String sourceClusterString,
                                              String targetClusterString,
                                              Map<String, String> sourceTopicStringTargetTopicStringMap,
                                              Map<String, Map<Integer, Integer>>
                                                      sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                                              Map<String, Map<Integer, Long>> sourceTopicStartOffsetsMap,
                                              Map<String, Map<Integer, Long>> sourceTopicEndOffsetsMap,
                                              Fun<ConsumerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> smtFun,
                                              boolean parallelBoolean,
                                              boolean fromBeginningBoolean,
                                              boolean untilLatestBoolean) {
        System.out.println("replicateTopicContents()");
        //
        System.out.printf("sourceClusterString: %s\n", sourceClusterString);
        System.out.printf("targetClusterString: %s\n", targetClusterString);
        System.out.printf("sourceTopicStringTargetTopicStringMap: %s\n", sourceTopicStringTargetTopicStringMap.toString());
        System.out.printf("sourceTopicStringPartitionIntTargetPartitionIntMapMap: %s\n", sourceTopicStringPartitionIntTargetPartitionIntMapMap);
        System.out.printf("sourceTopicStartOffsetsMap: %s\n", sourceTopicStartOffsetsMap.toString());
        System.out.printf("sourceTopicEndOffsetsMap: %s\n", sourceTopicEndOffsetsMap.toString());
        System.out.printf("parallelBoolean: %s\n", parallelBoolean);
        System.out.printf("fromBeginningBoolean: %s\n", fromBeginningBoolean);
        System.out.printf("untilLatestBoolean: %s\n", untilLatestBoolean);
        //
        // Get list of source topics
        var sourceTopicStringList =
                sourceTopicStringTargetTopicStringMap
                        .keySet()
                        .stream()
                        .sorted()
                        .collect(Collectors.toList());
        // Get mapping from source/target topics to their number of partitions
        var sourceTopicPartionsIntMap = sourceTopicStringTargetTopicStringMap
                .keySet()
                .stream()
                .map(sourceTopicString ->
                {
                    var partitionsInt = Topic.getPartitions(sourceClusterString, sourceTopicString);
                    return Map.entry(sourceTopicString, partitionsInt);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        var targetTopicPartionsIntMap = sourceTopicStringTargetTopicStringMap
                .values()
                .stream()
                .map(targetTopicString ->
                {
                    var partitionsInt = Topic.getPartitions(sourceClusterString, targetTopicString);
                    return Map.entry(targetTopicString, partitionsInt);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // Get list of procedures
        List<Proc<ConsumerRecord<byte[], byte[]>>> sourceDoConsumerRecordProcList =
                sourceTopicStringTargetTopicStringMap
                        .keySet()
                        .stream()
                        .sorted()
                        .map(sourceTopicString -> (Proc<ConsumerRecord<byte[], byte[]>>) consumerRecord ->
                        {
//                            System.out.printf("Topic %s, partition %d, offset: %d\n",
//                                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
                            // Get target topic (using sourceTopicStringTargetTopicStringMap)
                            var targetTopicString = sourceTopicStringTargetTopicStringMap.get(sourceTopicString);
                            // Get number of partitions of source/target topics
                            var sourcePartitionsInt = sourceTopicPartionsIntMap.get(sourceTopicString);
                            var targetPartitionsInt = targetTopicPartionsIntMap.get(targetTopicString);
                            // Get target partition (using sourceTopicStringPartitionIntTargetPartitionIntMapMap)
                            var sourcePartitionInt = consumerRecord.partition();
                            var targetPartitionInt = sourcePartitionInt;
                            if (sourceTopicStringPartitionIntTargetPartitionIntMapMap != null &&
                                    sourceTopicStringPartitionIntTargetPartitionIntMapMap.get(sourceTopicString) != null) {
                                // If the target partition is specified in the partition mapping, then get it from there
                                targetPartitionInt =
                                        sourceTopicStringPartitionIntTargetPartitionIntMapMap.get(sourceTopicString).get(sourcePartitionInt);
                            } else if (sourcePartitionsInt == targetPartitionsInt) {
                                // Else if the number of source and target partitions is the same, take the same partition as in the source
                                targetPartitionInt = sourcePartitionInt;
                            } else {
                                // Else set the target partition to -1 and use use the Kafka's default partitioner to choose the partition
                                targetPartitionInt = -1;
                            }
                            // Apply single message transform
                            ProducerRecord<byte[], byte[]> producerRecord = null;
                            if (smtFun != null) {
                                producerRecord = smtFun.apply(consumerRecord);
                            } else {
                                producerRecord = new ProducerRecord<byte[], byte[]>(
                                        targetTopicString, consumerRecord.key(), consumerRecord.value());
                            }
                            if (targetPartitionInt == -1) {
                                ProducerByteArray.produce(targetClusterString,
                                        targetTopicString,
                                        producerRecord.key(),
                                        producerRecord.value());
                            } else {
                                // Produce record to target topic (into the target partition)
                                ProducerByteArray.produce(targetClusterString,
                                        targetTopicString,
                                        targetPartitionInt,
                                        consumerRecord.timestamp(),
                                        producerRecord.key(),
                                        producerRecord.value(),
                                        consumerRecord.headers());
                            }
                            //
                            if (consumerRecord.offset() % 5000 == 0) {
                                System.out.printf("%s/%s/%d %d -> %s/%s/%d\n",
                                        sourceClusterString, sourceTopicString, sourcePartitionInt,
                                        consumerRecord.offset(),
                                        targetClusterString, targetTopicString, targetPartitionInt);
                            }
                        })
                        .collect(Collectors.toList());
        // Get list of predicates
        List<Pred<ConsumerRecord<byte[], byte[]>>> sourceUntilConsumerRecordPredList =
                sourceTopicStringTargetTopicStringMap
                        .keySet()
                        .stream()
                        .sorted()
                        .map(sourceTopicString -> (Pred<ConsumerRecord<byte[], byte[]>>) null)
                        .collect(Collectors.toList());
        // Create consumer group name
//        var sourceGroupString = String.format("sp-replicate-topic-content-%s-%s", sourceClusterString, targetClusterString);
        var pidLong = ProcessHandle.current().pid();
        var nowLong = new Date().getTime();
        var sourceGroupString = sourceTopicStringList.get(0) + ".sp.consumerstring.consume." + pidLong + "." + nowLong;
        // Delete consumer group if fromBeginningBoolean == true and the consumer group already exists
        if (fromBeginningBoolean && Group.list(sourceClusterString).contains(sourceGroupString)) {
            Group.delete(sourceClusterString, sourceGroupString, false);
        }
        //
        var sourceStartOffsetsList =
                sourceTopicStringTargetTopicStringMap
                        .keySet()
                        .stream()
                        .sorted()
                        .map(sourceTopicString -> {
                            if (sourceTopicStartOffsetsMap != null &&
                                    sourceTopicStartOffsetsMap.get(sourceTopicString) != null) {
                                return sourceTopicStartOffsetsMap.get(sourceTopicString);
                            } else {
                                if (fromBeginningBoolean) {
                                    var partitionsInt = Topic.getPartitions(sourceClusterString, sourceTopicString);
                                    var offsets = new HashMap<Integer, Long>();
                                    for (var i = 0; i < partitionsInt; i++) {
                                        offsets.put(i, 0L);
//                                        offsets.put(i, 80000L);
                                    }
                                    return offsets;
                                } else {
                                    return null;
                                }
                            }
                        })
                        .collect(Collectors.toList());
        //
        var sourceEndOffsetsList =
                sourceTopicStringTargetTopicStringMap
                        .keySet()
                        .stream()
                        .sorted()
                        .map(sourceTopicString -> {
                            if (sourceTopicEndOffsetsMap != null &&
                                    sourceTopicEndOffsetsMap.get(sourceTopicString) != null) {
                                return sourceTopicEndOffsetsMap.get(sourceTopicString);
                            } else {
                                if (untilLatestBoolean) {
                                    return Topic.getOffsets(sourceClusterString, sourceTopicString).getLatest();
                                } else {
                                    return null;
                                }
                            }
                        })
                        .collect(Collectors.toList());
        //
        if (parallelBoolean) {
            ConsumerByteArray.consumeParallel(sourceClusterString,
                    sourceGroupString,
                    sourceTopicStringList,
                    sourceStartOffsetsList,
                    sourceEndOffsetsList,
                    sourceDoConsumerRecordProcList,
                    sourceUntilConsumerRecordPredList,
                    1);
        } else {
            for (var i = 0; i < sourceTopicStringList.size(); i++) {
                var sourceTopicString = sourceTopicStringList.get(i);
                //
                var targetTopicString = sourceTopicStringTargetTopicStringMap.get(sourceTopicString);
                System.out.printf("replicateTopicContents() %s/%s, %s/%s\n",
                        sourceClusterString, sourceTopicString, targetClusterString, targetTopicString);
                //
                long totalSizeLong = Topic.getTotalSize(sourceClusterString, sourceTopicString);
                if (totalSizeLong == 0) {
                    continue;
                }
                //
                var sourceStartOffsets = sourceStartOffsetsList.get(i);
                var sourceEndOffsets = sourceEndOffsetsList.get(i);
                //
                boolean breakBoolean = true;
                var groupOffsets = Group.getOffsets(sourceClusterString, sourceGroupString).get(sourceTopicString);
                if (sourceEndOffsets.entrySet().stream().allMatch(entry -> {
                    var partitionInt = entry.getKey();
                    var offsetLong = entry.getValue();
                    return groupOffsets != null && offsetLong.equals(groupOffsets.get(partitionInt));
                })) {
                    continue;
                }
                //
                var sourceDoConsumerRecordProc = sourceDoConsumerRecordProcList.get(i);
                var sourceUntilConsumerRecordPred = sourceUntilConsumerRecordPredList.get(i);
                ConsumerByteArray.consume(sourceClusterString,
                        sourceTopicString,
                        sourceGroupString,
                        sourceStartOffsets,
                        sourceEndOffsets,
                        sourceDoConsumerRecordProc,
                        sourceUntilConsumerRecordPred,
                        500,
                        false,
                        1);
            }
        }
    }

    public static void replicateTopicContent(String sourceClusterString,
                                             String targetClusterString,
                                             String sourceTopicString,
                                             String targetTopicString,
                                             Map<Integer, Integer> sourcePartitionIntTargetPartitionIntMap,
                                             Map<Integer, Long> sourceStartOffsets,
                                             Map<Integer, Long> sourceEndOffsets,
                                             Fun<ConsumerRecord<byte[], byte[]>, ProducerRecord<byte[], byte[]>> smtFun,
                                             boolean parallelBoolean,
                                             boolean fromBeginningBoolean,
                                             boolean untilLatestBoolean) {
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put(sourceTopicString, targetTopicString);
        //
        var sourceTopicStringPartitionIntTargetPartitionIntMapMap = new HashMap<String, Map<Integer, Integer>>();
        sourceTopicStringPartitionIntTargetPartitionIntMapMap
                .put(sourceTopicString, sourcePartitionIntTargetPartitionIntMap);
        //
        var sourceTopicStartOffsetsMap = new HashMap<String, Map<Integer, Long>>();
        sourceTopicStartOffsetsMap.put(sourceTopicString, sourceStartOffsets);
        //
        var sourceTopicEndOffsetsMap = new HashMap<String, Map<Integer, Long>>();
        sourceTopicEndOffsetsMap.put(sourceTopicString, sourceEndOffsets);
        //
        replicateTopicContents(sourceClusterString,
                targetClusterString,
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                sourceTopicStartOffsetsMap,
                sourceTopicEndOffsetsMap,
                smtFun,
                parallelBoolean,
                fromBeginningBoolean,
                untilLatestBoolean);
    }

    public static void replicateConsumerGroups(String sourceClusterString,
                                               String targetClusterString,
                                               Map<String, String> sourceTopicStringTargetTopicStringMap,
                                               Map<String, Map<Integer, Integer>>
                                                       sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                                               List<String> includeGroupStringList,
                                               String includeGroupRegexpString,
                                               List<String> excludeGroupStringList,
                                               String excludeGroupRegexpString) {
        System.out.println("replicateConsumerGroups()");
        // Create name for the consumer group used for reading the time stamps
        var replicateConsumerGroupsGroupString = String.format("sp-replicate-consumer-groups-%s-%s",
                sourceClusterString, targetClusterString);
        // For all source topics...
        sourceTopicStringTargetTopicStringMap
                .keySet()
                .stream()
                .sorted()
                .forEach(sourceTopicString -> {
                    // Get target topic
                    var targetTopicString = sourceTopicStringTargetTopicStringMap
                            .get(sourceTopicString);

                    System.out.printf("replicateConsumerGroups() %s/%s, %s/%s\n",
                            sourceClusterString, targetClusterString, sourceTopicString, targetTopicString);

                    // Cannot replicate consumer group if target topic is empty
                    long targetTopicTotalSize = Topic.getTotalSize(targetClusterString, targetTopicString);
                    if (targetTopicTotalSize == 0) {
                        return;
                    }

                    // Get latest offsets
                    var sourceLatestOffsets =
                            Topic.getOffsets(sourceClusterString, sourceTopicString).getLatest();
                    var targetLatestOffsets =
                            Topic.getOffsets(targetClusterString, targetTopicString).getLatest();
                    // For all groups associated with the source topic...
                    Topic.listGroups(sourceClusterString, sourceTopicString)
                            .stream()
                            .filter(sourceGroupString -> {
                                if (excludeGroupStringList != null &&
                                        excludeGroupStringList.contains(sourceGroupString)) {
                                    // Do not take the group if it is in the black list
                                    return false;
                                } else if (excludeGroupRegexpString != null &&
                                        sourceGroupString.matches(excludeGroupRegexpString)) {
                                    // Do not take the group if it matches the black regular expression
                                    return false;
                                } else if (includeGroupStringList != null &&
                                        includeGroupStringList.contains(sourceGroupString)) {
                                    // Take the group if it is in the white list
                                    return true;
                                } else if (includeGroupRegexpString != null &&
                                        sourceGroupString.matches(includeGroupRegexpString)) {
                                    // Take the group if it matches the white regular expression
                                    return true;
                                } else if (includeGroupStringList != null &&
                                        !includeGroupStringList.contains(sourceGroupString)) {
                                    // Do not take the group if there is a white list but it is not included in it
                                    return false;
                                } else if (includeGroupRegexpString != null &&
                                        !sourceGroupString.matches(includeGroupRegexpString)) {
                                    // Do not take the group if there is a white regular expression but it does not match it
                                    return false;
                                } else {
                                    // Take the group otherwise
                                    return true;
                                }
                            })
                            .forEach(sourceGroupString -> {
                                // Get source group offsets
                                var sourceGroupOffsets =
                                        Group.getOffsets(sourceClusterString, sourceGroupString)
                                                .get(sourceTopicString);
                                // Get timestamps corresponding to the source group offsets
                                var sourceGroupTimestamps = sourceGroupOffsets.entrySet()
                                        .stream()
                                        .map(sourceGroupOffset -> {
                                            var sourceGroupPartitionInt = sourceGroupOffset.getKey();
                                            var sourceGroupOffsetLong = sourceGroupOffset.getValue();
                                            long sourceGroupTimestampLong = 0;
                                            if (sourceGroupOffsetLong > 0) {
                                                // If the source group offset > 0,
                                                // consume the record before the current one and get its timestamp
                                                sourceGroupTimestampLong =
                                                        ConsumerByteArray.consumeN(
                                                                        sourceClusterString,
                                                                        sourceTopicString,
                                                                        replicateConsumerGroupsGroupString,
                                                                        1,
                                                                        sourceGroupPartitionInt,
                                                                        sourceGroupOffsetLong - 1,
                                                                        false,
                                                                        1)
                                                                .get(0).timestamp();
                                            }
                                            return Map.entry(sourceGroupPartitionInt, sourceGroupTimestampLong);
                                        })
                                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                                // Set target group offsets corresponding to the timestamps
                                var targetGroupOffsets = new HashMap<Integer, Long>();
                                var targetGroupLags = new HashMap<Integer, Long>();
                                sourceGroupTimestamps
                                        .forEach((sourceGroupPartitionInt, sourceGroupTimestampLong) -> {
                                            // Get target partition
                                            var targetGroupPartitionInt = sourceGroupPartitionInt;
                                            if (sourceTopicStringPartitionIntTargetPartitionIntMapMap != null &&
                                                    sourceTopicStringPartitionIntTargetPartitionIntMapMap
                                                            .get(sourceTopicString) != null) {
                                                var sourcePartitionIntTargetPartitionIntMap =
                                                        sourceTopicStringPartitionIntTargetPartitionIntMapMap
                                                                .get(sourceTopicString);
                                                targetGroupPartitionInt = sourcePartitionIntTargetPartitionIntMap
                                                        .get(sourceGroupPartitionInt);
                                            }
                                            // Get target group offset corresponding to the source group timestamp
                                            var targetOffsetLong = 0L;
                                            if (sourceGroupTimestampLong > 0L) {
                                                targetOffsetLong = Topic.getOffsets(
                                                                targetClusterString,
                                                                targetTopicString,
                                                                sourceGroupTimestampLong + 1)
                                                        .get(targetGroupPartitionInt);
                                                // topicGetOffsets() returns -1 if we are at the end of the topic...
                                                if (targetOffsetLong == -1) {
                                                    targetOffsetLong =
                                                            targetLatestOffsets.get(targetGroupPartitionInt);
                                                }
                                            }
                                            // If partitions are joined, choose the target group offset such that no record is skipped
                                            long sourceGroupOffsetLong =
                                                    sourceGroupOffsets.get(sourceGroupPartitionInt);
                                            long sourceLatestOffsetLong =
                                                    sourceLatestOffsets.get(sourceGroupPartitionInt);
                                            long sourceGroupLagLong = sourceLatestOffsetLong - sourceGroupOffsetLong;
                                            // If the targetGroupOffsets already contains an offset for partition
                                            // targetGroupPartitionInt - i.e., at least two partitions are joined...
                                            if (targetGroupOffsets.containsKey(targetGroupPartitionInt)) {
                                                long oldTargetGroupOffsetLong =
                                                        targetGroupOffsets.get(targetGroupPartitionInt);
                                                long oldSourceGroupLagLong =
                                                        targetGroupLags.get(targetGroupPartitionInt);
                                                if (targetOffsetLong < oldTargetGroupOffsetLong &&
                                                        sourceGroupLagLong > 0) {
                                                    targetGroupOffsets.put(targetGroupPartitionInt,
                                                            targetOffsetLong);
                                                } else if (targetOffsetLong > oldTargetGroupOffsetLong &&
                                                        oldSourceGroupLagLong == 0) {
                                                    targetGroupOffsets.put(targetGroupPartitionInt,
                                                            targetOffsetLong);
                                                }
                                            } else {
                                                targetGroupOffsets.put(targetGroupPartitionInt,
                                                        targetOffsetLong);
                                                targetGroupLags.put(targetGroupPartitionInt,
                                                        sourceGroupLagLong);
                                            }

                                            System.out.printf("%s (%s/%d): %d -> %s (%s/%d): %d\n",
                                                    sourceGroupString, sourceTopicString, sourceGroupPartitionInt, sourceGroupOffsetLong,
                                                    sourceGroupString, targetTopicString, targetGroupPartitionInt, targetOffsetLong);

                                        });
                                targetGroupOffsets
                                        .forEach((targetGroupPartitionInt, targetGroupOffsetLong) -> {
                                            if (targetGroupOffsetLong > 0) {
                                                ConsumerByteArray.consumeN(targetClusterString,
                                                        targetTopicString,
                                                        sourceGroupString,
                                                        1,
                                                        targetGroupPartitionInt,
                                                        targetGroupOffsetLong - 1,
                                                        false,
                                                        1);
                                            }
                                        });
                            });
                });
    }

    public static void replicateConsumerGroup(String sourceClusterString,
                                              String targetClusterString,
                                              String sourceTopicString,
                                              String targetTopicString,
                                              Map<Integer, Integer> sourcePartitionIntTargetPartitionIntMap,
                                              List<String> includeGroupStringList,
                                              String includeGroupRegexpString,
                                              List<String> excludeGroupStringList,
                                              String excludeGroupRegexpString) {
        var sourceTopicStringTargetTopicStringMap = new HashMap<String, String>();
        sourceTopicStringTargetTopicStringMap.put(sourceTopicString, targetTopicString);
        //
        var sourceTopicStringPartitionIntTargetPartitionIntMapMap = new HashMap<String, Map<Integer, Integer>>();
        sourceTopicStringPartitionIntTargetPartitionIntMapMap
                .put(sourceTopicString, sourcePartitionIntTargetPartitionIntMap);
        //
        replicateConsumerGroups(sourceClusterString,
                targetClusterString,
                sourceTopicStringTargetTopicStringMap,
                sourceTopicStringPartitionIntTargetPartitionIntMapMap,
                includeGroupStringList,
                includeGroupRegexpString,
                excludeGroupStringList,
                excludeGroupRegexpString);
    }

    public static void fixSchemaId(String clusterString,
                                   String topicString,
                                   Map<Integer, Integer> oldSchemaIdIntNewSchemaIdIntMap) {
        //
        // Replicate original topic to temporary topic
        //
        String tmpTopicString = topicString + ".tmp";
        Replicate.copy(clusterString, clusterString, topicString, tmpTopicString);
        //
        // Replicate temporary topic back to the original topic (and fix the schema IDs of the messages)
        //
        Fun<byte[], byte[]> fixSchemaSmtFun =
                valueBytes -> {
                    var magicByte = valueBytes[0];
                    //
                    var oldSchemaIdBytes = new byte[4];
                    System.arraycopy(valueBytes, 1, oldSchemaIdBytes, 0, 4);
                    var oldSchemaIdInt = new BigInteger(oldSchemaIdBytes).intValue();
                    //
                    var dataBytesInt = valueBytes.length - 5;
                    var dataBytes = new byte[dataBytesInt];
                    System.arraycopy(valueBytes, 5, dataBytes, 0, dataBytesInt);
                    //
                    var destSchemaIdBytes = oldSchemaIdBytes;
                    if (oldSchemaIdIntNewSchemaIdIntMap.containsKey(oldSchemaIdInt)) {
                        var newSchemaIdInt = oldSchemaIdIntNewSchemaIdIntMap.get(oldSchemaIdInt);
                        destSchemaIdBytes = ByteBuffer.allocate(4).putInt(newSchemaIdInt).array();
                        System.out.printf("Changing schema ID: %d -> %d\n", oldSchemaIdInt, newSchemaIdInt);
                    } else {
                        System.out.printf("Keeping schema ID: %d\n", oldSchemaIdInt);
                    }
                    //
                    byte[] destValueBytes = new byte[valueBytes.length];
                    destValueBytes[0] = magicByte;
                    System.arraycopy(destSchemaIdBytes, 0, destValueBytes, 1, 4);
                    System.arraycopy(dataBytes, 0, destValueBytes, 5, dataBytesInt);
                    //
                    return destValueBytes;
                };
        Replicate.copy(clusterString,
                clusterString,
                tmpTopicString,
                topicString,
                fixSchemaSmtFun,
                true,
                true);
        //
//        Topic.delete(clusterString, tmpTopicString, false);
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
