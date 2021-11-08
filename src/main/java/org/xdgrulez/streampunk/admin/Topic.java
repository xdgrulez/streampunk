package org.xdgrulez.streampunk.admin;

import org.xdgrulez.streampunk.consumer.ConsumerByteArray;
import org.xdgrulez.streampunk.exception.ExecutionRuntimeException;
import org.xdgrulez.streampunk.exception.InterruptedRuntimeException;
import org.xdgrulez.streampunk.helper.Helpers;
import org.xdgrulez.streampunk.record.ConfigRec;
import org.xdgrulez.streampunk.record.ConfigResourceRec;
import org.xdgrulez.streampunk.record.OffsetsRec;
import org.xdgrulez.streampunk.record.TopicDescriptionRec;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Topic {
    // CreatePartitionsResult createPartitions(Map<String,NewPartitions> newPartitions, CreatePartitionsOptions options)
    // DeleteRecordsResult 	deleteRecords(Map<TopicPartition,RecordsToDelete> recordsToDelete, DeleteRecordsOptions options)

    public static void create(String clusterString, String topicString,
                              Integer numPartitionsInt,
                              Integer replicationFactorInt,
                              Map<String, String> configStringStringMap) {
        Short replicationFactorShort = null;
        if (replicationFactorInt != null) {
            replicationFactorShort = (short) (int) replicationFactorInt;
        }
        var newTopic = new NewTopic(
                topicString,
                Optional.ofNullable(numPartitionsInt),
                Optional.ofNullable(replicationFactorShort));
        if (configStringStringMap != null) {
            newTopic.configs(configStringStringMap);
        }
        var createTopicsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .createTopics(Collections.singletonList(newTopic));
        try {
            createTopicsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
    }

    public static void delete(String clusterString, String topicString,
                              boolean interactiveBoolean) {
        if (!interactiveBoolean ||
                Helpers.yesNoPrompt(String.format("Are you sure to delete topic \"%s\" (y/N)?",
                        topicString))) {
            var deleteTopicsResult = AdminClientEnum.INSTANCE.get(clusterString)
                    .deleteTopics(Collections.singletonList(topicString));
            try {
                deleteTopicsResult.all().get();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            } catch (ExecutionException e) {
                throw new ExecutionRuntimeException(e);
            }
        }
    }

    public static void setConfig(String clusterString, String topicString,
                                 Map<String, String> configStringStringMap) {
        var configResourceRec = new ConfigResourceRec("TOPIC", topicString);
        var configRec = new ConfigRec(configResourceRec, configStringStringMap);
        Admin.alterConfig(clusterString, configRec);
    }

    public static Map<String, String> getConfig(String clusterString, String topicString) {
        var configResourceRec = new ConfigResourceRec("TOPIC", topicString);
        return Admin.describeConfig(clusterString, configResourceRec);
    }

    private static Map<String, TopicDescription> getTopicStringTopicDescriptionMap(
            DescribeTopicsResult describeTopicsResult) {
        Map<String, TopicDescription> topicStringTopicDescriptionMap;
        try {
            topicStringTopicDescriptionMap = describeTopicsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        return topicStringTopicDescriptionMap;
    }

    private static Map<Integer, Long> getOffsets(String clusterString, String topicString,
                                                 OffsetSpec offsetSpec) {
        var describeTopicsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .describeTopics(Collections.singletonList(topicString));
        var topicStringTopicDescriptionMap =
                getTopicStringTopicDescriptionMap(describeTopicsResult);
        var topicDescription = topicStringTopicDescriptionMap.get(topicString);
        var topicPartitionInfoList = topicDescription.partitions();
        var topicPartitionOffsetSpecMap = topicPartitionInfoList
                .stream()
                .map(topicPartitionInfo ->
                        Map.entry(
                                new TopicPartition(topicString, topicPartitionInfo.partition()),
                                offsetSpec))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        var listOffsetsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .listOffsets(topicPartitionOffsetSpecMap);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> topicPartitionListOffsetsResultInfo;
        try {
            topicPartitionListOffsetsResultInfo = listOffsetsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        var offsets = topicPartitionInfoList
                .stream()
                .map(topicPartitionInfo ->
                        Map.entry(
                                topicPartitionInfo.partition(),
                                topicPartitionListOffsetsResultInfo.get(
                                        new TopicPartition(topicString, topicPartitionInfo.partition())).offset()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        //
        return offsets;
    }

    public static TopicDescriptionRec describe(String clusterString, String topicString) {
        var topicStringList = Collections.singletonList(topicString);
        var describeTopicsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .describeTopics(topicStringList);
        var topicStringTopicDescriptionMap =
                getTopicStringTopicDescriptionMap(describeTopicsResult);
        var topicDescription = topicStringTopicDescriptionMap.get(topicString);
        return new TopicDescriptionRec(topicDescription);
    }

    public static int getPartitions(String clusterString, String topicString) {
        var topicDescriptionRec = describe(clusterString, topicString);
        //
        return topicDescriptionRec.getPartitions().size();
    }

    public static int getReplicationFactor(String clusterString, String topicString) {
        var topicDescriptionRec = describe(clusterString, topicString);
        //
        return topicDescriptionRec.getPartitions().get(0).getReplicas().size();
    }

    public static OffsetsRec getOffsets(String clusterString, String topicString) {
        var earliestOffsets = getOffsets(clusterString, topicString, OffsetSpec.earliest());
        var latestOffsets = getOffsets(clusterString, topicString, OffsetSpec.latest());
        //
        return new OffsetsRec(earliestOffsets, latestOffsets);
    }

    public static Map<Integer, Long> getOffsets(String clusterString, String topicString, long timestampLong) {
        return getOffsets(clusterString, topicString, OffsetSpec.forTimestamp(timestampLong));
    }

    public static List<String> list(String clusterString) {
        var listTopicsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .listTopics();
        Set<String> topicStringSet;
        try {
            topicStringSet = listTopicsResult.names().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        //
        return topicStringSet
                .stream()
                .sorted()
                .collect(Collectors.toList());
    }

    // Extended functionality beyond the Kafka API

    public static Map<Integer, Long> getLastOffsets(String clusterString, String topicString) {
        var latestOffsets = Topic.getOffsets(clusterString, topicString).getLatest();
        //
        var cleanupPolicyString = Topic.getConfig(clusterString, topicString).get("cleanup.policy");
        if ("compact".equals(cleanupPolicyString)) {
            var partitionsInt = Topic.getPartitions(clusterString, topicString);
            var lastOffsets = new HashMap<Integer, Long>();
            for (int i = 0; i < partitionsInt; i++) {
                Long lastOffsetLong = getLastOffset(clusterString, topicString, i,
                        latestOffsets.get(i));
                lastOffsets.put(i, lastOffsetLong);
            }
            //
            return lastOffsets;
        } else {
            return Topic.getOffsets(clusterString, topicString).getLatest();
        }
    }

    public static Long getLastOffset(String clusterString, String topicString, int partitionInt,
                                     long latestOffsetLong) {
        // TODO find the last record in a compacted topic (go backward from latestOffsetLong)
        for (long i = latestOffsetLong - 1; i > 0L; i--) {
            var consumerRecordList =
                    ConsumerByteArray.consumeN(clusterString,
                            topicString,
                            "test",
                            1,
                            partitionInt,
                            i,
                            false,
                            1);
        }
        return 0L;
    }

    public static boolean exists(String clusterString, String topicString) {
        List<String> topicStringList = Topic.list(clusterString);
        return topicStringList.contains(topicString);
    }

    public static Map<Integer, Long> getSizes(String clusterString, String topicString) {
        Map<Integer, Long> earliestOffsets =
                Topic.getOffsets(clusterString, topicString).getEarliest();
        Map<Integer, Long> latestOffsets =
                Topic.getOffsets(clusterString, topicString).getLatest();
        //
        return latestOffsets.entrySet().stream().map(partitionIntLatestOffsetLongEntry -> {
            var partitionInt = partitionIntLatestOffsetLongEntry.getKey();
            return Map.entry(partitionInt,
                    partitionIntLatestOffsetLongEntry.getValue() - earliestOffsets.get(partitionInt));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static long getTotalSize(String clusterString, String topicString) {
        Map<Integer, Long> partitionIntSizeLongMap = getSizes(clusterString, topicString);
        long totalSizeLong = 0;
        for (long sizeLong : partitionIntSizeLongMap.values()) {
            totalSizeLong += sizeLong;
        }
        return totalSizeLong;
    }

    // Extended functionality beyond the Kafka API: Topic + Groups

    public static List<String> listGroups(String clusterString, String topicString) {
        return Group.list(clusterString)
                .stream()
                .filter(groupString -> Group.getOffsets(clusterString, groupString)
                        .keySet()
                        .stream()
                        .anyMatch(topicString1 -> topicString1.equals(topicString)))
                .distinct()
                .sorted()
                .collect(Collectors.toList());
    }

    public static Long groupGetTotalOffset(String clusterString, String topicString, String groupString) {
        var partitionIntOffsetLongMap = Group.getOffsets(clusterString, groupString).get(topicString);
        return partitionIntOffsetLongMap.values().stream().reduce(0L, Long::sum);
    }

    public static Map<Integer, Long> groupGetLags(String clusterString, String topicString, String groupString) {
        Map<Integer, Long> sizes = getSizes(clusterString, topicString);
        Map<Integer, Long> latestOffsets =
                Topic.getOffsets(clusterString, topicString).getLatest();
        Map<Integer, Long> groupOffsets = Group.getOffsets(clusterString, groupString).get(topicString);
        //
        return latestOffsets.entrySet().stream().map(partitionIntLatestOffsetLongEntry -> {
            var partitionInt = partitionIntLatestOffsetLongEntry.getKey();
            //
            var size = sizes.get(partitionInt);
            var lagLong = 0L;
            if (sizes.get(partitionInt) > 0) {
                if (groupOffsets != null && groupOffsets.containsKey(partitionInt)) {
                    lagLong = partitionIntLatestOffsetLongEntry.getValue() - groupOffsets.get(partitionInt);
                }
            }
            return Map.entry(partitionInt, lagLong);
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static long groupGetTotalLag(String clusterString, String topicString, String groupString) {
        Map<Integer, Long> lags = groupGetLags(clusterString, topicString, groupString);
        long totalLagLong = 0;
        for (long lagLong : lags.values()) {
            totalLagLong += lagLong;
        }
        return totalLagLong;
    }
}
