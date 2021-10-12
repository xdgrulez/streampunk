package org.xdgrulez.streampunk.admin;

import org.xdgrulez.streampunk.exception.ExecutionRuntimeException;
import org.xdgrulez.streampunk.exception.InterruptedRuntimeException;
import org.xdgrulez.streampunk.helper.Helpers;
import org.xdgrulez.streampunk.record.ConfigRec;
import org.xdgrulez.streampunk.record.ConfigResourceRec;
import org.xdgrulez.streampunk.record.OffsetsRec;
import org.xdgrulez.streampunk.record.TopicDescriptionRec;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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
}
