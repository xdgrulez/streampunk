package org.xdgrulez.streampunk.admin;

import org.xdgrulez.streampunk.exception.ExecutionRuntimeException;
import org.xdgrulez.streampunk.exception.InterruptedRuntimeException;
import org.xdgrulez.streampunk.helper.Helpers;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Group {
    public static void delete(String clusterString, String groupString) {
        delete(clusterString, groupString, true);
    }

    public static void delete(String clusterString, String groupString,
                              boolean interactiveBoolean) {
        if (!interactiveBoolean ||
                Helpers.yesNoPrompt(String.format("Are you sure to delete consumer group \"%s\" (y/N)?",
                        groupString), false)) {
            var deleteConsumerGroupsResult = AdminClientEnum.INSTANCE.get(clusterString)
                    .deleteConsumerGroups(Collections.singleton(groupString));
            try {
                deleteConsumerGroupsResult.all().get();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            } catch (ExecutionException e) {
                throw new ExecutionRuntimeException(e);
            }
        }
    }

    public static void deleteOffsets(String clusterString, String groupString,
                                     Map<String, List<Integer>> topicStringPartitionIntListMap,
                                     boolean interactiveBoolean) {
        if (!interactiveBoolean ||
                Helpers.yesNoPrompt(String.format("Are you sure to delete offsets for consumer group \"%s\" (y/N)?",
                        groupString), false)) {
            Set<TopicPartition> topicPartitionSet = new HashSet<>();
            if (topicStringPartitionIntListMap != null) {
                topicPartitionSet =
                        topicStringPartitionIntListMap.entrySet()
                                .stream()
                                .flatMap(topicStringPartitionIntListEntry ->
                                {
                                    var topicString = topicStringPartitionIntListEntry.getKey();
                                    var partitionIntList = topicStringPartitionIntListEntry.getValue();
                                    return partitionIntList
                                            .stream()
                                            .map(partitionInt -> new TopicPartition(topicString, partitionInt));
                                })
                                .collect(Collectors.toSet());
            }
            //
            var deleteConsumerGroupOffsetsResult = AdminClientEnum.INSTANCE.get(clusterString)
                    .deleteConsumerGroupOffsets(groupString, topicPartitionSet);
            try {
                deleteConsumerGroupOffsetsResult.all().get();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            } catch (ExecutionException e) {
                throw new ExecutionRuntimeException(e);
            }
        }
    }

    public static void deleteOffsets(String clusterString, String groupString,
                                     String topicString, List<Integer> partitionIntList,
                                     boolean interactiveBoolean) {
        var topicStringPartitionIntListMap = new HashMap<String, List<Integer>>();
        topicStringPartitionIntListMap.put(topicString, partitionIntList);
        deleteOffsets(clusterString, groupString, topicStringPartitionIntListMap, interactiveBoolean);
    }

    public static ConsumerGroupDescription describe(String clusterString, String groupString) {
        var describeConsumerGroupsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .describeConsumerGroups(Collections.singletonList(groupString));
        Map<String, ConsumerGroupDescription> groupStringConsumerGroupDescriptionMap;
        try {
            groupStringConsumerGroupDescriptionMap = describeConsumerGroupsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        //
        return groupStringConsumerGroupDescriptionMap.get(groupString);
    }

    public static void setOffsets(String clusterString, String groupString,
                                  Map<String, Map<Integer, Long>> topicStringOffsetsMap,
                                  boolean interactiveBoolean) {
        if (!interactiveBoolean ||
                Helpers.yesNoPrompt(String.format("Are you sure to alter the offsets of consumer group \"%s\" (y/N)?",
                        groupString), false)) {
            var topicPartitionOffsetAndMetadataMap = topicStringOffsetsMap
                    .entrySet()
                    .stream()
                    .flatMap(topicStringOffsetsEntry ->
                    {
                        var topicString = topicStringOffsetsEntry.getKey();
                        var offsets = topicStringOffsetsEntry.getValue();
                        return offsets
                                .entrySet()
                                .stream()
                                .map(partitionIntOffsetLongEntry ->
                                {
                                    var partitionInt = partitionIntOffsetLongEntry.getKey();
                                    var offsetLong = Helpers.getLong(partitionIntOffsetLongEntry.getValue());
                                    var topicPartition = new TopicPartition(topicString, partitionInt);
                                    var offsetAndMetadata = new OffsetAndMetadata(offsetLong);
                                    return Map.entry(topicPartition, offsetAndMetadata);
                                });
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            //
            var alterConsumerGroupOffsetsResult = AdminClientEnum.INSTANCE.get(clusterString)
                    .alterConsumerGroupOffsets(groupString, topicPartitionOffsetAndMetadataMap);
            try {
                alterConsumerGroupOffsetsResult.all().get();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            } catch (ExecutionException e) {
                throw new ExecutionRuntimeException(e);
            }
        }
    }

    public static void setOffsets(String clusterString, String groupString,
                                  String topicString, Map<Integer, Long> offsets,
                                  boolean interactiveBoolean) {
        var topicStringOffsetsMap = new HashMap<String, Map<Integer, Long>>();
        topicStringOffsetsMap.put(topicString, offsets);
        setOffsets(clusterString, groupString, topicStringOffsetsMap, interactiveBoolean);
    }

    public static Map<String, Map<Integer, Long>> getOffsets(String clusterString, String groupString) {
        var listConsumerGroupOffsetsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .listConsumerGroupOffsets(groupString);
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap;
        try {
            topicPartitionOffsetAndMetadataMap = listConsumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        //
        Map<String, Map<Integer, Long>> topicStringOffsetsMap = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : topicPartitionOffsetAndMetadataMap.entrySet()) {
            var topicString = entry.getKey().topic();
            var partitionInt = entry.getKey().partition();
            var offsetAndMetadata = entry.getValue();
            Map<Integer, Long> offsets;
            if (topicStringOffsetsMap.containsKey(topicString)) {
                offsets = topicStringOffsetsMap.get(topicString);
            } else {
                offsets = new HashMap<>();
            }
            //
            offsets.put(partitionInt, offsetAndMetadata.offset());
            topicStringOffsetsMap.put(topicString, offsets);
        }
        //
        return topicStringOffsetsMap;
    }

    public static Map<Integer, Long> getOffsets(String clusterString, String groupString,
                                                String topicString) {
        var topicStringOffsetsMap = getOffsets(clusterString, groupString);
        return topicStringOffsetsMap.get(topicString);
    }

    public static void removeMembers(String clusterString, String groupString,
                                     List<String> groupInstanceIdStringList,
                                     boolean interactiveBoolean) {
        if (!interactiveBoolean ||
                Helpers.yesNoPrompt(String.format("Are you sure to remove members from consumer group \"%s\" (y/N)?",
                        groupString), false)) {
            Set<MemberToRemove> memberToRemoveSet = new HashSet<>();
            if (groupInstanceIdStringList != null) {
                memberToRemoveSet = groupInstanceIdStringList
                        .stream()
                        .map(MemberToRemove::new)
                        .collect(Collectors.toSet());
            }
            var removeMembersFromConsumerGroupOptions =
                    new RemoveMembersFromConsumerGroupOptions(memberToRemoveSet);
            //
            var removeMembersFromConsumerGroupResult =
                    AdminClientEnum.INSTANCE.get(clusterString)
                            .removeMembersFromConsumerGroup(groupString, removeMembersFromConsumerGroupOptions);
            try {
                removeMembersFromConsumerGroupResult.all().get();
            } catch (InterruptedException e) {
                throw new InterruptedRuntimeException(e);
            } catch (ExecutionException e) {
                throw new ExecutionRuntimeException(e);
            }
        }
    }

    public static List<String> list(String clusterString) {
        var listConsumerGroupsResult = AdminClientEnum.INSTANCE.get(clusterString)
                .listConsumerGroups();
        Collection<ConsumerGroupListing> consumerGroupListingCollection;
        try {
            consumerGroupListingCollection = listConsumerGroupsResult.all().get();
        } catch (InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } catch (ExecutionException e) {
            throw new ExecutionRuntimeException(e);
        }
        //
        return consumerGroupListingCollection
                .stream()
                .map(ConsumerGroupListing::groupId)
                .sorted()
                .collect(Collectors.toList());
    }

    public static boolean exists(String clusterString, String groupString) {
        var groupList = list(clusterString);
        return groupList.contains(groupString);
    }
}
