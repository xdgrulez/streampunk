package org.xdgrulez.streampunk.addon;

import org.xdgrulez.streampunk.admin.Cluster;
import org.xdgrulez.streampunk.admin.Group;
import org.xdgrulez.streampunk.admin.Topic;
import org.xdgrulez.streampunk.consumer.ConsumerByteArray;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopicExt {
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
        List<String> topicStringList = Cluster.listTopics(clusterString);
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
        return Cluster.listGroups(clusterString)
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
