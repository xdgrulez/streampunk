package org.xdgrulez.streampunk.record;

import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Map;
import java.util.stream.Collectors;

public class TopicDescriptionRec {
    private String nameString;
    private boolean internalBoolean;
    private Map<Integer, TopicPartitionInfoRec> partitionIntTopicPartitionInfoRecMap;

    public String getName() {
        return nameString;
    }
    public void setName(String nameString) {
        this.nameString = nameString;
    }

    public boolean isInternal() {
        return internalBoolean;
    }
    public void setInternal(boolean internalBoolean) {
        this.internalBoolean = internalBoolean;
    }

    public Map<Integer, TopicPartitionInfoRec> getPartitions() {
        return partitionIntTopicPartitionInfoRecMap;
    }
    public void setPartitions(Map<Integer, TopicPartitionInfoRec> partitionIntTopicPartitionInfoRecMap) {
        this.partitionIntTopicPartitionInfoRecMap = partitionIntTopicPartitionInfoRecMap;
    }

    public TopicDescriptionRec(String nameString,
                               boolean internalBoolean,
                               Map<Integer, TopicPartitionInfoRec> partitionIntTopicPartitionInfoRecMap) {
        this.nameString = nameString;
        this.internalBoolean = internalBoolean;
        this.partitionIntTopicPartitionInfoRecMap = partitionIntTopicPartitionInfoRecMap;
    }

    public TopicDescriptionRec(TopicDescription topicDescription) {
        this.nameString = topicDescription.name();
        this.internalBoolean = topicDescription.isInternal();
        this.partitionIntTopicPartitionInfoRecMap =
                topicDescription
                .partitions()
                .stream()
                .map(topicPartitionInfo ->
                        Map.entry(topicPartitionInfo.partition(), new TopicPartitionInfoRec(topicPartitionInfo)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
