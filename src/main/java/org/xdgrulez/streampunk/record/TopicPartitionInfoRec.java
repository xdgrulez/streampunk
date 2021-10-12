package org.xdgrulez.streampunk.record;

import org.apache.kafka.common.TopicPartitionInfo;

import java.util.List;
import java.util.stream.Collectors;

public class TopicPartitionInfoRec {
    private int partitionInt;
    private NodeRec leaderNodeRec;
    private List<NodeRec> replicasNodeRecList;
    private List<NodeRec> isrNodeRecList;

    public int getPartition() {
        return partitionInt;
    }
    public void setPartition(int partitionInt) {
        this.partitionInt = partitionInt;
    }

    public NodeRec getLeader() {
        return leaderNodeRec;
    }
    public void setLeader(NodeRec leaderNodeRec) {
        this.leaderNodeRec = leaderNodeRec;
    }

    public List<NodeRec> getReplicas() {
        return replicasNodeRecList;
    }
    public void setReplicas(List<NodeRec> replicasNodeRecList) {
        this.replicasNodeRecList = replicasNodeRecList;
    }

    public List<NodeRec> getIsr() {
        return isrNodeRecList;
    }
    public void setIsr(List<NodeRec> isrNodeRecList) {
        this.isrNodeRecList = isrNodeRecList;
    }

    public TopicPartitionInfoRec(int partitionInt,
                                 NodeRec leaderNodeRec,
                                 List<NodeRec> replicasNodeRecList,
                                 List<NodeRec> isrNodeRecList) {
        this.partitionInt = partitionInt;
        this.leaderNodeRec = leaderNodeRec;
        this.replicasNodeRecList = replicasNodeRecList;
        this.isrNodeRecList = isrNodeRecList;
    }

    public TopicPartitionInfoRec(TopicPartitionInfo topicPartitionInfo) {
        this.partitionInt = topicPartitionInfo.partition();
        this.leaderNodeRec = new NodeRec(topicPartitionInfo.leader());
        this.replicasNodeRecList = topicPartitionInfo
                .replicas()
                .stream()
                .map(node -> new NodeRec(node))
                .collect(Collectors.toList());
        this.isrNodeRecList = topicPartitionInfo
                .isr()
                .stream()
                .map(node -> new NodeRec(node))
                .collect(Collectors.toList());;
    }

    public TopicPartitionInfo topicPartitionInfo() {
        return new TopicPartitionInfo(
                this.partitionInt,
                this.leaderNodeRec.node(),
                this.replicasNodeRecList.stream().map(nodeRec -> nodeRec.node()).collect(Collectors.toList()),
                this.isrNodeRecList.stream().map(nodeRec -> nodeRec.node()).collect(Collectors.toList()));
    }
}
