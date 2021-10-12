package org.xdgrulez.streampunk.record;

import java.util.Map;

public class OffsetsRec {
    private Map<Integer, Long> earliestPartitionIntOffsetLongMap;
    private Map<Integer, Long> latestPartitionIntOffsetLongMap;

    public Map<Integer, Long> getEarliest() {
        return earliestPartitionIntOffsetLongMap;
    }
    public void setEarliest(Map<Integer, Long> earliestPartitionIntOffsetLongMap) {
        this.earliestPartitionIntOffsetLongMap = earliestPartitionIntOffsetLongMap;
    }

    public Map<Integer, Long> getLatest() {
        return latestPartitionIntOffsetLongMap;
    }
    public void setLatest(Map<Integer, Long> latestPartitionIntOffsetLongMap) {
        this.latestPartitionIntOffsetLongMap = latestPartitionIntOffsetLongMap;
    }

    public OffsetsRec(Map<Integer, Long> earliestPartitionIntOffsetLongMap,
                      Map<Integer, Long> latestPartitionIntOffsetLongMap) {
        this.earliestPartitionIntOffsetLongMap = earliestPartitionIntOffsetLongMap;
        this.latestPartitionIntOffsetLongMap = latestPartitionIntOffsetLongMap;
    }
}
