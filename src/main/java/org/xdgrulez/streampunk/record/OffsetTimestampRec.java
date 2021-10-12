package org.xdgrulez.streampunk.record;

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;

public class OffsetTimestampRec {
    private long offsetLong;
    private long timestampLong;

    public long getOffset() { return offsetLong; };
    public void setOffset(long offsetLong) {
        this.offsetLong = offsetLong;
    }

    public long getTimestamp() {
        return this.timestampLong;
    }
    public void setTimestamp(long timestampLong) {
        this.timestampLong = timestampLong;
    }

    public OffsetTimestampRec(long offsetLong, long timestampLong) {
        this.offsetLong = offsetLong;
        this.timestampLong = timestampLong;
    }

    public OffsetTimestampRec(OffsetAndTimestamp offsetAndTimestamp) {
        this.offsetLong = offsetAndTimestamp.offset();
        this.timestampLong = offsetAndTimestamp.timestamp();
    }

    public OffsetAndTimestamp offsetAndTimestamp() {
        return new OffsetAndTimestamp(this.offsetLong, this.timestampLong);
    }
}
