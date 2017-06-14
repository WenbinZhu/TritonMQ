package edu.ucsd.tritonmq.broker;

import edu.ucsd.tritonmq.common.Record;

public class BrokerRecord<T> implements Record<T> {
    private String topic;
    private T value;
    private long timestamp;

    public BrokerRecord (String topic, T value, long timestamp) {
        this.topic = topic;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public T value() {
        return value;
    }

    public long timestamp() {
        return timestamp;
    }
}
