package edu.ucsd.tritonmq.consumer;

import edu.ucsd.tritonmq.common.Record;


public class ConsumerRecord<T> implements Record<T> {
    private String topic;
    private T value;

    public ConsumerRecord(String topic, T value) {
        this.topic = topic;
        this.value = value;
    }

    @Override
    public String topic() {
        return this.topic;
    }

    @Override
    public T value() {
        return this.value;
    }
}
