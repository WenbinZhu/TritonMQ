package edu.ucsd.tritonmq.producer;

import edu.ucsd.tritonmq.common.CommonRecord;

import java.util.UUID;

public class ProducerRecord<T> implements CommonRecord<T> {
    private String topic;
    private T value;
    private UUID uuid;

    public ProducerRecord(String topic, T value, UUID uuid) {
        this.topic = topic;
        this.value = value;
        this.uuid = uuid;
    }

    @Override
    public String topic() {
        return this.topic;
    }

    @Override
    public T value() {
        return this.value;
    }

    public UUID uuid() {
        return this.uuid;
    }
}
