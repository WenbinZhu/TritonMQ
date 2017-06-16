package edu.ucsd.tritonmq.producer;

import edu.ucsd.tritonmq.common.Record;

import java.util.UUID;


public class ProducerRecord<T> implements Record<T> {
    private String topic;
    private T value;
    private UUID uuid;
    private int groupId = -1;

    public ProducerRecord(String topic, T value, UUID uuid) {
        this.topic = topic;
        this.value = value;
        this.uuid = uuid;
    }

    public ProducerRecord(String topic, T value) {
        this.topic = topic;
        this.value = value;
        this.uuid = UUID.randomUUID();
    }


    public void setGroupId(int groupId) {
        this.groupId = groupId;
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

    public int groupId() {
        return this.groupId;
    }
}
