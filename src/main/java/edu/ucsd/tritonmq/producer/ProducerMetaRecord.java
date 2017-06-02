package edu.ucsd.tritonmq.producer;

import edu.ucsd.tritonmq.common.MetaRecord;

import java.util.UUID;

public class ProducerMetaRecord implements MetaRecord {
    private String topic;
    private UUID uuid;
    private boolean succ;

    @Override
    public String topic() {
        return this.topic;
    }

    public UUID uuid() {
        return this.uuid;
    }

    public boolean succ() {
        return this.succ;
    }
}
