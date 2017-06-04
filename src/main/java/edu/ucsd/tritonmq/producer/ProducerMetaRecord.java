package edu.ucsd.tritonmq.producer;

import edu.ucsd.tritonmq.common.MetaRecord;

import java.util.UUID;

/**
 * Created by Wenbin on 5/31/17.
 */
public class ProducerMetaRecord implements MetaRecord {
    private String topic;
    private UUID uuid;
    private boolean succ;

    public ProducerMetaRecord(String topic, UUID uuid, boolean succ) {
        this.topic = topic;
        this.uuid = uuid;
        this.succ = succ;
    }

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
