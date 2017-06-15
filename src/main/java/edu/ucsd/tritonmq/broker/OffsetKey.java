package edu.ucsd.tritonmq.broker;

public class OffsetKey {
    String topic;
    String consumer;
    Long offset;

    public OffsetKey(String topic, String consumer, Long offset) {
        this.topic = topic;
        this.consumer = consumer;
        this.offset = offset;
    }

    @Override
    public int hashCode() {
        int result = 17;

        result = 31 * result + topic.hashCode();
        result = 31 * result + consumer.hashCode();
        result = 31 * result + offset.hashCode();

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || getClass() != obj.getClass())
            return false;

        if(this == obj)
            return true;

        OffsetKey key = (OffsetKey) obj;

        return topic.equals(key.topic) && consumer.equals(key.consumer) && offset.equals(key.offset);

    }
}

