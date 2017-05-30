package edu.ucsd.tritonmq.producer;

import edu.ucsd.tritonmq.common.Callback;

import java.util.Properties;

/**
 * Created by dangyi on 5/28/17.
 */
public class Producer {
    private String zooKeeperAddr;

    /**
     * Create a producer
     *
     * @param configs producer configs including zk address etc
     */
    public Producer(Properties configs) {
        this.zooKeeperAddr = configs.getProperty("zooKeeperAddr");
    }

    /**
     * Asynchronously send the message to broker.
     *
     * 1. Look for the group for topic
     * 2. Connect to the primary of that group and send message
     *
     * @param record producer record
     */
    public void publish(ProducerRecord record, Callback callback) {

    }

    /**
     * Close the producer connection
     */
    public void close() {

    }

}
