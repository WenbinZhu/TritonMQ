package edu.ucsd.tritonmq.producer;

import edu.ucsd.tritonmq.common.Callback;

/**
 * Created by dangyi on 5/28/17.
 */
public class Producer {
    /**
     * Create a producer
     *
     * @param zooKeeperAddr curator connect string
     */
    public Producer(String zooKeeperAddr) {

    }

    /**
     * Asynchronously send the message to broker.
     *
     * 1. Look for the group for topic
     * 2. Connect to the primary of that group and send message
     *
     * @param topic
     * @param message
     */
    public void publish(String topic, Object message, Callback callback) {

    }

    public void close() {

    }

}
