package edu.ucsd.tritonmq.consumer;

import edu.ucsd.tritonmq.common.Callback;
import edu.ucsd.tritonmq.common.TopicInfo;

/**
 * Created by dangyi on 5/28/17.
 */
public class Consumer {
    /**
     * Create a consumer
     *
     * @param zooKeeperAddr curator connect string
     */
    Consumer(String zooKeeperAddr) {

    }

    /**
     * Asynchronously subscribe to a topic
     *
     * 1. setup and start an RPC server
     * 2. register itself's address to ZooKeeper
     *
     * @param topic message topic
     * @param callback callback upon message received, will only be called at
     *                 most once at a time
     */
    void subscribe(String topic, Callback callback) {

    }

    /**
     * unsubscribe to a topic
     *
     * @param topic message topic
     */
    void unSubscribe(String topic) {

    }

    /**
     * List all subscribed topics
     */
    TopicInfo[] listTopics() {
        throw new UnsupportedOperationException();
    }

    /**
     * Close a consumer connection
     */
    void close() {

    }
}
