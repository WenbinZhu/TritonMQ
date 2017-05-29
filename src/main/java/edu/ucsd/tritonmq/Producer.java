package edu.ucsd.tritonmq;

/**
 * Created by dangyi on 5/28/17.
 */
public class Producer {
    /**
     *
     * @param zooKeeperAddr curator connect string
     */
    Producer(String zooKeeperAddr) {

    }

    /**
     * Asynchronous method.
     *
     * 1. Look for the group for topic
     * 2. Connect to the primary of that group and send message
     *
     * @param topic
     * @param message
     */
    void publish(String topic, Object message) {

    }

}
