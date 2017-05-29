package edu.ucsd.tritonmq;

import org.apache.curator.CuratorZookeeperClient;

import java.net.InetSocketAddress;

/**
 * Created by dangyi on 5/28/17.
 */
public class Broker {
    /**
     * Create a new broker in a specific group. ZooKeeper will automatically
     * selects a leader.
     *
     * @param zooKeeperAddr curator connect string
     * @param groupId which group this broker belongs to
     */
    Broker(String zooKeeperAddr, int groupId) {

    }

    /**
     * 1. Create a ephemeral node under /groups/1/replica/
     * 2. Run for leader
     * 3. If becomes leader, create /groups/1/primary and listen for producers
     * 4. Otherwise, listen for primary
     */
    void start() {

    }
}
