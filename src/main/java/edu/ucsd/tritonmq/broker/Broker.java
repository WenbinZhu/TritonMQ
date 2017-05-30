package edu.ucsd.tritonmq.broker;

import org.apache.curator.CuratorZookeeperClient;

import java.net.InetSocketAddress;

/**
 * Created by dangyi on 5/28/17.
 */
public class Broker extends BrokerService {
    /**
     * Create a new broker in a specific group. ZooKeeper will automatically
     * selects the leader.
     *
     * 1. Create a connection to ZooKeeper
     *
     * @param zooKeeperAddr curator connect string
     * @param groupId which group this broker belongs to
     */
    public Broker(String zooKeeperAddr, int groupId) {
    }

    /**
     * Start the broker. It should be able to serve requests upon return.
     *
     * 1. Create a ephemeral node under /groups/1/replica/
     * 2. Run for leader
     * 3. If becomes leader
     *    1. create ephemeral /groups/1/primary
     *    2. listen for producers asynchronously
     *    3. listen for new-joined backups
     * 4. Otherwise, listen for primary asynchronously
     */
    public void start() {
        throw new UnsupportedOperationException();
    }

    /**
     * Stop the broker
     *
     * 1. Close the ZooKeeper connection.
     */
    public void stop() {
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @return the address this broker listens
     */
    public String getListenAddr() {
        throw new UnsupportedOperationException();
    }
}
