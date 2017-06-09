package edu.ucsd.tritonmq.broker;

import edu.ucsd.tritonmq.producer.ProducerRecord;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Deque;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;
import static edu.ucsd.tritonmq.common.Utils.*;


/**
 * Created by dangyi on 5/28/17.
 */
public class Broker {
    private int groupId;
    private int retry;
    private int timeout;
    private int port;
    private String host;
    private String address;
    private String zkAddr;
    private LeaderLatch latch;
    private CuratorFramework zkClient;
    private volatile boolean started;
    private Map<String, Deque<ProducerRecord<?>>> records;

    /**
     * Create a new broker in a specific group.
     *
     * @param groupId which group this broker belongs to
     * @param configs broker configs including zk address etc
     */
    public Broker(int groupId, Properties configs) {
        int nr = (Integer) configs.get("retry");
        this.groupId = groupId;
        this.started = false;
        this.host = configs.getProperty("host");
        this.port = (Integer) configs.get("port");
        this.address = host + ":" + port;
        this.zkAddr = configs.getProperty("zkAddr");
        this.timeout = (Integer) configs.get("timeout");
        this.retry = Integer.min(5, Integer.max(nr, 0));
        this.records = new ConcurrentHashMap<>();
        this.zkClient = initZkClient(Second, 1, this.zkAddr, 100, 100);
        this.zkClient.start();

        assert zkClient != null;
        assert zkClient.getState() == CuratorFrameworkState.STARTED;

        register();
    }

    public void register() {
        // TODO: add listener

        try {
            latch = new LeaderLatch(zkClient, ReplicaPath + String.valueOf(groupId), address);
            latch.addListener(new LeaderLatchListener() {

                public void notLeader() {
                    System.out.println(address + " not leader");
                }

                public void isLeader() {
                    System.out.println(address+ " is leader");
                }
            });

            latch.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
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
    public synchronized void start() {
        if (started)
            return;

        throw new NotImplementedException();
    }

    /**
     *
     * @return the address this broker listens
     */
    public String getListenAddr() {
        throw new NotImplementedException();
    }
}
