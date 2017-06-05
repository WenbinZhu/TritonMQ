package edu.ucsd.tritonmq.consumer;

import edu.ucsd.tritonmq.common.Callback;
import edu.ucsd.tritonmq.common.TopicInfo;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.Properties;

import static edu.ucsd.tritonmq.common.GlobalConfig.Second;

/**
 * Created by dangyi on 5/28/17.
 */
public class Consumer {
    private int port;
    private String host;
    private String address;
    private String zkAddr;
    private String zkPath;
    private CuratorFramework zkClient;

    /**
     * Create a consumer
     *
     * @param configs consumer configs including zk address etc
     */
    public Consumer(Properties configs) {
        this.host = configs.getProperty("host");
        this.port = (Integer) configs.get("port");
        this.address = host + "::" + port;
        this.zkAddr = configs.getProperty("zkAddr");
        this.zkPath = "/consumer/" + address + "_";
        setZkClientConn();
        register();
    }

    /**
     * Set connection to Zookeeper
     */
    private void setZkClientConn() {
        RetryPolicy rp = new ExponentialBackoffRetry(Second, 3);
        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(this.zkAddr)
                .sessionTimeoutMs(Second)
                .connectionTimeoutMs(3 * Second)
                .retryPolicy(rp).build();

        zkClient.start();
    }

    private void register() {
        try {
            zkClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
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
