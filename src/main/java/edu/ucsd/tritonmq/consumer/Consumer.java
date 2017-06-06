package edu.ucsd.tritonmq.consumer;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.*;

import static edu.ucsd.tritonmq.common.GlobalConfig.Second;
import static edu.ucsd.tritonmq.common.GlobalConfig.SubscribePath;


/**
 * Created by dangyi on 5/28/17.
 */
public class Consumer {
    private int port;
    private String host;
    private String address;
    private String zkAddr;
    private Thread recvThread;
    private volatile boolean started;
    private CuratorFramework zkClient;
    private HashSet<String> subscription;
    private HashMap<String, Queue<ConsumerRecord<?>>> records;

    /**
     * Create a consumer
     *
     * @param configs consumer configs including zk address etc
     */
    public Consumer(Properties configs) {
        this.started = false;
        this.host = configs.getProperty("host");
        this.port = (Integer) configs.get("port");
        this.address = host + ":" + port;
        this.zkAddr = configs.getProperty("zkAddr");
        this.subscription = new HashSet<>();
        this.records = new HashMap<>();
        this.recvThread = new Thread(new RecvThread(this));

        setZkClientConn();

        assert zkClient != null;
        assert zkClient.getState() == CuratorFrameworkState.STARTED;
    }

    private void setZkClientConn() {
        RetryPolicy rp = new ExponentialBackoffRetry(Second, 2);
        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(this.zkAddr)
                .sessionTimeoutMs(Second)
                .connectionTimeoutMs(Second)
                .retryPolicy(rp).build();

        zkClient.start();
    }

    private void register(String topic) {
        String path = SubscribePath + topic + "/" + address;

        try {
            if (!subscription.contains(topic)) {
                zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path);
                subscription.add(topic);
                records.put(topic, new LinkedList<>());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void unregister(String topic) {
        String path = SubscribePath + topic + "/" + address;

        try {
            if (subscription.contains(topic)) {
                zkClient.delete().deletingChildrenIfNeeded().forPath(path);
                subscription.remove(topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Subscribe to some topics
     *
     * @param topics message topics
     */
    public void subscribe(String[] topics) {
        for (String topic : topics) {
            subscribe(topic);
        }
    }

    /**
     * Subscribe to a topic
     *
     * @param topic message topic
     */
    public void subscribe(String topic) {
        register(topic);
    }

    /**
     * unsubscribe to some topics
     *
     * @param topics message topics
     */
    public void unSubscribe(String[] topics) {
        for (String topic: topics) {
            unSubscribe(topic);
        }
    }

    /**
     * unsubscribe to a topic
     *
     * @param topic message topic
     */
    public void unSubscribe(String topic) {
        unregister(topic);
    }

    /**
     * list all subscribed topics
     *
     * @return all subscribed topics
     */
    public String[] subscription() {
        return subscription.toArray(new String[0]);
    }

    /**
     * start receiving records, use records() to get records
     */
    public synchronized void start() {
        if (started)
            return;

        recvThread.start();
    }

    /**
     * stop receiving records
     */
    public synchronized void stop() {
        try {
            recvThread.interrupt();
            recvThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        started = false;
    }

    /**
     * Get the queue with received records
     *
     * @return queue with received records
     */
    public HashMap<String, Queue<ConsumerRecord<?>>> records() {
        return records;
    }

    /**
     * List all subscribed topics
     */
    public String[] listAllTopics() {
        try {
            List<String> topics = zkClient.getChildren().forPath(SubscribePath);
            return topics.toArray(new String[0]);
        } catch (Exception e) {
            e.printStackTrace();
            return new String[0];
        }
    }

    /**
     * Close a consumer connection
     */
    void close() {

    }
}
