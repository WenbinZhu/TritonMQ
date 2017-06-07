package edu.ucsd.tritonmq.consumer;

import com.linecorp.armeria.common.SerializationFormat;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.thrift.THttpService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.net.InetSocketAddress;
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
    private Server server;
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
                System.out.println("Subscribed to topic: " + topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void unRegister(String topic) {
        String path = SubscribePath + topic + "/" + address;

        try {
            if (subscription.contains(topic)) {
                zkClient.delete().deletingChildrenIfNeeded().forPath(path);
                subscription.remove(topic);
                System.out.println("unSubscribed to topic: " + topic);
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
        unRegister(topic);
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

        InetSocketAddress isa = new InetSocketAddress(host, port);
        ServerBuilder sb = new ServerBuilder();
        sb.port(isa, SessionProtocol.HTTP).serviceAt("/deliver",
                THttpService.of(new RecvThread(records), SerializationFormat.THRIFT_BINARY));

        server =  sb.build();
        server.start();
    }

    /**
     * stop receiving records from all topic
     */
    public synchronized void stop() {
        unSubscribe(subscription());
        server.stop();
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
