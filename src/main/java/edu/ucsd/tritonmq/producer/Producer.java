package edu.ucsd.tritonmq.producer;

import edu.ucsd.tritonmq.common.Callback;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayDeque;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

import static java.util.Calendar.SECOND;

/**
 * Created by dangyi on 5/28/17.
 */
public class Producer<T> {
    private String zooKeeperAddr;
    private int numRetry;
    private int maxInFlight;
    private CuratorFramework zkClient;
    private ConcurrentLinkedQueue<ProducerRecord<T>> bufferQueue;

    /**
     * Create a producer
     *
     * @param configs producer configs including zk address etc
     */
    public Producer(Properties configs) {
        int nr = Integer.valueOf(configs.getProperty("numRetry"));
        int mif = Integer.valueOf(configs.getProperty("maxInFlight"));
        this.zooKeeperAddr = configs.getProperty("zooKeeperAddr");
        this.numRetry = Integer.min(5, Integer.max(nr, 0));
        this.maxInFlight = Integer.min(10, Integer.max(mif, 0));
        this.bufferQueue = new ConcurrentLinkedQueue<>();
        setZkClientConn();
    }

    private void setZkClientConn() {
        RetryPolicy rp = new ExponentialBackoffRetry(SECOND, 3);
        this.zkClient = CuratorFrameworkFactory
                        .builder()
                        .connectString(this.zooKeeperAddr)
                        .sessionTimeoutMs(5 * SECOND)
                        .connectionTimeoutMs(3 * SECOND)
                        .retryPolicy(rp).build();

        this.zkClient.start();
        DoSendThread<T> t = new DoSendThread<>(bufferQueue);
        new Thread(t).start();
    }

    /**
     * Asynchronously send the message to broker.
     *
     * 1. Look for the group for topic
     * 2. Connect to the primary of that group and send message
     *
     * @param record producer record
     */
    public Future<ProducerMetaRecord> publish(ProducerRecord<T> record, Callback callback) {
        // Find group number
        int groupId = record.topic().hashCode() % NumBrokerGroups;
        record.setGroupId(groupId);

        // Append to buffer queue
        bufferQueue.offer(record);

        // TODO: return future
        throw new NotImplementedException();
    }

    /**
     * Close the producer connection
     */
    public void close() {

    }

}
