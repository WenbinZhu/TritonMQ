package edu.ucsd.tritonmq.producer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

/**
 * Created by dangyi on 5/28/17.
 */
public class Producer<T> {
    private int retry;
    private int timeout;
    private int maxInFlight;
    private String zkAddr;
    private SendThread sendThread;

    /**
     * Create a producer
     *
     * @param configs producer configs including zk address etc
     */
    public Producer(Properties configs) {
        int nr = (Integer) configs.get("retry");
        int mif = (Integer) configs.get("maxInFlight");

        if (nr < 0 || mif < 0) {
            throw new IllegalArgumentException("retry and maxInFlight cannot be negative");
        }

        this.timeout = (Integer) configs.get("timeout");
        this.zkAddr = configs.getProperty("zkAddr");
        this.retry = Integer.min(5, Integer.max(nr, 0));
        this.maxInFlight = Integer.min(10, Integer.max(mif, 0));
        this.sendThread = new SendThread(timeout, retry, maxInFlight, zkAddr);

        sendThread.start();
    }

    /**
     * Asynchronously publish the message to broker
     * and return a future to user
     *
     * @param record producer generated record
     */
    public CompletableFuture<ProducerMetaRecord> publish(ProducerRecord<T> record) {
        // Find group number
        int groupId = (record.topic().hashCode() & 0x7fffffff) % NumBrokerGroups;
        record.setGroupId(groupId);

        // Construct future
        CompletableFuture<ProducerMetaRecord> future = new CompletableFuture<>();

        // Append to buffer queue
        sendThread.send(record, future);

        // Return future
        return future;
    }

    /**
     * Close the producer connection
     */
    public void close() {
        try {
            sendThread.interrupt();
            sendThread.close();
            sendThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Properties configs = new Properties();
        configs.put("retry", 2);
        configs.put("timeout", 500);
        configs.put("maxInFlight", 4);
        configs.put("zkAddr", ZkAddr);

        Producer<String> producer = new Producer<>(configs);
        ProducerRecord<String> record1 = new ProducerRecord<>("test topic", "test message");
        ProducerRecord<String> record2 = new ProducerRecord<>("test topic", "test message");
        ProducerRecord<String> record3 = new ProducerRecord<>("next topic", "test message");
        // ProducerRecord<String> record4 = new ProducerRecord<>("test topic", "test message");
        // ProducerRecord<String> record5 = new ProducerRecord<>("test topic", "test message");
        // ProducerRecord<String> record6 = new ProducerRecord<>("test topic", "test message");

        CompletableFuture<ProducerMetaRecord> future1 = producer.publish(record1);
        CompletableFuture<ProducerMetaRecord> future2 = producer.publish(record2);

        future1.thenAccept(meta -> {
            System.out.println(meta.topic() + ", " + meta.succ());
        });

        future2.thenAccept(meta -> {
            System.out.println(meta.topic() + ", " + meta.succ());
        });

        Thread.sleep(2000);

        CompletableFuture<ProducerMetaRecord> future3 = producer.publish(record3);

        future3.thenAccept(meta -> {
            System.out.println(meta.topic() + ", " + meta.succ());
        });
    }
}
