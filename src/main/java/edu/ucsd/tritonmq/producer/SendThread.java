package edu.ucsd.tritonmq.producer;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import edu.ucsd.tritonmq.broker.BrokerService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;
import static edu.ucsd.tritonmq.common.Utils.*;

/**
 * Created by Wenbin on 5/31/17.
 */
public class SendThread<T> extends Thread {
    private int timeout;
    private int numRetry;
    private int maxInFlight;
    private String zkAddr;
    private CuratorFramework zkClient;
    private ExecutorService executors;
    private PathChildrenCache primaryMonitor;
    private BrokerService.AsyncIface[] primaryClients;
    private ConcurrentLinkedQueue<ProducerRecord<T>> bufferQueue;
    private Map<ProducerRecord, CompletableFuture<ProducerMetaRecord>> futureMap;

    SendThread(int timeout, int numRetry, int maxInFlight, String zkAddr) {
        this.timeout = timeout;
        this.numRetry = numRetry;
        this.maxInFlight = maxInFlight;
        this.zkAddr = zkAddr;
        this.futureMap = new HashMap<>();
        this.bufferQueue = new ConcurrentLinkedQueue<>();
        this.executors = Executors.newFixedThreadPool(maxInFlight);
        this.primaryClients = new BrokerService.AsyncIface[NumBrokerGroups];
        this.zkClient = initZkClient(Second, 1, this.zkAddr, Second, Second);
        this.zkClient.start();

        initPrimaryListener();

        assert zkClient != null;
        assert zkClient.getState() == CuratorFrameworkState.STARTED;
    }

    /**
     * Get notified if any group primary changes
     */
    private void initPrimaryListener() {
        PathChildrenCacheListener plis = (client, event) -> {
            switch (event.getType()) {
                case CHILD_UPDATED: {
                    String[] segments = event.getData().getPath().split("/");
                    int groupId = Integer.valueOf(segments[segments.length - 1]);
                    updatePrimary(groupId);
                    break;
                }
            }
        };

        String primaryPath = "/primary";
        primaryMonitor = new PathChildrenCache(zkClient, primaryPath, false);
        try {
            primaryMonitor.start();
            primaryMonitor.getListenable().addListener(plis);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Update the connection to a group primary
     *
     * @param groupId the group number to be updated
     */
    private void updatePrimary(int groupId) {
        String primaryPath = "/primary/" + String.valueOf(groupId);
        try {
            String primaryUrl = new String(zkClient.getData().forPath(primaryPath));
            primaryUrl = "tbinary+http://" + primaryUrl + "/send";

            BrokerService.AsyncIface client = Clients.newClient(primaryUrl, BrokerService.AsyncIface.class);
            primaryClients[groupId] = client;

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Push record to the buffer queue
     *
     * @param record the producer generated record
     * @param future the producer future to be updated when the record is actually sent
     */
    public void send(ProducerRecord<T> record, CompletableFuture<ProducerMetaRecord> future) {
        futureMap.put(record, future);
        bufferQueue.offer(record);
    }

    @Override
    public void run() {
        CountDownLatch executorLatch = new CountDownLatch(maxInFlight);

        while (true) {
            if (Thread.interrupted()) {
                break;
            }

            for (int i = 0; i < maxInFlight; i++) {
                ProducerRecord<T> record = bufferQueue.poll();

                if (record == null) {
                    executorLatch.countDown();
                    continue;
                }

                executors.execute(new SendHandler(record, executorLatch));
            }

            try {
                executorLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Close the send thread
     */
    public void close() {
        executors.shutdown();
        zkClient.close();
    }

    /**
     * Handler thread for sending a record
     */
    private class SendHandler extends Thread {
        private int groupId;
        private ProducerRecord<T> record;
        private CountDownLatch executorLatch;
        private volatile boolean done = false;

        SendHandler(ProducerRecord<T> record, CountDownLatch executorLatch) {
            this.record = record;
            this.groupId = record.groupId();
            this.executorLatch = executorLatch;
        }

        public void done() {
            done = true;
        }

        @Override
        public void run() {
            for (int i = 0; i < numRetry + 1 && !done; i++) {
                CountDownLatch sendLatch = new CountDownLatch(1);
                try {
                    ThriftCompletableFuture<String> future = new ThriftCompletableFuture<>();
                    ByteArrayOutputStream bao = new ByteArrayOutputStream();
                    ObjectOutputStream output = new ObjectOutputStream(bao);
                    output.writeObject(record);
                    byte[] bytes = bao.toByteArray();

                    if (primaryClients[groupId] == null)
                        updatePrimary(groupId);

                    primaryClients[groupId].send(ByteBuffer.wrap(bytes), future);

                    future.thenAccept(response -> {
                        if (response.equals(Succ)) {
                            done();
                            sendLatch.countDown();
                        }
                    })
                    .exceptionally(cause -> {
                        sendLatch.countDown();
                        return null;
                    });

                    sendLatch.await(timeout, TimeUnit.MILLISECONDS);

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (done) {
                ProducerMetaRecord metaRecord = new ProducerMetaRecord(record.topic(), record.uuid(), true);
                futureMap.get(record).complete(metaRecord);
            } else {
                ProducerMetaRecord metaRecord = new ProducerMetaRecord(record.topic(), record.uuid(), false);
                futureMap.get(record).complete(metaRecord);
            }

            // Should remove then countdown
            futureMap.remove(record);
            System.out.println(done);
            executorLatch.countDown();
        }
    }
}
