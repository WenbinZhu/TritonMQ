package edu.ucsd.tritonmq.producer;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import edu.ucsd.tritonmq.broker.BrokerService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;
import static edu.ucsd.tritonmq.common.Utils.*;


public class SendThread extends Thread {
    private int retry;
    private int timeout;
    private int maxInFlight;
    private String zkAddr;
    private CuratorFramework zkClient;
    private ExecutorService executors;
    private PathChildrenCache primaryMonitor;
    private BrokerService.AsyncIface[] primaryClients;
    private BlockingQueue<ProducerRecord<?>> bufferQueue;
    private Map<ProducerRecord, CompletableFuture<ProducerMetaRecord>> futureMap;

    SendThread(int timeout, int retry, int maxInFlight, String zkAddr) {
        this.retry = retry;
        this.timeout = timeout;
        this.maxInFlight = maxInFlight;
        this.zkAddr = zkAddr;
        this.futureMap = new HashMap<>();
        this.bufferQueue = new LinkedBlockingQueue<>();
        this.executors = Executors.newFixedThreadPool(maxInFlight);
        this.primaryClients = new BrokerService.AsyncIface[NumBrokerGroups];
        this.zkClient = initZkClient(Second, 1, this.zkAddr, Second, Second);

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
                case CHILD_ADDED:
                case CHILD_UPDATED: {
                    String last = ZKPaths.getNodeFromPath(event.getData().getPath());
                    int groupId = Integer.valueOf(last);
                    updatePrimary(groupId);
                    break;
                }
            }
        };

        String path = new File(PrimaryPath, "").toString();
        primaryMonitor = new PathChildrenCache(zkClient, path, false);

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
        String path = new File(PrimaryPath, String.valueOf(groupId)).toString();
        try {
            String primaryAddr = new String(zkClient.getData().forPath(path));
            primaryAddr = "tbinary+http://" + primaryAddr;

            BrokerService.AsyncIface client = Clients.newClient(primaryAddr, BrokerService.AsyncIface.class);
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
    public void send(ProducerRecord<?> record, CompletableFuture<ProducerMetaRecord> future) {
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

            try {
                for (int i = 0; i < maxInFlight; i++) {
                    ProducerRecord<?> record = bufferQueue.poll(1, TimeUnit.DAYS);

                    if (record == null) {
                        i--;
                        continue;
                    }

                    executors.execute(new SendHandler(record, executorLatch));
                }

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
        private ProducerRecord<?> record;
        private CountDownLatch executorLatch;
        private volatile boolean done = false;

        SendHandler(ProducerRecord<?> record, CountDownLatch executorLatch) {
            this.record = record;
            this.groupId = record.groupId();
            this.executorLatch = executorLatch;
        }

        public synchronized void done() {
            done = true;
        }

        @Override
        public void run() {
            for (int i = 0; i < retry + 1 && !done; i++) {
                CountDownLatch sendLatch = new CountDownLatch(1);
                ByteArrayOutputStream bao = null;
                ObjectOutputStream output = null;

                try {
                    ThriftCompletableFuture<String> future = new ThriftCompletableFuture<>();
                    bao = new ByteArrayOutputStream();
                    output = new ObjectOutputStream(bao);
                    output.writeObject(record);
                    ByteBuffer bytes = ByteBuffer.wrap(bao.toByteArray());

                    if (primaryClients[groupId] == null)
                        updatePrimary(groupId);

                    primaryClients[groupId].send(bytes, future);

                    future.thenAccept(response -> {
                        if (response.equals(Succ)) {
                            done();
                            sendLatch.countDown();
                        }
                    }).exceptionally(cause -> {
                        sendLatch.countDown();
                        return null;
                    });

                    sendLatch.await(timeout, TimeUnit.MILLISECONDS);

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    closeResource(bao, output);
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
            executorLatch.countDown();
        }
    }
}
