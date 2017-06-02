package edu.ucsd.tritonmq.producer;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import edu.ucsd.tritonmq.broker.BrokerService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;
import static java.util.Calendar.SECOND;

public class SendThread<T> extends Thread {
    private int numRetry;
    private int maxInFlight;
    private String zkAddr;
    private String[] groupLeaders;
    private CuratorFramework zkClient;
    private ExecutorService executors;
    private BrokerService.AsyncIface[] leaderClients;
    private ConcurrentLinkedQueue<ProducerRecord<T>> bufferQueue;
    private Map<ProducerRecord, CompletableFuture<ProducerMetaRecord>> futureMaps;

    SendThread(int numRetry, int maxInFlight, String zkAddr) {
        this.numRetry = numRetry;
        this.maxInFlight = maxInFlight;
        this.zkAddr = zkAddr;
        this.groupLeaders = new String[NumBrokerGroups];
        this.bufferQueue = new ConcurrentLinkedQueue<>();
        this.executors = Executors.newFixedThreadPool(maxInFlight);
        setZkClientConn();
        initGroupLeaders();

        // TODO: Get group leaders

    }

    private void setZkClientConn() {
        RetryPolicy rp = new ExponentialBackoffRetry(SECOND, 3);
        this.zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(this.zkAddr)
                .sessionTimeoutMs(5 * SECOND)
                .connectionTimeoutMs(3 * SECOND)
                .retryPolicy(rp).build();

        this.zkClient.start();

    }

    private void initGroupLeaders() {
        // TODO: Init group leader addresses

        // TODO: Init group leader clients

    }

    private void updateGroupLeader(int groupId, String leaderAddr) {
        // TODO: update group leader
    }

    public void send(ProducerRecord<T> record, CompletableFuture<ProducerMetaRecord> future) {
        futureMaps.put(record, future);
        bufferQueue.offer(record);
    }

    @Override
    public void run() {
        while (true) {
            if (Thread.interrupted()) {
                break;
            }

            for (int i = 0; i < maxInFlight; i++) {
                ProducerRecord<T> record = bufferQueue.poll();
                if (record == null)
                    break;
                executors.execute(new SendHandler(record));
            }
        }
    }

    public void close() {
        executors.shutdown();
        zkClient.close();
    }

    private class SendHandler extends Thread {
        private ProducerRecord<T> record;

        SendHandler(ProducerRecord<T> record) {
            this.record = record;
        }

        @Override
        public void run() {
            for (int i = 0; i < numRetry; i++) {
                int groupID = record.groupId();
                ThriftCompletableFuture<String> future = new ThriftCompletableFuture<>();

                try {
                    ByteArrayOutputStream bao = new ByteArrayOutputStream();
                    ObjectOutputStream output = new ObjectOutputStream(bao);
                    output.writeObject(record);
                    byte[] bytes = bao.toByteArray();

                    leaderClients[groupID].send(ByteBuffer.wrap(bytes), future);

                    // TODO: break if received succ

                } catch (TException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
