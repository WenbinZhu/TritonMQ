package edu.ucsd.tritonmq.broker;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import edu.ucsd.tritonmq.producer.ProducerRecord;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

public class BrokerHandler implements BrokerService.AsyncIface {
    private  Broker broker;

    public BrokerHandler(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void send(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {
        ByteArrayInputStream bai = null;
        ObjectInputStream input = null;
        ProducerRecord<?> prod = null;

        // Check if primary gets the request
        if (!broker.isPrimary) {
            resultHandler.onError(new Exception("Not primary, primary may failed"));
            return;
        }

        // Parse request
        try {
            bai = new ByteArrayInputStream(record.array());
            input = new ObjectInputStream(bai);
            prod = (ProducerRecord<?>) input.readObject();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return;
        } finally {
            close(bai, input);
        }

        try {
            // Handle duplicate client requests
            synchronized (broker.requests) {
                String status = broker.requests.get(prod.uuid());

                if (status == null) {
                    broker.requests.put(prod.uuid(), Pend);
                } else {
                    resultHandler.onComplete(status);
                    return;
                }
            }

            synchronized (this) {
                // Push record into queue
                String topic = prod.topic();
                BrokerRecord<?> brod = new BrokerRecord<>(topic, prod.value(), broker.incrementTs());

                synchronized (broker.backups) {
                    if (!getBackupResponse(brod)) {
                        resultHandler.onComplete(Fail);
                        return;
                    }

                    if (!broker.records.containsKey(topic)) {
                        broker.records.put(topic, new ConcurrentLinkedDeque<>());
                    }

                    broker.records.get(topic).offer(brod);
                    broker.requests.put(prod.uuid(), Succ);

                    resultHandler.onComplete(Succ);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            resultHandler.onComplete(Fail);
            System.exit(1);
        }
    }

    @Override
    public void migrate(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {

    }

    @Override
    public void replicate(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {
        resultHandler.onComplete(Succ);
    }

    private void close(Closeable... resource) {
        for (Closeable res : resource) {
            try {
                if (res != null)
                    res.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean getBackupResponse(BrokerRecord<?> record) throws Exception {
        int size = broker.backups.size();
        Count count = new Count();
        CountDownLatch latch = new CountDownLatch(size);
        ExecutorService executors = Executors.newFixedThreadPool(size);
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(bao);
        output.writeObject(record);
        ByteBuffer bytes = ByteBuffer.wrap(bao.toByteArray());

        for (String backup : broker.backups) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        CountDownLatch threadLatch = new CountDownLatch(1);
                        String backupAddr = "tbinary+http://" + backup;
                        ThriftCompletableFuture<String> future = new ThriftCompletableFuture<>();
                        BrokerService.AsyncIface client = Clients.newClient(backupAddr, BrokerService.AsyncIface.class);
                        client.replicate(bytes, future);

                        future.thenAccept(response -> {
                            count.increment();
                            threadLatch.countDown();
                        }).exceptionally(cause -> {
                            cause.printStackTrace();
                            count.increment();
                            threadLatch.countDown();
                            return null;
                        });

                        threadLatch.await();
                        latch.countDown();

                    } catch (Exception e) {
                        latch.countDown();
                        e.printStackTrace();
                    }
                }
            });
        }

        latch.await();

        return count.count == size;
    }

    private class Count {
        volatile int count = 0;

        synchronized void increment() {
            count++;
        }
    }
}
