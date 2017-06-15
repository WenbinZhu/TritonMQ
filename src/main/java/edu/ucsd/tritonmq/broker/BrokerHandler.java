package edu.ucsd.tritonmq.broker;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import edu.ucsd.tritonmq.producer.ProducerRecord;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;
import static edu.ucsd.tritonmq.common.Utils.*;

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
            closeResource(bai, input);
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

            String topic = prod.topic();
            BrokerRecord<?> brod = new BrokerRecord<>(topic, prod.value(), broker.incrementTs());

            synchronized (broker.backups) {
                multicast(brod);

                // Push record into queue
                if (!broker.records.containsKey(topic)) {
                    broker.records.put(topic, new ConcurrentSkipListMap<>());
                }

                broker.records.get(topic).put(brod.timestamp(), brod);
                broker.requests.put(prod.uuid(), Succ);

                resultHandler.onComplete(Succ);
            }

        } catch (Exception e) {
            resultHandler.onComplete(Fail);
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public void replicate(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {
        ByteArrayInputStream bai = null;
        ObjectInputStream input = null;
        BrokerRecord<?> brod = null;

        if (broker.isPrimary) {
            resultHandler.onError(new Exception("Not backup"));
            return;
        }

        // Parse BrokerRecord
        try {
            bai = new ByteArrayInputStream(record.array());
            input = new ObjectInputStream(bai);
            brod = (BrokerRecord<?>) input.readObject();

        } catch (IOException | ClassNotFoundException e) {
            resultHandler.onError(new Exception("Invalid record"));
            e.printStackTrace();
            return;
        } finally {
            closeResource(bai, input);
        }

        // Push to backup's queue
        try {
            if (!broker.records.containsKey(brod.topic())) {
                broker.records.put(brod.topic(), new ConcurrentSkipListMap<>());
            }

            broker.records.get(brod.topic()).put(brod.timestamp(), brod);
            resultHandler.onComplete(Succ);

        } catch (Exception e) {
            resultHandler.onComplete(Fail);
        }
    }

    private void multicast(BrokerRecord<?> record) throws Exception {
        int size = broker.backups.size();

        // No backup
        if (size == 0) {
            return;
        }

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
                            if (response.equals(Succ))
                            threadLatch.countDown();
                        }).exceptionally(cause -> {
                            cause.printStackTrace();
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
        executors.shutdownNow();
    }

    private class Count {
        volatile int count = 0;

        synchronized void increment() {
            count++;
        }
    }
}
