package edu.ucsd.tritonmq.broker;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import edu.ucsd.tritonmq.consumer.ConsumerRecord;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

public class DeliverThread extends Thread {
    private Broker broker;
    private Map<String, Map<String, Long>> offsets;
    private ExecutorService executors;

    DeliverThread(Broker broker) {
        this.broker = broker;
        this.offsets = new ConcurrentHashMap<>();
        this.executors = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
        while (true) {
            Set<String> topics = broker.records.keySet();

            try {
                // Initialize consumer offsets
                for (String topic : topics) {
                    if (offsets.containsKey(topic)) continue;

                    String topicPath = new File(SubscribePath, topic).toString();
                    offsets.put(topic, new ConcurrentHashMap<>());

                    if (broker.zkClient.checkExists().forPath(topicPath) == null) {
                        broker.zkClient.create().creatingParentsIfNeeded().forPath(topicPath, null);
                    }

                    executors.execute(new DeliverHandler(topic));
                }

                Thread.sleep(20);

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    private class DeliverHandler extends Thread {
        private String topic;
        private ExecutorService executors;

        DeliverHandler(String topic) {
            this.topic = topic;
            this.executors = Executors.newCachedThreadPool();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String topicPath = new File(SubscribePath, topic).toString();
                    List<String> consumers = broker.zkClient.getChildren().forPath(topicPath);

                    for (String consumer : consumers) {
                        if (offsets.get(topic).containsKey(consumer)) continue;

                        String consumerPath = new File(topicPath, consumer).toString();
                        byte[] data = broker.zkClient.getData().forPath(consumerPath);
                        Long offset = data == null ? 0 : Long.valueOf(new String(data));

                        // Consumer cannot get records before it subscribes
                        if (broker.records.containsKey(topic)) {
                            offset = broker.largestTimeStamp(topic);
                            broker.zkClient.setData().forPath(consumerPath, offset.toString().getBytes());
                        }

                        offsets.get(topic).put(consumer, offset);
                        executors.execute(new SendHandler(topic, consumer));
                    }

                    Thread.sleep(20);

                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    private class SendHandler extends Thread {
        private String topic;
        private String consumer;
        private ConcurrentSkipListMap<Long, BrokerRecord<?>> skipList;

        SendHandler(String topic, String consumer) {
            this.topic = topic;
            this.consumer = consumer;
            this.skipList = broker.records.get(topic);
        }

        @Override
        public void run() {
            while (true) {
                try {
                    CountDownLatch latch = new CountDownLatch(1);
                    Long offset = offsets.get(topic).get(consumer);
                    assert offset != null;

                    Map.Entry<Long, BrokerRecord<?>> next;

                    if (offset == 0) {
                        next = skipList.firstEntry();
                    } else {
                        next = skipList.higherEntry(offset);
                    }

                    if (next == null) {
                        Thread.sleep(20);
                        continue;
                    }

                    BrokerRecord<?> brod = next.getValue();
                    ConsumerRecord<?> record = new ConsumerRecord<>(brod.topic(), brod.value());
                    ByteArrayOutputStream bao = new ByteArrayOutputStream();
                    ObjectOutputStream output = new ObjectOutputStream(bao);
                    output.writeObject(record);
                    ByteBuffer bytes = ByteBuffer.wrap(bao.toByteArray());

                    String consumerAddr = "tbinary+http://" + consumer + "/deliver";
                    ThriftCompletableFuture<String> future = new ThriftCompletableFuture<>();
                    ConsumerService.AsyncIface client = Clients.newClient(consumerAddr, ConsumerService.AsyncIface.class);
                    client.deliver(bytes, future);

                    future.thenAccept(response -> {
                        if (response.equals(Succ)) {
                            latch.countDown();
                            updateOffset(brod.timestamp());
                        }
                    }).exceptionally(cause -> {
                        // cause.printStackTrace();
                        latch.countDown();
                        return null;
                    });

                    latch.await();
                    Thread.sleep(20);

                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }

        private void updateOffset(Long timestamp) {
            String topicPath = new File(SubscribePath, topic).toString();
            String consumerPath = new File(topicPath, consumer).toString();

            try {
                broker.zkClient.setData().forPath(consumerPath, timestamp.toString().getBytes());
                offsets.get(topic).put(consumer, timestamp);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
    }
}