package edu.ucsd.tritonmq.broker;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

public class DeliverThread extends Thread {
    private Broker broker;
    protected Map<String, Map<String, Long>> offsets;

    public DeliverThread(Broker broker) {
        this.broker = broker;
        this.offsets = new ConcurrentHashMap<>();
    }

    @Override
    public void run() {
        while (true) {
            Set<String> topics = broker.records.keySet();

            // Initialize consumer offsets
            try {
                for (String topic : topics) {
                    String topicPath = new File(SubscribePath, topic).toString();

                    if (broker.zkClient.checkExists().forPath(topicPath) == null)
                        broker.zkClient.create().creatingParentsIfNeeded().forPath(topicPath);

                    if (!offsets.containsKey(topic))
                        offsets.put(topic, new ConcurrentHashMap<>());

                    List<String> consumers = broker.zkClient.getChildren().forPath(topicPath);

                    for (String consumer : consumers) {
                        String consumerPath = new File(topicPath, consumer).toString();
                        byte[] data = broker.zkClient.getData().forPath(consumerPath);
                        Long offset = data == null ? 0 : Long.valueOf(new String(data));

                        offsets.get(topic).put(consumer, offset);
                    }
                }

                Thread.sleep(10);

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }


        }
    }
}
