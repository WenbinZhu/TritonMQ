package edu.ucsd.tritonmq.broker;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

public class DeliverThread extends Thread {
    private Broker broker;

    public DeliverThread(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void run() {
        while (true) {
            Set<String> topics = broker.records.keySet();

            // Initialize consumer offsets
            topics.forEach(topic -> {
                String topicPath = new File(SubscribePath, topic).toString();

                if (!broker.offsets.containsKey(topic)) {
                    try {
                        List<String> consumers = broker.zkClient.getChildren().forPath(topicPath);

                        broker.offsets.put(topic, new ConcurrentHashMap<>());
                        consumers.forEach(consumer -> {
                            broker.offsets.get(topic).put(consumer, 0);
                        });

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });


        }
    }
}
