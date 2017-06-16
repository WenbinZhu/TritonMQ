package edu.ucsd.tritonmq.broker;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

public class PurgeThread extends Thread {
    Broker broker;

    PurgeThread(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(60 * Second);

                synchronized (broker.records) {
                    for (Map.Entry<String, ConcurrentSkipListMap<Long, BrokerRecord<?>>> entry : broker.records.entrySet()) {
                        String topic = entry.getKey();
                        List<Long> offsets = new ArrayList<>();
                        String topicPath = new File(SubscribePath, topic).toString();
                        List<String> consumers = broker.zkClient.getChildren().forPath(topicPath);

                        for (String consumer : consumers) {
                            String consumerPath = new File(topicPath, consumer).toString();
                            byte[] data = broker.zkClient.getData().forPath(consumerPath);

                            if (data == null)
                                continue;

                            offsets.add(Long.valueOf(new String(data)));
                        }

                        long largest = offsets.size() == 0 ? 0 : Collections.min(offsets);
                        ConcurrentSkipListMap<Long, BrokerRecord<?>> skipList = entry.getValue();

                        for (Long timestamp : skipList.keySet()) {
                            if (timestamp < largest)
                                skipList.remove(timestamp);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
