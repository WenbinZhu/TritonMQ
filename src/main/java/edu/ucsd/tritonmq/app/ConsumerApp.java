package edu.ucsd.tritonmq.app;

import edu.ucsd.tritonmq.consumer.Consumer;
import edu.ucsd.tritonmq.consumer.ConsumerRecord;
import it.unimi.dsi.fastutil.objects.Object2BooleanArrayMap;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;

/**
 * Created by dangyi on 6/14/17.
 */
public class ConsumerApp {
    public static void main(String[] args) throws InterruptedException {
        Properties configs = new Properties();

        configs.put("host", "localhost");
        configs.put("port", 5001);
        configs.put("zkAddr", ZkAddr);

        configs.put("topicCount", 15);


        for (String arg : args) {
            if (arg.substring(0, 2).equals("--")) {
                String[] splits = arg.substring(2).split("=");
                try {
                    configs.put(splits[0], Integer.parseInt(splits[1]));
                } catch (NumberFormatException e) {
                    configs.put(splits[0], splits[1]);
                }
            }
        }

        int topicCount = (Integer) configs.get("topicCount");
        Consumer consumer = new Consumer(configs);
        Thread[] threads = new Thread[topicCount];
        HashMap<String, BlockingQueue<ConsumerRecord<?>>> records = consumer.records();

        for (int i = 0; i < topicCount; i++) {
            String topic = "topic" + i;
            consumer.subscribe(topic);

            threads[i] = new Thread(() -> {
                while (true) {
                    try {
                        ConsumerRecord<?> record = records.get(topic).poll(30, TimeUnit.SECONDS);
                        if (record != null) {
                            String value = (String) record.value();
                            System.out.println("[" + topic + "] " + value.substring(0, 10));
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < topicCount; i++) {
            threads[i].join();
        }
    }
}
