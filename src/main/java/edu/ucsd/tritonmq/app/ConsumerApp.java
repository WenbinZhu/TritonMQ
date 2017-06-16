package edu.ucsd.tritonmq.app;

import edu.ucsd.tritonmq.consumer.Consumer;
import edu.ucsd.tritonmq.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;


public class ConsumerApp {
    public static void main(String[] args) throws InterruptedException {
        createConsumer(args, record -> {
            String value = (String) record.value();
            System.out.println("[" + record.topic() + "] " + value.split(" ")[0]);
        });
    }

    public static Consumer createConsumer(String[] args, java.util.function.Consumer<ConsumerRecord> cb) throws InterruptedException {
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

        consumer.start();

        HashMap<String, BlockingQueue<ConsumerRecord<?>>> records = consumer.records();

        for (int i = 0; i < topicCount; i++) {
            String topic = "topic" + i;
            BlockingQueue<ConsumerRecord<?>> consumerRecords = records.get(topic);
            consumer.subscribe(topic);

            new Thread(() -> {
                while (true) {
                    try {
                        ConsumerRecord<?> record = consumerRecords.poll(30, TimeUnit.SECONDS);
                        if (record != null) {
                            cb.accept(record);
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        break;
                    }
                }
            }).start();
        }

        return consumer;
    }
}
