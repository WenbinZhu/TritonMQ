package edu.ucsd.tritonmq.app;

import edu.ucsd.tritonmq.broker.Broker;
import edu.ucsd.tritonmq.producer.Producer;
import edu.ucsd.tritonmq.producer.ProducerMetaRecord;
import edu.ucsd.tritonmq.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;


public class ProducerApp {
    public static void main(String[] args) throws InterruptedException {
        createProducer(args, (meta, value) -> {
            System.out.println(meta.topic() + " " + value + ": " + meta.succ());
        });
    }

    public static void createProducer(String[] args, BiConsumer<ProducerMetaRecord, String> cb) throws InterruptedException {
        Properties configs = new Properties();

        configs.put("retry", 2);
        configs.put("timeout", 500);
        configs.put("maxInFlight", 2);
        configs.put("zkAddr", ZkAddr);

        configs.put("recordCount", 50);
        configs.put("topicCount", 10);
        configs.put("recordSize", 100);
        configs.put("interval", 200);

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

        Producer<String> producer = new Producer<>(configs);
        int recordCount = (Integer) configs.get("recordCount");
        int topicCount = (Integer) configs.get("topicCount");
        int recordSize = (Integer) configs.get("recordSize");
        int interval = (Integer) configs.get("interval");

        for (int i = 0; i < recordCount; i++) {
            for (int j = 0; j < topicCount; j++) {
                String value = "value" + i;
                String topic = "topic" + j;
                producer.publish(new ProducerRecord<String>(
                        topic,
                        value + " " + String.join("", Collections.nCopies(recordSize - 10, "-"))
                )).thenAccept(meta -> cb.accept(meta, value));
            }
            if (interval > 0) {
                Thread.sleep(interval);
            }
        }
    }
}
