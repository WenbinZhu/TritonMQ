package edu.ucsd.tritonmq.app;

import edu.ucsd.tritonmq.broker.Broker;
import edu.ucsd.tritonmq.producer.Producer;
import edu.ucsd.tritonmq.producer.ProducerRecord;

import java.util.Collections;
import java.util.Properties;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;

/**
 * Created by dangyi on 6/14/17.
 */
public class ProducerApp {
    public static void main(String[] args) throws InterruptedException {
        Properties configs = new Properties();

        configs.put("retry", 2);
        configs.put("timeout", 500);
        configs.put("maxInFlight", 4);
        configs.put("zkAddr", ZkAddr);

        configs.put("recordCount", 100);
        configs.put("topicCount", 10);
        configs.put("recordSize", 100);

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

        Producer producer = new Producer(configs);
        int recordCount = (Integer) configs.get("recordCount");
        int topicCount = (Integer) configs.get("topicCount");
        int recordSize = (Integer) configs.get("recordSize");

        for (int i = 0; i < recordCount; i++) {
            for (int j = 0; j < topicCount; j++) {
                producer.publish(new ProducerRecord<>(
                        "topic" + j,
                        "value " + i + " " +
                                String.join("", Collections.nCopies(recordSize-10, "-"))
                ));
            }
        }
    }
}
