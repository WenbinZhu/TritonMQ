package edu.ucsd.tritonmq.app;

import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dangyi on 6/16/17.
 */
public class ConsumerProducerApp {

    public static void main(String[] argv) throws InterruptedException {
        ConcurrentHashMap<String, ConcurrentHashSet<String>> dict = new ConcurrentHashMap<>();
        AtomicInteger counter = new AtomicInteger(200 * 10);

        ConsumerApp.createConsumer(new String[]{}, record -> {
            String value = (String) record.value();
            value = value.split(" ")[0];
            System.out.println("Received " + record.topic() + ", " + value);

            assert dict.get(record.topic()).remove(value) : record.topic() + "  " + value;
            if (counter.decrementAndGet() == 0) {
                System.exit(0);
            }
        });

        for (int j = 0; j < 10; j++) {
            String topic = "topic" + j;
            ConcurrentHashSet<String> set = new ConcurrentHashSet<>();

            for (int i = 0; i < 200; i++) {
                set.add("value" + i);
            }
            dict.put(topic, set);
        }

        ProducerApp.createProducer(new String[]{"--recordCount=200", "--interval=1000"}, (meta, value) -> {
            String topic = meta.topic();
            if (!meta.succ()) {
                dict.get(topic).remove(value);
                System.out.println("Fail sending " + topic + " " + value);
                counter.decrementAndGet();
            }
        });
    }
}
