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
        int recordCount = 200;
        AtomicInteger counter = new AtomicInteger(recordCount * 10);
        AtomicInteger succ = new AtomicInteger(0), fail = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int j = 0; j < 10; j++) {
            String topic = "topic" + j;
            ConcurrentHashSet<String> set = new ConcurrentHashSet<>();

            for (int i = 0; i < recordCount; i++) {
                set.add("value" + i);
            }
            dict.put(topic, set);
        }

        ConsumerApp.createConsumer(new String[]{}, record -> {
            String value = (String) record.value();
            value = value.split(" ")[0];
//            System.out.println("Received " + record.topic() + ", " + value);

            if (dict.get(record.topic()).remove(value) == false) {
                System.err.println("Duplicate! " + record.topic() + "  " + value);
            }
            succ.incrementAndGet();
            if (counter.decrementAndGet() == 0) {
                System.out.println("succ: " + succ.get() + "\t fail: " + fail.get());
                System.out.println(System.currentTimeMillis() - startTime);
                System.exit(0);
            }
        });

        ProducerApp.createProducer(new String[]{"--recordCount=" + recordCount, "--interval=0"}, (meta, value) -> {
            String topic = meta.topic();
            if (!meta.succ()) {
                dict.get(topic).remove(value);
                System.out.println("Fail sending " + topic + " " + value);
                counter.decrementAndGet();
                fail.incrementAndGet();
            }
        });
    }
}
