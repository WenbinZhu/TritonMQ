package edu.ucsd.tritonmq.app;

import edu.ucsd.tritonmq.producer.Producer;
import edu.ucsd.tritonmq.producer.ProducerRecord;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * Created by dangyi on 6/16/17.
 */
public class ConsumerProducerLatency {

    public static void main(String[] argv) throws InterruptedException {
        ConcurrentHashMap<String, ConcurrentHashMap<String, Long>> dict = new ConcurrentHashMap<>();
        int recordCount = Integer.parseInt(argv[1]);
        int topicCount = Integer.parseInt(argv[0]);
        AtomicInteger counter = new AtomicInteger(recordCount * topicCount);
        AtomicInteger succ = new AtomicInteger(0), fail = new AtomicInteger(0);

        for (int j = 0; j < 10; j++) {
            String topic = "topic" + j;
            ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();
            dict.put(topic, map);
        }

        LongAdder totalTime = new LongAdder();


        ConsumerApp.createConsumer(new String[]{}, record -> {
            String value = (String) record.value();
            value = value.split(" ")[0];
//            System.out.println("Received " + record.topic() + ", " + value);

            Long startTime = dict.get(record.topic()).remove(value);

            if (startTime == null) {
                System.err.println("Duplicate! " + record.topic() + "  " + value);
                return;
            }
            totalTime.add(System.currentTimeMillis() - startTime);
            succ.incrementAndGet();

            if (counter.decrementAndGet() == 0) {
                System.out.println(succ.get() + "/" + (recordCount * topicCount) + " " + totalTime.longValue());
                System.exit(0);
            }
        });

        Producer<String> producer = ProducerApp.createProducer(new String[]{"--recordCount=0", "--interval=1000"}, null);

        for (int i = 0; i < recordCount; i++) {
            for (int j = 0; j < topicCount; j++) {
                String value = "value" + i;
                String topic = "topic" + j;
                dict.get(topic).put(value, System.currentTimeMillis());
                producer.publish(new ProducerRecord<String>(
                        topic,
                        value + " " + String.join("", Collections.nCopies(90, "-"))
                )).thenAccept(meta -> {
                    if (!meta.succ()) {
                        dict.get(topic).remove(value);
                        System.out.println("Fail sending " + topic + " " + value);
                        counter.decrementAndGet();
                        fail.incrementAndGet();
                    }
                });
            }
            Thread.sleep(300);
        }
    }
}
