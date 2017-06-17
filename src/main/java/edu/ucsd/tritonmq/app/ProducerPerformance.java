package edu.ucsd.tritonmq.app;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dangyi on 6/16/17.
 */
public class ProducerPerformance {
    public static void main(String[] args) throws InterruptedException {
        final AtomicLong startTime = new AtomicLong(0);
        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicInteger succ = new AtomicInteger(0);
        int recordCount = Integer.parseInt(args[0]);
        int maxInFlight = Integer.parseInt(args[1]);

        ProducerApp.createProducer(new String[]{"--recordCount=" + recordCount, "--maxInFlight=" + maxInFlight, "--interval=0"}, (meta, value) -> {
            if (startTime.get() == 0) {
                startTime.set(System.currentTimeMillis());
            }
            if (meta.succ()) {
                succ.incrementAndGet();
            }
            if (counter.incrementAndGet() == recordCount * 10) {
                System.out.println(succ.get() + "/" + counter.get() + ": " + (System.currentTimeMillis() - startTime.get()));
                System.exit(0);
            }
        });
    }
}
