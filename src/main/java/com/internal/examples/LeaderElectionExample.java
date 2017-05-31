package com.internal.examples;

import java.text.MessageFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 *
 * Zookeeper - Curator - Leader Latch
 *
 */
public class LeaderElectionExample {

    private static final int SECOND = 1000;

    public static void main(String[] args) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; i++) {
            final int index = i;
            service.submit(new Runnable() {
                public void run() {
                    try {
                        new LeaderElectionExample().schedule(index);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        Thread.sleep(10 * SECOND);
        service.shutdownNow();
    }

    private void schedule(final int thread) throws Exception {
        CuratorFramework client = this.getStartedClient(thread);
        String path = "/leader_latch";
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path);
        }

        LeaderLatch latch = new LeaderLatch(client, path);
        latch.addListener(new LeaderLatchListener() {

            public void notLeader() {
                System.out.println(MessageFormat.format("Thread [" + thread
                        + "] I am not the leader... timestamp [{0}]", System.currentTimeMillis()));
            }

            public void isLeader() {
                System.out.println(MessageFormat.format("Thread [" + thread + "] I am the leader... timestamp [{0}]",
                        System.currentTimeMillis()));
            }
        });

        latch.start();

        Thread.sleep(3 * (thread + 1) * SECOND);
        if (latch != null) {
            latch.close();
        }
        if (client != null) {
            client.close();
        }
        System.out.println("Thread [" + thread + "] Server closed...");
    }

    private CuratorFramework getStartedClient(final int thread) {
        RetryPolicy rp = new ExponentialBackoffRetry(1 * SECOND, 3);
        // Fluent style client creation
        CuratorFramework cfFluent = CuratorFrameworkFactory.builder().connectString("localhost:2181")
                .sessionTimeoutMs(5 * SECOND).connectionTimeoutMs(3 * SECOND).retryPolicy(rp).build();
        cfFluent.start();
        System.out.println("Thread [" + thread + "] Server connected...");
        return cfFluent;
    }
}
