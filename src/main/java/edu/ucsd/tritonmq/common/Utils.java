package edu.ucsd.tritonmq.common;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class Utils {

    public static CuratorFramework initZkClient(int sleepTime, int maxRetry,
                                                 String zkAddr, int sessionTimeout, int connTimeout) {

        RetryPolicy rp = new ExponentialBackoffRetry(sleepTime, maxRetry);
        CuratorFramework zkClient = CuratorFrameworkFactory
                                    .builder()
                                    .connectString(zkAddr)
                                    .sessionTimeoutMs(sessionTimeout)
                                    .connectionTimeoutMs(connTimeout)
                                    .retryPolicy(rp).build();

        return zkClient;
    }
}
