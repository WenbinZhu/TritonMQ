import edu.ucsd.tritonmq.broker.Broker;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;
import static org.junit.Assert.*;

/**
 * Created by dangyi on 5/29/17.
 */
public class BrokerTest {
    @Test
    public void brokerCanStart() throws Exception {
        Broker[] brokers = new Broker[3];

        new Thread(new Runnable() {
            @Override
            public void run() {
                Properties configs = new Properties();
                configs.put("retry", 2);
                configs.put("timeout", 1000);
                configs.put("host", "localhost");
                configs.put("port", 9006);
                configs.put("zkAddr", ZkAddr);

                Broker b = new Broker(2, configs);
                brokers[0] = b;
                b.start();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                Properties configs = new Properties();
                configs.put("retry", 2);
                configs.put("timeout", 1000);
                configs.put("host", "localhost");
                configs.put("port", 9007);
                configs.put("zkAddr", ZkAddr);

                Broker b = new Broker(2, configs);
                brokers[1] = b;
                b.start();
            }
        }).start();

        Thread.sleep(500);

        new Thread(new Runnable() {
            @Override
            public void run() {
                Properties configs = new Properties();
                configs.put("retry", 2);
                configs.put("timeout", 1000);
                configs.put("host", "localhost");
                configs.put("port", 9008);
                configs.put("zkAddr", ZkAddr);

                Broker b = new Broker(2, configs);
                brokers[2] = b;
                b.start();
            }
        }).start();

        CountDownLatch latch = new CountDownLatch(1);
        latch.await();
    }
}
