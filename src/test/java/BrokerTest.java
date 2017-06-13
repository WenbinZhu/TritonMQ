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
                configs.put("timeout", 200);
                configs.put("host", "localhost");
                configs.put("port", 9001);
                configs.put("zkAddr", ZkAddr);

                Broker b = new Broker(0, configs);
                b.start();
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                Properties configs = new Properties();
                configs.put("retry", 2);
                configs.put("timeout", 200);
                configs.put("host", "localhost");
                configs.put("port", 9002);
                configs.put("zkAddr", ZkAddr);

                Broker b = new Broker(0, configs);
                b.start();
            }
        }).start();

        Thread.sleep(3000);

        new Thread(new Runnable() {
            @Override
            public void run() {
                Properties configs = new Properties();
                configs.put("retry", 2);
                configs.put("timeout", 200);
                configs.put("host", "localhost");
                configs.put("port", 9003);
                configs.put("zkAddr", ZkAddr);

                Broker b = new Broker(0, configs);
                b.start();
            }
        }).start();

        Thread.sleep(5000);
    }
}
