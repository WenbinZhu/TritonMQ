import edu.ucsd.tritonmq.broker.Broker;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by dangyi on 5/29/17.
 */
public class BrokerTest {
    @Test
    public void brokerCanStartAndStop() throws Exception {
        Broker broker = new Broker(Constant.zooKeeperAddr, 0);
        broker.start();

        CuratorFramework client = CuratorFrameworkFactory.newClient(Constant.zooKeeperAddr,
                new ExponentialBackoffRetry(200, 3));
        client.start();

        String primary = new String(client.getData().forPath("/groups/0/primary"));

        assertEquals(broker.getListenAddr(), primary);
        assertNotNull(client.checkExists().forPath("/groups/0/replica/" + primary));

        broker.stop();
        assertNull(client.checkExists().forPath("/groups/0/replica/" + primary));
        assertNull(client.checkExists().forPath("/groups/0/primary"));
    }
}
