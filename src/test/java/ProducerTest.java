import edu.ucsd.tritonmq.producer.Producer;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by dangyi on 5/29/17.
 */
public class ProducerTest {
    @Test
    public void producerCanClose() {
        Properties configs = new Properties();
        configs.put("zooKeeperAddr", "localhost:2181");
        Producer producer = new Producer(configs);
        producer.close();
    }
}
