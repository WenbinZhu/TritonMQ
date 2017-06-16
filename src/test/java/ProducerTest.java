import edu.ucsd.tritonmq.consumer.Consumer;
import edu.ucsd.tritonmq.producer.Producer;
import edu.ucsd.tritonmq.producer.ProducerRecord;
import org.junit.Test;

import java.util.Properties;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;


public class ProducerTest {

    @Test
    public void producerCanStart() {
        Properties configs = new Properties();
        configs.put("numRetry", 2);
        configs.put("timeout", 100);
        configs.put("maxInFlight", 4);
        configs.put("zkAddr", ZkAddr);

        Producer<String> producer = new Producer<>(configs);
    }

    @Test
    public void producerCanClose() {
        // Properties configs = new Properties();
        // configs.put("zooKeeperAddr", "localhost:2181");
        // Producer producer = new Producer(configs);
        // producer.close();
    }
}
