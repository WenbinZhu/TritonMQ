import edu.ucsd.tritonmq.producer.Producer;
import org.junit.Test;

/**
 * Created by dangyi on 5/29/17.
 */
public class ProducerTest {
    @Test
    public void producerCanClose() {
        Producer producer = new Producer(Constant.zooKeeperAddr);
        producer.close();
    }
}
