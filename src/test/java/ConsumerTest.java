import edu.ucsd.tritonmq.consumer.Consumer;
import org.junit.Test;

import java.util.Properties;
import static edu.ucsd.tritonmq.common.GlobalConfig.*;

/**
 * Created by dangyi on 5/29/17.
 */
public class ConsumerTest {
    @Test
    public void ConsumerCanStart() {
        for (int i = 5001; i < 5005; i++) {
            Properties configs = new Properties();
            configs.put("host", "localhost");
            configs.put("port", i);
            configs.put("zkAddr", ZkAddr);

            Consumer consumer = new Consumer(configs);
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
