import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import edu.ucsd.tritonmq.broker.ConsumerService;
import edu.ucsd.tritonmq.consumer.Consumer;
import edu.ucsd.tritonmq.consumer.ConsumerRecord;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Properties;
import java.util.Queue;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

/**
 * Created by dangyi on 5/29/17.
 */
public class ConsumerTest {
    @Test
    public void ConsumerCanStart() throws InterruptedException {
        for (int i = 5001; i < 5005; i++) {
            Properties configs = new Properties();
            configs.put("host", "localhost");
            configs.put("port", i);
            configs.put("zkAddr", ZkAddr);

            Consumer consumer = new Consumer(configs);

            consumer.subscribe(new String[]{"t1", "t2"});
            Thread.sleep(2000);
            consumer.unSubscribe(new String[]{"t1"});
            consumer.subscribe(new String[]{"t1"});

            consumer.start();
        }
    }

    @Test
    public void ConsumerReceive() throws Exception {
        ConsumerService.AsyncIface cs = Clients.newClient(
                "tbinary+http://localhost:5001/deliver",
                ConsumerService.AsyncIface.class);

        for (int i=0; i < 5; i++) {
            ThriftCompletableFuture<String> future = new ThriftCompletableFuture<String>();
            ConsumerRecord<?> record = new ConsumerRecord<>("t1", "haha");
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            ObjectOutputStream output = new ObjectOutputStream(bao);
            output.writeObject(record);
            byte[] bytes = bao.toByteArray();
            cs.deliver(ByteBuffer.wrap(bytes), future);


            future.thenAccept(response -> {
                System.out.println(response);
            })
            .exceptionally(cause -> {
                // cause.printStackTrace();
                System.out.println("error");
                return null;
            });
        }

        Thread.sleep(2000);
    }
}
