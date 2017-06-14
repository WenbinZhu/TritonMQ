package edu.ucsd.tritonmq.consumer;

import edu.ucsd.tritonmq.broker.ConsumerService;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;
import static edu.ucsd.tritonmq.common.Utils.*;

/**
 * Created by Wenbin on 6/5/17.
 */
public class RecvThread implements ConsumerService.AsyncIface {
    private HashMap<String, BlockingQueue<ConsumerRecord<?>>> queue;

    RecvThread(HashMap<String, BlockingQueue<ConsumerRecord<?>>> queue) {
        this.queue = queue;
    }

    @Override
    public void deliver(ByteBuffer byteBuffer, AsyncMethodCallback<String> resultHandler) throws TException {
        ByteArrayInputStream bis = null;
        ObjectInputStream input = null;

        try {
            bis = new ByteArrayInputStream(byteBuffer.array());
            input = new ObjectInputStream(bis);
            ConsumerRecord record = (ConsumerRecord) input.readObject();
            String topic = record.topic();

            if (queue.containsKey(topic)) {
                queue.get(topic).offer(record);
                resultHandler.onComplete(Succ);
            } else {
                resultHandler.onComplete(Fail);
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            resultHandler.onComplete(Fail);
        } finally {
            close(bis, input);
        }
    }
}
