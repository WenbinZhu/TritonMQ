package edu.ucsd.tritonmq.consumer;

import edu.ucsd.tritonmq.broker.ConsumerService;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.nio.ByteBuffer;


/**
 * Created by Wenbin on 6/5/17.
 */
public class RecvThread implements Runnable, ConsumerService.AsyncIface {
    private Consumer consumer;

    public RecvThread(Consumer cosumer) {
        this.consumer = consumer;
    }

    @Override
    public void deliver(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {

    }

    @Override
    public void run() {

    }
}
