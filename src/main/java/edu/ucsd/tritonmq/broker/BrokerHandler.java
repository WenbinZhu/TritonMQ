package edu.ucsd.tritonmq.broker;

import edu.ucsd.tritonmq.producer.ProducerRecord;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

import static edu.ucsd.tritonmq.common.GlobalConfig.*;

public class BrokerHandler implements BrokerService.AsyncIface {
    private  Broker broker;

    public BrokerHandler(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void send(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {
        ByteArrayInputStream bai = null;
        ObjectInputStream input = null;
        ProducerRecord<?> prod = null;

        // Check if primary gets the request
        if (!broker.isPrimary) {
            resultHandler.onError(new Exception("Not primary, primary may failed"));
            return;
        }

        // Parse request
        try {
            bai = new ByteArrayInputStream(record.array());
            input = new ObjectInputStream(bai);
            prod = (ProducerRecord<?>) input.readObject();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return;
        } finally {
            close(bai, input);
        }

        try {
            // Handle duplicate client requests
            synchronized (broker.requests) {
                String status = broker.requests.get(prod.uuid());

                if (status == null) {
                    broker.requests.put(prod.uuid(), Pend);
                } else {
                    resultHandler.onComplete(status);
                    return;
                }
            }

            // Push record into queue
            String topic = prod.topic();
            BrokerRecord<?> brod = new BrokerRecord<>(topic, prod.value(), broker.incrementTs());

            synchronized (broker.records) {
                if (!broker.records.containsKey(topic)) {
                    broker.records.put(topic, new ConcurrentLinkedDeque<>());
                }
            }

            broker.records.get(topic).offer(brod);
            broker.requests.put(prod.uuid(), Succ);
            resultHandler.onComplete(Succ);

        } catch (Exception e) {
            resultHandler.onComplete(Fail);
        }
    }

    @Override
    public void migrate(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {

    }

    @Override
    public void replicate(ByteBuffer record, AsyncMethodCallback<String> resultHandler) throws TException {

    }

    private void close(Closeable... resource) {
        for (Closeable res : resource) {
            try {
                if (res != null)
                    res.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
