package edu.ucsd.tritonmq.broker;

import edu.ucsd.tritonmq.producer.ProducerRecord;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;

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
        ProducerRecord<?> prd = null;

        // Check if primary gets the request
        if (!broker.isPrimary) {
            resultHandler.onError(new Exception("Not primary, primary may failed"));
            return;
        }

        // Parse request
        try {
            bai = new ByteArrayInputStream(record.array());
            input = new ObjectInputStream(bai);
            prd = (ProducerRecord<?>) input.readObject();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return;
        } finally {
            close(bai, input);
        }

        // Handle duplicate client requests
        if (Succ.equals(broker.requests.get(prd.uuid()))) {
            resultHandler.onComplete(Fail);
            return;
        }

        // TODO: normal handling case, assign timestamp
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
