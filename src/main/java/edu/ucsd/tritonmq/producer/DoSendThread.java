package edu.ucsd.tritonmq.producer;

import java.util.concurrent.ConcurrentLinkedQueue;
import static edu.ucsd.tritonmq.common.GlobalConfig.*;

public class DoSendThread<T> implements Runnable {
    private ConcurrentLinkedQueue<ProducerRecord<T>> bufferQueue;
    private String[] groupLeaders;

    DoSendThread(ConcurrentLinkedQueue<ProducerRecord<T>> bufferQueue) {
        this.bufferQueue = bufferQueue;
        this.groupLeaders = new String[NumBrokerGroups];

        // Get group leaders

    }

    private void updateGroupLeader(int groupId) {

    }

    @Override
    public void run() {
        // Get group leader

        // Async send

    }
}
