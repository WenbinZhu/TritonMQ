package edu.ucsd.tritonmq.broker;

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;

import java.io.*;
import java.nio.ByteBuffer;


public class MigrateThread extends Thread {
    Broker primary;
    String backup;

    MigrateThread(Broker primary, String backup) {
        this.primary = primary;
        this.backup = backup;
    }

    @Override
    public void run() {
        System.out.println("Migrate " + this.primary + " to " + this.backup);
        String backupAddr = "tbinary+http://" + backup;
        BrokerService.AsyncIface client = Clients.newClient(backupAddr, BrokerService.AsyncIface.class);

        primary.records.forEach((topic, skipList) -> {
            skipList.forEach((ts, record) -> {
                try {
                    ByteArrayOutputStream bao = new ByteArrayOutputStream();
                    ObjectOutputStream output = new ObjectOutputStream(bao);
                    output.writeObject(record);
                    ByteBuffer bytes = ByteBuffer.wrap(bao.toByteArray());

                    client.replicate(bytes, new ThriftCompletableFuture<>());

                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        });
    }
}
