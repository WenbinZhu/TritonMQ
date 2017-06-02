package com.internal.examples;

import com.linecorp.armeria.client.ClientBuilder;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.retry.RetryingClient;
import com.linecorp.armeria.client.retry.RetryingRpcClient;
import com.linecorp.armeria.common.Request;
import com.linecorp.armeria.common.Response;
import com.linecorp.armeria.common.thrift.ThriftCompletableFuture;
import org.apache.thrift.TException;

import java.util.concurrent.CountDownLatch;

public class TestArmeriaClient {
    public static void main(String[] args) throws TException {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        HelloService.AsyncIface helloService = Clients.newClient(
                "tbinary+http://127.0.0.1:10008/hello",
                HelloService.AsyncIface.class);

        ThriftCompletableFuture<String> future = new ThriftCompletableFuture<String>();
        helloService.hello("Armerian World", future);

        future.thenAccept(response -> {
            System.out.println(response.equals("Hello, Armerian World!"));
            countDownLatch.countDown();
        })
                .exceptionally(cause -> {
                    cause.printStackTrace();
                    return null;
                });

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // HelloService.Iface helloService = Clients.newClient(
        //         "tbinary+http://127.0.0.1:10008/hello",
        //         HelloService.Iface.class);
        // String greeting = helloService.hello("Armerian World");
        // System.out.println(greeting.equals("Hello, Armerian World!"));
    }
}

