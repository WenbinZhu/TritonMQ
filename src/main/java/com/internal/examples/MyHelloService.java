package com.internal.examples;

import org.apache.thrift.async.AsyncMethodCallback;

public class MyHelloService implements HelloService.AsyncIface {
    @Override
    public void hello(String name, AsyncMethodCallback<String> resultHandler) {
        resultHandler.onComplete("Hello, " + name + '!');
    }
}

// public class MyHelloService implements HelloService.Iface {
//     @Override
//     public String hello(String name) {
//         return "Hello, " + name + '!';
//     }
// }