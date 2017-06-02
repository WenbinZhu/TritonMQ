package com.internal.examples;

import com.linecorp.armeria.common.SerializationFormat;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.thrift.THttpService;

public class TestArmeriaServer {
    public static void main(String[] args) {
        ServerBuilder sb = new ServerBuilder();
        sb.port(10008, SessionProtocol.HTTP).serviceAt("/hello", THttpService.of(new MyHelloService(), SerializationFormat.THRIFT_BINARY));
        sb.build().start();
    }
}