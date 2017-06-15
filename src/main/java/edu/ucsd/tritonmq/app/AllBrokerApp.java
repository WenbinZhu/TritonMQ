package edu.ucsd.tritonmq.app;

/**
 * Created by dangyi on 6/14/17.
 */
public class AllBrokerApp {
    public static void main(String[] args) throws InterruptedException {
        BrokerApp.main(new String[]{"--port=9000", "--groupId=0"});
        BrokerApp.main(new String[]{"--port=9001", "--groupId=1"});
        BrokerApp.main(new String[]{"--port=9002", "--groupId=2"});
        BrokerApp.main(new String[]{"--port=9003", "--groupId=0"});
        BrokerApp.main(new String[]{"--port=9004", "--groupId=1"});
        BrokerApp.main(new String[]{"--port=9005", "--groupId=2"});
        BrokerApp.main(new String[]{"--port=9006", "--groupId=0"});
        BrokerApp.main(new String[]{"--port=9007", "--groupId=1"});
        BrokerApp.main(new String[]{"--port=9008", "--groupId=2"});
    }
}
