package edu.ucsd.tritonmq.app;

import edu.ucsd.tritonmq.broker.Broker;

import java.util.Properties;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;


public class BrokerApp {
    public static void main(String[] args) throws InterruptedException {
        Properties configs = new Properties();

        configs.put("retry", 1);
        configs.put("timeout", 1000);
        configs.put("host", "100.81.36.167");
        configs.put("port", 8000);
        configs.put("zkAddr", ZkAddr);

        configs.put("groupId", 0);

        for (String arg : args) {
            if (arg.substring(0, 2).equals("--")) {
                String[] splits = arg.substring(2).split("=");
                try {
                    configs.put(splits[0], Integer.parseInt(splits[1]));
                } catch (NumberFormatException e) {
                    configs.put(splits[0], splits[1]);
                }
            }
        }

        System.out.println("configs: " + configs);

        Broker broker = new Broker((Integer)configs.get("groupId"), configs);

        broker.start();
    }
}
