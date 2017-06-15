package edu.ucsd.tritonmq.app;

import edu.ucsd.tritonmq.broker.Broker;

import java.util.Properties;

import static edu.ucsd.tritonmq.common.GlobalConfig.ZkAddr;

/**
 * Created by dangyi on 6/14/17.
 */
public class BrokerApp {
    public static void main(String[] args) throws InterruptedException {
        Properties configs = new Properties();

        configs.put("retry", 2);
        configs.put("timeout", 200);
        configs.put("host", "localhost");
        configs.put("port", 9006);
        configs.put("zkAddr", ZkAddr);

        configs.put("groupId", 2);

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
        broker.wait();
    }
}
