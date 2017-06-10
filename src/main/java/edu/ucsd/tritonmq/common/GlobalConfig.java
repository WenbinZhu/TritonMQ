package edu.ucsd.tritonmq.common;


/**
 * Created by Wenbin on 5/31/17.
 */
public class GlobalConfig {
    public final static int Second = 1000;

    public final static int NumBrokerGroups = 3;

    public final static int BrokerRetry = 2;

    public final static int BrokerTimeout = 200;

    public final static String Succ = "Succ";

    public final static String Fail = "Fail";

    public final static String ZkAddr = "localhost:2181";

    public final static String SubscribePath = "/subscribe";

    public final static String ReplicaPath = "/replica";

    public final static String PrimaryPath = "/primary";

}
