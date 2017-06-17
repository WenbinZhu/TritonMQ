package edu.ucsd.tritonmq.common;


public class GlobalConfig {
    public final static int Second = 1000;

    public final static int NumBrokerGroups = 3;

    public final static int BrokerRetry = 2;

    public final static int BrokerTimeout = 200;

    public final static String Succ = "Succ";

    public final static String Fail = "Fail";

    public final static String Pend = "Pend";

    public final static String ZkAddr = "100.81.36.167:2181";

    public final static String SubscribePath = "/subscribe";

    public final static String ReplicaPath = "/replica";

    public final static String PrimaryPath = "/primary";

}
