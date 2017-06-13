package edu.ucsd.tritonmq.broker;

public class MigrateThread extends Thread {
    String target;

    public MigrateThread(String target) {
        this.target = target;
    }
}
