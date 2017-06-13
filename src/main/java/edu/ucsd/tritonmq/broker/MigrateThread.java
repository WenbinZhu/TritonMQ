package edu.ucsd.tritonmq.broker;

public class MigrateThread extends Thread {
    String target;

    public MigrateThread(String target) {
        System.out.println("migration");
        this.target = target;
    }
}
