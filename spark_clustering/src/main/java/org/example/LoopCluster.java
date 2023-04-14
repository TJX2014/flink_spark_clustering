package org.example;

public class LoopCluster {

    public static void main(String[] args) throws InterruptedException {
        while (true) {
            ClusterJob.main(args);
            Thread.sleep(1000);
        }
    }
}
