package cs455.scaling.server;

import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class StatisticsThread implements Runnable {

    private final double UPDATE_INTERVAL = 20d;

    @Override
    public void run() {

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        long lastLoopTime = System.currentTimeMillis();
        final long millisBetweenStarts = (long)(UPDATE_INTERVAL * 1000);

        while(true) {
            try {
                long nextLoopStartTime = lastLoopTime + millisBetweenStarts;
                long sleepTime = nextLoopStartTime - System.currentTimeMillis();
                if(sleepTime > 0)
                    Thread.sleep(sleepTime);
                lastLoopTime = nextLoopStartTime;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Collection<AtomicInteger> throughputs = Server.getTheInstance().getThroughputs();

            int sum = 0;
            int[] values = new int[throughputs.size()];
            int i = 0;

            for(AtomicInteger atomic : throughputs) {
                int value = atomic.getAndSet(0);
                values[i++] = value;
                sum += value;
            }
            final double msgsPerSec = (double) sum / UPDATE_INTERVAL;

            final double mean;
            if(throughputs.size() > 0)
                mean = (double) sum / throughputs.size() / UPDATE_INTERVAL;
            else
                mean = 0;

            double sumDiffSq = 0;
            for(int value : values) {
                double perSec = value / UPDATE_INTERVAL;
                double diff = perSec-mean;
                sumDiffSq += diff*diff;
            }

            double stdDev;
            if(throughputs.size() > 1)
                stdDev = Math.sqrt(sumDiffSq / (throughputs.size()-1) );
            else
                stdDev = 0;

            LocalDateTime now = LocalDateTime.now();

            System.out.printf("[%s] Server throughput: %.2f messages/s, Active Client Connections: %d, Mean Per-client Throughput: %.2f messages/s, Std. Dev. Of Per-client Throughput: %.3f messages/s\n",
                    dtf.format(now), msgsPerSec, throughputs.size(), mean, stdDev);
        }
    }

}