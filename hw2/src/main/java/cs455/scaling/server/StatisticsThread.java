package cs455.scaling.server;

import java.nio.channels.SocketChannel;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StatisticsThread implements Runnable {

    private static StatisticsThread theInstance = null;

    private final double UPDATE_INTERVAL = 5d;
    private final ConcurrentHashMap<SocketChannel, AtomicInteger> countMap = new ConcurrentHashMap<>();

    private StatisticsThread() { }

    public static StatisticsThread getInstance()
    {
        if (theInstance == null)
            theInstance = new StatisticsThread();

        return theInstance;
    }

    public void addChannel(SocketChannel channel) {
        this.countMap.put(channel, new AtomicInteger(0));
    }

    public void removeChannel(SocketChannel channel) {
        this.countMap.remove(channel);
    }

    public void addToCount(SocketChannel channel) {
        AtomicInteger atomic = this.countMap.get(channel);
        if(atomic != null) {
            atomic.getAndIncrement();
        }
    }

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

            Collection<AtomicInteger> counts = this.countMap.values();

            int sum = 0;
            int[] values = new int[counts.size()];
            int i = 0;

            for(AtomicInteger atomic : counts) {
                int value = atomic.getAndSet(0);
                values[i++] = value;
                sum += value;
            }
            final double msgsPerSec = (double) sum / UPDATE_INTERVAL;

            final double mean;
            if(counts.size() > 0)
                mean = (double) sum / counts.size() / UPDATE_INTERVAL;
            else
                mean = 0;

            double sumDiffSq = 0;
            for(int value : values) {
                double perSec = value / UPDATE_INTERVAL;
                double diff = perSec-mean;
                sumDiffSq += diff*diff;
            }

            double stdDev;
            if(counts.size() > 1)
                stdDev = Math.sqrt(sumDiffSq / (counts.size()-1) );
            else
                stdDev = 0;

            LocalDateTime now = LocalDateTime.now();

            System.out.printf("[%s] Server throughput: %.2f messages/s, Active Client Connections: %d, Mean Per-client Throughput: %.2f messages/s, Std. Dev. Of Per-client Throughput: %.3f messages/s\n",
                    dtf.format(now), msgsPerSec, counts.size(), mean, stdDev);
        }
    }

}