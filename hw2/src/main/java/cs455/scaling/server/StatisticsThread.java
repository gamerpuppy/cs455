package cs455.scaling.server;

import java.util.Collection;
import java.time.format.DateTimeFormatter;

public class StatisticsThread implements Runnable {

    final Server server;

    public StatisticsThread(Server server) {
        this.server = server;
    }

    @Override
    public void run() {

        while(true) {
            Collection<ChannelWrapper> wrappers = this.server.getWrappers();

            int sum = 0;

            int[] values = new int[wrappers.size()];
            int i = 0;

            for(ChannelWrapper wrapper : wrappers) {
                int value = wrapper.throughput.getAndSet(0);
                values[i] = value;
                sum += value;
            }

            final double mean = (double) sum / wrappers.size();

            double sumDiffSq = 0;
            for(int value : values) {
                double diff = mean-value;
                sumDiffSq += diff*diff;
            }

            double stdDev;
            if(wrappers.size() > 1)
                stdDev = sumDiffSq / (wrappers.size()-1);
            else
                stdDev = 0;


            double msgsPerSec = (double) sum / 20;

            System.out.println(DateTimeFormatter.ISO_LOCAL_TIME + "Server throughput: "+msgsPerSec + " messages/sec,");

            try {
                Thread.sleep(20*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
