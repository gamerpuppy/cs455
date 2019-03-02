package cs455.scaling.server;

import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ChannelWrapper {

    public SocketChannel channel;
    public Lock readLock;
    public Lock writeLock;
    AtomicInteger throughput;

    public ChannelWrapper(SocketChannel channel) {
        this.channel = channel;
        this.readLock = new ReentrantLock();
        this.writeLock = new ReentrantLock();
        this.throughput = new AtomicInteger(0);
    }

}
