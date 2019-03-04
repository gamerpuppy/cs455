package cs455.scaling.server.tasks;

import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.ReentrantLock;

public class SocketChannelWrapper {

    public final SocketChannel channel;
    public ReentrantLock writeLock = new ReentrantLock();
    // read lock not necessary because only one read task can be active at once

    public SocketChannelWrapper(SocketChannel channel) {
        this.channel = channel;
    }

}
