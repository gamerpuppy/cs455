package cs455.scaling.server.tasks;

import cs455.scaling.threadpool.Task;
import cs455.scaling.server.StatisticsThread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class WriteTask implements Task {

    private final SocketChannelWrapper wrapper;
    private final byte[] bytes;

    public WriteTask(SocketChannelWrapper wrapper, byte[] bytes) {
        this.wrapper = wrapper;
        this.bytes = bytes;

    }

    @Override
    public void execute() {
        SocketChannel channel = this.wrapper.channel;
        try {
            ByteBuffer buf = ByteBuffer.wrap(this.bytes);

            this.wrapper.writeLock.lock();
            try {
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }

            } finally {
                this.wrapper.writeLock.unlock();
            }
            StatisticsThread.getInstance().addToCount(channel);

        } catch (IOException e) {
//            e.printStackTrace();
            StatisticsThread.getInstance().removeChannel(channel);

        }
    }

}
