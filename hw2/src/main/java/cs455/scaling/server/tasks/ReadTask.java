package cs455.scaling.server.tasks;

import cs455.scaling.threadpool.Task;
import cs455.scaling.threadpool.ThreadPoolManager;
import cs455.scaling.server.Server;
import cs455.scaling.server.StatisticsThread;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadTask implements Task {

    private final SocketChannelWrapper wrapper;

    public ReadTask(SocketChannelWrapper wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public void execute() {
        SocketChannel channel = this.wrapper.channel;
        try {

            while(true) {
                byte[] data = new byte[8000];
                ByteBuffer buf = ByteBuffer.wrap(data);
                int bytesRead = channel.read(buf);

                if (bytesRead <= 0) {
                    break;
                }

                HashTask task = new HashTask(wrapper, data);
                ThreadPoolManager.getInstance().addTask(task);
            }

            Server.getTheInstance().registerChannel(channel, SelectionKey.OP_READ);


        } catch (IOException e) {
//            e.printStackTrace();
            StatisticsThread.getInstance().removeChannel(channel);

        }
    }

}
