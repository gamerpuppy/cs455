package cs455.scaling.server.tasks;

import cs455.scaling.threadpool.Task;
import cs455.scaling.server.Server;
import cs455.scaling.server.StatisticsThread;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class RegisterTask implements Task {

    private final ServerSocketChannel serverSocketChannel;

    public RegisterTask(ServerSocketChannel serverSocketChannel) {
        this.serverSocketChannel = serverSocketChannel;
    }

    private SocketChannel acceptChannel() {
        try {
            return this.serverSocketChannel.accept();

        } catch (IOException e) {
            e.printStackTrace();
            return null;

        }
    }

    @Override
    public void execute() {
        Server server = Server.getTheInstance();

        SocketChannel channel;
        while((channel = this.acceptChannel()) != null) {
            try {
                channel.configureBlocking(false);
                server.registerChannel(channel, SelectionKey.OP_READ);
                StatisticsThread.getInstance().addChannel(channel);

            } catch (IOException e){}
        }

        server.registerChannel(serverSocketChannel, SelectionKey.OP_ACCEPT);
    }

}
