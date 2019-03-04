package cs455.scaling.server;

import cs455.scaling.threadpool.ThreadPoolManager;
import cs455.scaling.server.tasks.ReadTask;
import cs455.scaling.server.tasks.RegisterTask;
import cs455.scaling.server.tasks.SocketChannelWrapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;

public class Server {

    private static Server theInstance;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final Map<SocketChannel, SocketChannelWrapper> wrapperMap = new HashMap<>();

    private Server(int port) throws IOException {
        this.selector = Selector.open();

        this.server = ServerSocketChannel.open();
        this.server.bind(new InetSocketAddress(port));
        this.server.configureBlocking(false);
        this.server.register(selector, SelectionKey.OP_ACCEPT);

        theInstance = this;
    }

    public static Server getTheInstance() {
        return theInstance;
    }

    private void run() throws IOException
    {

        while (true)
        {
            this.selector.selectNow();

            Set<SelectionKey> set = this.selector.selectedKeys();
            Iterator<SelectionKey> it = set.iterator();

            while(it.hasNext())
            {
                SelectionKey key = it.next();
                it.remove();

                if (!key.isValid()) {
                    continue;
                }

                if (key.isAcceptable()) {
                    key.cancel();
                    RegisterTask task = new RegisterTask(this.server);
                    ThreadPoolManager.getInstance().addTask(task);


                } else if (key.isReadable()) {
                    key.cancel();
                    SocketChannel channel = (SocketChannel) key.channel();

                    SocketChannelWrapper wrapper;
                    if(wrapperMap.containsKey(channel)) {
                        wrapper = wrapperMap.get(channel);
                    } else {
                        wrapper = new SocketChannelWrapper(channel);
                    }

                    ReadTask task = new ReadTask(wrapper);
                    ThreadPoolManager.getInstance().addTask(task);

                }
            }

        }

    }

    public void registerChannel(SelectableChannel channel, int keyType) {
        try {
            channel.register(selector, keyType);

        } catch (CancelledKeyException e) {
//            e.printStackTrace();

            if(SocketChannel.class == channel.getClass())
                StatisticsThread.getInstance().removeChannel((SocketChannel) channel);

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    public static void main(String[] args) {
        if(args.length < 4) {
            System.out.println("Usage: portnum thread-pool-size batch-size batch-time");
            System.exit(1);
        }

        int portNum = Integer.parseInt(args[0]);
        int threadPoolSize = Integer.parseInt(args[1]);
        int batchSize = Integer.parseInt(args[2]);

        double batchTime = Double.parseDouble(args[3]);

        try {
            Server server = new Server(portNum);

            StatisticsThread statistics = StatisticsThread.getInstance();
            Thread thread = new Thread(statistics);
            thread.start();

            ThreadPoolManager manager = new ThreadPoolManager(threadPoolSize, batchSize, batchTime);
            manager.createAndStartWorkers();

            server.run();

        } catch (IOException e) {
            e.printStackTrace();

        }

    }

}
