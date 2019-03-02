package cs455.scaling.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;

public class Server {

    private static Server theInstance;
    private Selector selector;
    private ServerSocketChannel server;
    private ThreadPoolManager manager;
    private StatisticsThread statisticsThread;

    private final Map<SocketChannel, ChannelWrapper> wrappers = new HashMap<>();

    private Server(int port, int threadPoolSize, int batchSize, long batchTime) throws IOException {
        this.selector = Selector.open();

        this.server = ServerSocketChannel.open();
        this.server.bind(new InetSocketAddress(port));
        this.server.configureBlocking(false);
        this.server.register(selector, SelectionKey.OP_ACCEPT);

        this.manager = new ThreadPoolManager(threadPoolSize, batchSize, batchTime);

        this.statisticsThread = new StatisticsThread(this);

        theInstance = this;
    }

    private void run() throws IOException
    {
        Thread thread = new Thread(this.statisticsThread);
        thread.start();

        this.manager.createAndStartWorkers();

        while (true)
        {
            this.selector.select();

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
                    this.register();

                } else if (key.isReadable()) {
                    this.makeTask(key);
                }
            }
        }

    }

    private void register() throws IOException {
        SocketChannel channel = this.server.accept();
        channel.configureBlocking(false);

        System.out.println("Connection received from "+channel.getRemoteAddress());

        channel.register(this.selector, SelectionKey.OP_READ);
        synchronized (this.wrappers) {
            this.wrappers.put(channel, new ChannelWrapper(channel));
        }
    }

    private void makeTask(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        ChannelWrapper wrapper;
        synchronized (this.wrappers) {
            wrapper = this.wrappers.get(channel);
        }
        Task task = new HashTask(wrapper);

        System.out.println("creating task for "+channel.getRemoteAddress());

        manager.addTask(task);
//        key.cancel();
    }

//    public void finishTask(SocketChannel channel) {
//
//        synchronized (this) {
////            try {
//                int throughput = this.clientDataMap.get(channel);
//                this.clientDataMap.put(channel, throughput + 1);
////                channel.register(selector, SelectionKey.OP_READ);
//
////            } catch (ClosedChannelException e) {
////                e.printStackTrace();
////            }
//        }
//
//    }

    public Collection<ChannelWrapper> getWrappers() {
        synchronized (this.wrappers) {
            return this.wrappers.values();
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
        long batchTimeNanos = (long)(batchTime * 1000000000);

        try {
            Server server = new Server(portNum, threadPoolSize, batchSize, batchTimeNanos);
            server.run();

        } catch (IOException e) {
            e.printStackTrace();

        }

    }

}
