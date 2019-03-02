package cs455.scaling.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Server {

    private static Server theInstance;
    private final Selector selector;
    private final ServerSocketChannel server;
    private final ThreadPoolManager manager;
    private final StatisticsThread statisticsThread;

    private final ConcurrentHashMap<SocketChannel, AtomicInteger> throughputMap = new ConcurrentHashMap<>();

    private Server(int port, int threadPoolSize, int batchSize, long batchTime) throws IOException {
        this.selector = Selector.open();

        this.server = ServerSocketChannel.open();
        this.server.bind(new InetSocketAddress(port));
        this.server.configureBlocking(false);
        this.server.register(selector, SelectionKey.OP_ACCEPT);

        this.manager = new ThreadPoolManager(threadPoolSize, batchSize, batchTime);

        this.statisticsThread = new StatisticsThread();

        theInstance = this;
    }

    private void run() throws IOException
    {
        Thread thread = new Thread(this.statisticsThread);
        thread.start();

        this.manager.createAndStartWorkers();

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
                    this.register();

                } else if (key.isReadable()) {
                    this.makeTask(key);

                }
            }

        }

    }

    private void register() throws IOException {
        SocketChannel channel = this.server.accept();
        channel.socket().setReceiveBufferSize(4096*4);
        channel.configureBlocking(false);

//        System.out.println("connection received from "+channel.getRemoteAddress());

        channel.register(this.selector, SelectionKey.OP_READ);
        this.throughputMap.put(channel, new AtomicInteger(0));
    }

    private void makeTask(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        Task task = new HashTask(channel);

//        System.out.println("creating task for "+channel.getRemoteAddress());

        this.manager.addTask(task);
        key.cancel();
    }

    public void removeChannel(SocketChannel channel) {
        this.throughputMap.remove(channel);
    }

    public void finishTask(SocketChannel channel) {
        try {
            channel.register(selector, SelectionKey.OP_READ);

            AtomicInteger atomic = this.throughputMap.get(channel);
            atomic.getAndIncrement();

        } catch (IOException e) {
            this.throughputMap.remove(channel);

        }
    }

    public static Server getTheInstance() {
        return theInstance;
    }

    public Collection<AtomicInteger> getThroughputs() {
        return this.throughputMap.values();
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
