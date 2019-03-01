package cs455.scaling.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Server {

    private static Server theInstance;
    private Selector selector;
    private ServerSocketChannel server;
    private ThreadPoolManager manager;

    private Map<SocketChannel, Integer> clientDataMap = new HashMap<>();


    private Server(int port, int threadPoolSize, int batchSize, long batchTime) throws IOException {
        this.selector = Selector.open();

        this.server = ServerSocketChannel.open();
        this.server.bind(new InetSocketAddress(port));
        this.server.configureBlocking(false);
        this.server.register(selector, SelectionKey.OP_ACCEPT);

        this.manager = new ThreadPoolManager(threadPoolSize, batchSize, batchTime);

        theInstance = this;
    }

    private void run() throws IOException
    {
        this.manager.createAndStartWorkers();

        while (true)
        {
            this.selector.select();

            for(SelectionKey key : selector.selectedKeys())
            {
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

        synchronized (this) {
            channel.register(this.selector, SelectionKey.OP_READ);
            this.clientDataMap.put(channel, 0);
        }
    }

    private void makeTask(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        Task task = new HashTask(channel);
        manager.addTask(task);
        key.cancel();
    }

    public void finishTask(SocketChannel channel) {

        synchronized (this) {
            try {
                int throughput = this.clientDataMap.get(channel);
                this.clientDataMap.put(channel, throughput + 1);
                channel.register(selector, SelectionKey.OP_READ);

            } catch (ClosedChannelException e) {
                e.printStackTrace();
            }
        }

    }

    public static Server getTheInstance() {
        return Server.theInstance;
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
