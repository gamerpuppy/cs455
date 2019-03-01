package cs455.scaling.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

public class Server {

    private static Server theInstance;
    private Selector selector;
    private ServerSocketChannel server;
    private ThreadPoolManager manager;

    private int activeChannels = 0;

    private Server(int port, int threadPoolSize, int batchSize, long batchTime) throws IOException {
        this.server = ServerSocketChannel.open();
        this.server.bind(new InetSocketAddress(port));

        this.selector = Selector.open();
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
                    this.register(key);
                }

                else if (key.isReadable()) {
                    this.makeTask(key);
                }

            }


        }

    }

    public void register(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        channel.configureBlocking(false);
        channel.register(this.selector, SelectionKey.OP_READ);

        this.activeChannels += 1;
    }

    public void makeTask(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        Task task = new HashTask(channel);
        manager.addTask(task);
        key.cancel();
    }

    public static Server getTheInstance() {
        return Server.theInstance;
    }

    public Selector getSelector() {


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
