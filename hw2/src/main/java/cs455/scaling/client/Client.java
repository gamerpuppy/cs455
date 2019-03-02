package cs455.scaling.client;

import cs455.scaling.util.SHA;

import java.lang.Thread;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Client {

    private final SocketChannel channel;
    private final ConcurrentHashMap<String, Object> unconfirmedHashes = new ConcurrentHashMap<>();
    private final AtomicInteger sentCount = new AtomicInteger(0);
    private final AtomicInteger receivedCount = new AtomicInteger(0);
    private final double msgRate;

    public Client(String host, int port, double msgRate) throws IOException {
        channel = SocketChannel.open(new InetSocketAddress(host, port));
        channel.configureBlocking(false);

        this.msgRate = msgRate;
    }

    public void loop() throws Exception {
        Thread thread = new Thread(new ReceiverThread(channel));
        thread.start();

        Thread thread2 = new Thread(new ClientStatisticsThread());
        thread2.start();

        Random r = new Random();
        byte[] b = new byte[8000];

        long lastLoopTime = System.currentTimeMillis();
        final long millisBetweenStarts = (long)(1/this.msgRate * 1000);

        while(true) {
            r.nextBytes(b);
            String hash = SHA.SHA1FromBytesPadded(b, 40);
//            System.out.println("sending hash " + hash.substring(0, 8));

            ByteBuffer outBuf = ByteBuffer.wrap(b);
            while (outBuf.hasRemaining())
                this.channel.write(outBuf);

            this.unconfirmedHashes.put(hash, new Object());
            this.sentCount.getAndIncrement();

            try {
                long nextLoopStartTime = lastLoopTime + millisBetweenStarts;
                long sleepTime = nextLoopStartTime - System.currentTimeMillis();
                if (sleepTime > 0)
                    Thread.sleep(sleepTime);
                lastLoopTime = nextLoopStartTime;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        if(args.length < 3){
            System.out.println("Usage: host port message-rate");
            System.exit(1);
        }

        final String host = args[0];
        final int port = Integer.parseInt(args[1]);
        final double msgRate = Double.parseDouble(args[2]);

        try {
            Client client = new Client(host, port, msgRate);
            client.loop();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class ReceiverThread implements Runnable {

        private Selector selector;

        public ReceiverThread(SocketChannel channel) throws IOException{
            this.selector = Selector.open();
            channel.register(selector, SelectionKey.OP_READ);
            channel.configureBlocking(false);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    this.selector.select();
                    Iterator<SelectionKey> it = this.selector.selectedKeys().iterator();
                    while(it.hasNext()) {
                        SelectionKey key = it.next();
                        it.remove();

                        if(key.isReadable()){
                            readKey(key);
                        }

                    }
                }
            } catch(Exception e){
                e.printStackTrace();
                System.exit(1);
            }
        }

        private void readKey(SelectionKey key) throws Exception {
            SocketChannel channel = (SocketChannel) key.channel();
            ByteBuffer buf = ByteBuffer.allocate(40);
            int numRead = channel.read(buf);

            if(numRead < 40){
                throw new Exception("only "+numRead+" bytes read");
            }

            String hash = new String(buf.array(), StandardCharsets.US_ASCII);
//            System.out.println("received hash " + hash.substring(0,8));

            Client.this.unconfirmedHashes.remove(hash);
            Client.this.receivedCount.getAndIncrement();
        }

    }

    class ClientStatisticsThread implements Runnable {

        private final double UPDATE_INTERVAL = 20d;

        @Override
        public void run() {

            long lastLoopTime = System.currentTimeMillis();
            final long millisBetweenStarts = (long)(UPDATE_INTERVAL * 1000);

            while(true) {
                try {
                    long nextLoopStartTime = lastLoopTime + millisBetweenStarts;
                    long sleepTime = nextLoopStartTime - System.currentTimeMillis();
                    if(sleepTime > 0)
                        Thread.sleep(sleepTime);
                    lastLoopTime = nextLoopStartTime;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int sentValue = Client.this.sentCount.getAndSet(0);
                int receivedValue = Client.this.receivedCount.getAndSet(0);

                System.out.printf("[timestamp] Total Sent Count: %d, Total Received Count: %d\n",sentValue , receivedValue);


            }
        }

    }

}
