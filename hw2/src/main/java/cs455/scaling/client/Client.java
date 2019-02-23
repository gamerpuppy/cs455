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
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class Client {

    final SocketChannel channel;
    final List<String> unconfirmedHashes;
    final double msgRate;

    public Client(String host, int port, double msgRate) throws IOException {
        channel = SocketChannel.open(new InetSocketAddress(host, port));
        unconfirmedHashes = new LinkedList<>();
        this.msgRate = msgRate;
        ReceiverThread receiverThread = new ReceiverThread(channel);

        Thread thread = new Thread(receiverThread);
        thread.start();
    }

    public void loop() throws Exception {
        Random r = new Random();
        byte[] b = new byte[8000];

        while(true){
            r.nextBytes(b);
            String hash = SHA.SHA1FromBytesPadded(b, 40);

            synchronized (this.unconfirmedHashes) {
                unconfirmedHashes.add(hash);
            }

            ByteBuffer outBuf = ByteBuffer.wrap(hash.getBytes(StandardCharsets.US_ASCII));
            while(outBuf.hasRemaining())
                channel.write(outBuf);

            Thread.sleep((int)(1000/this.msgRate));
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
                    for(SelectionKey key : this.selector.selectedKeys()) {

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

            synchronized (Client.this.unconfirmedHashes) {
                unconfirmedHashes.remove(hash);
            }
        }

    }

}
