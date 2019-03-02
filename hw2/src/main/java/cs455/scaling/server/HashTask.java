package cs455.scaling.server;

import cs455.scaling.util.SHA;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

public class HashTask implements Task {

    private final SocketChannel channel;

    public HashTask(SocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void execute() {
        try {
            byte[] data = this.readFromChannel();
            String hash = SHA.SHA1FromBytesPadded(data, 40);

//            System.out.println("sending hash " + hash.substring(0, 8));

            this.writeToChannel(hash);

//            counter.getAndIncrement();
//            Server.getTheInstance().reRegisterChannel(channel);

            Server.getTheInstance().finishTask(channel);


        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    private byte[] readFromChannel() throws IOException {
        byte[] ret = new byte[8000];
        ByteBuffer buf = ByteBuffer.wrap(ret);
        
        int bytesRead = channel.read(buf);

        return ret;
    }

    private void writeToChannel(String hash) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(hash.getBytes(StandardCharsets.US_ASCII));

        while(buf.hasRemaining()) {
            channel.write(buf);
        }
    }

}
