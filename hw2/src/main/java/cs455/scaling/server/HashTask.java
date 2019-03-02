package cs455.scaling.server;

import cs455.scaling.util.SHA;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class HashTask implements Task {

    private final ChannelWrapper wrapper;

    public HashTask(ChannelWrapper wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public void execute() {
        try {
            byte[] data = this.readFromChannel();
            String hash = SHA.SHA1FromBytesPadded(data, 40);
            this.writeToChannel(hash);
//            Server.getTheInstance().finishTask(channel);
            this.wrapper.throughput.incrementAndGet();

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    private byte[] readFromChannel() throws IOException {
        byte[] ret = new byte[8000];
        ByteBuffer buf = ByteBuffer.wrap(ret);

        try {
            this.wrapper.readLock.lock();
            int bytesRead = this.wrapper.channel.read(buf);

        } finally {
            this.wrapper.readLock.unlock();
        }

        return ret;
    }

    private void writeToChannel(String hash) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(hash.getBytes(StandardCharsets.US_ASCII));

        try {
            this.wrapper.writeLock.lock();
            while (buf.hasRemaining()) {
                this.wrapper.channel.write(buf);
            }

        } finally {
            this.wrapper.writeLock.unlock();
        }
    }

}
