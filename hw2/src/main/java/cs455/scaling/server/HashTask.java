package cs455.scaling.server;

import cs455.scaling.util.SHA;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class HashTask implements Task {

    private final SocketChannel channel;

    public HashTask(SocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void execute() {
        try {
            byte[] data = readFromChannel();
            String hash = SHA.SHA1FromBytesPadded(data, 40);

//            System.out.println("sending hash " + hash.substring(0, 8));

            this.writeToChannel(hash);
            Server.getTheInstance().finishTask(channel);

        } catch (IOException e) {
            if(!this.channel.isOpen())
                Server.getTheInstance().removeChannel(this.channel);

        }
    }

    private byte[] readFromChannel() throws IOException {
        byte[] data = new byte[8000];
        ByteBuffer buf = ByteBuffer.wrap(data);
        int bytesRead = this.channel.read(buf);

        if(bytesRead == -1) {
            throw new IOException();
        }

        return data;
    }

    private void writeToChannel(String hash) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(hash.getBytes(StandardCharsets.US_ASCII));

        while(buf.hasRemaining()) {
            channel.write(buf);
        }
    }

}
