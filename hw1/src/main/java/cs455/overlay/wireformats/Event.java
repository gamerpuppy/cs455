package cs455.overlay.wireformats;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public interface Event {

    public int getCode();


    public static void main(String[] args){
        String ipAddress = "127.0.0.1";
        int port = 10000;

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.REGISTER_REQUEST);
        buf.put(ipAddress.getBytes(StandardCharsets.US_ASCII));
        buf.put((byte)0);
        buf.putInt(port);

        int len = buf.limit();
        buf.flip();

        RegisterRequest e = (RegisterRequest) EventFactory.createEvent(len, buf);

        System.out.println(e.ipAddress);
        System.out.println(e.port);
    }


}
