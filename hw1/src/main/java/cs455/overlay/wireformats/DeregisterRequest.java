package cs455.overlay.wireformats;

import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;

public class DeregisterRequest implements Event {

    public final String ipAddress;
    public final int port;

    public DeregisterRequest(ByteBuffer buf){
        ipAddress = BufUtils.getString(buf);
        port = buf.getInt();
    }

    @Override
    public int getCode() {
        return 2;
    }
}
