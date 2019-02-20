package cs455.overlay.wireformats;

import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;

public class Connect implements Event {

    public final String ip;
    public final int port;

    public Connect(ByteBuffer buf){
        ip = BufUtils.getString(buf);
        port = buf.getInt();
    }

    @Override
    public int getCode() {
        return 11;
    }
}
