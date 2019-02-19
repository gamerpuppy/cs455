package cs455.overlay.wireformats;

import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;

public class DeregisterResponse implements Event {

    public final boolean status;
    public final String info;

    public DeregisterResponse(ByteBuffer buf){
        status = buf.get() != 0;
        info = BufUtils.getString(buf);
    }

    @Override
    public int getCode() {
        return 3;
    }
}
