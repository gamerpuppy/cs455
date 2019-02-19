package cs455.overlay.wireformats;

import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;

public class RegisterResponse implements Event {

    public final boolean status;
    public final String info;

    public RegisterResponse(ByteBuffer buf){
        this.status = buf.get() != 0;
        this.info = BufUtils.getString(buf);
    }

    @Override
    public int getCode() {
        return 1;
    }
}
