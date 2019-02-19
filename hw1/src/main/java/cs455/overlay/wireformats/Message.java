package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class Message implements Event{

    public Message(ByteBuffer buf){

    }

    @Override
    public int getCode() {
        return 10;
    }
}
