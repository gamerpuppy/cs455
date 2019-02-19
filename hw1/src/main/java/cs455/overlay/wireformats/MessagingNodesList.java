package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class MessagingNodesList implements Event {


    public MessagingNodesList(ByteBuffer buf){

    }

    @Override
    public int getCode() {
        return 4;
    }
}
