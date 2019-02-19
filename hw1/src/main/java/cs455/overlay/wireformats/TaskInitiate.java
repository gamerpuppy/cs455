package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class TaskInitiate implements Event {

    public TaskInitiate(ByteBuffer buf){

    }

    @Override
    public int getCode() {
        return 6;
    }
}
