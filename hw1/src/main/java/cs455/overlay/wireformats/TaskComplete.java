package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class TaskComplete implements Event {

    public TaskComplete(ByteBuffer buf){

    }

    @Override
    public int getCode() {
        return 7;
    }
}
