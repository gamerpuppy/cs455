package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class TaskInitiate implements Event {

    public final int rounds;

    public TaskInitiate(ByteBuffer buf){
        this.rounds = buf.getInt();
    }

    @Override
    public int getCode() {
        return 6;
    }
}
