package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class TrafficSummaryResponse implements Event {

    public TrafficSummaryResponse(ByteBuffer buf){

    }

    @Override
    public int getCode() {
        return 9;
    }
}
