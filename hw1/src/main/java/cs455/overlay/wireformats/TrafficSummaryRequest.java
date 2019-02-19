package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class TrafficSummaryRequest implements Event {

    public TrafficSummaryRequest(ByteBuffer buf){

    }

    @Override
    public int getCode() {
        return 8;
    }
}
