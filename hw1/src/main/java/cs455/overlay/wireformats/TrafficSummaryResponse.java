package cs455.overlay.wireformats;

import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;

public class TrafficSummaryResponse implements Event {

    public final String ip;
    public final int port;
    public final int sentTracker;
    public final long sentSum;
    public final int receiveTracker;
    public final long receiveSum;
    public final int relayedTracker;

    public TrafficSummaryResponse(ByteBuffer buf){
        ip = BufUtils.getString(buf);
        port = buf.getInt();
        sentTracker = buf.getInt();
        sentSum = buf.getLong();
        receiveTracker = buf.getInt();
        receiveSum = buf.getLong();
        relayedTracker = buf.getInt();
    }

    @Override
    public String toString(){
        return ip+":"+port+"\t"+sentTracker+"\t"+receiveTracker+"\t"+sentSum+"\t"+receiveSum+"\t"+relayedTracker;
    }

    @Override
    public int getCode() {
        return 9;
    }
}
