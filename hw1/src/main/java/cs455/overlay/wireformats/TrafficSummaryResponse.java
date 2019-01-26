package cs455.overlay.wireformats;

public class TrafficSummaryResponse implements Event {
    @Override
    public int getType() {
        return 9;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
