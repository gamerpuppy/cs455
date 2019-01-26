package cs455.overlay.wireformats;

public class TrafficSummaryRequest implements Event {
    @Override
    public int getType() {
        return 8;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
