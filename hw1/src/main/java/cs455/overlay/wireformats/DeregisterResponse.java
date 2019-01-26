package cs455.overlay.wireformats;

public class DeregisterResponse implements Event {
    @Override
    public int getType() {
        return 3;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
