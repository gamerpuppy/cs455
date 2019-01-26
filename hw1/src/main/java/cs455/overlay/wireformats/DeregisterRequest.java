package cs455.overlay.wireformats;

public class DeregisterRequest implements Event {
    @Override
    public int getType() {
        return 2;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
