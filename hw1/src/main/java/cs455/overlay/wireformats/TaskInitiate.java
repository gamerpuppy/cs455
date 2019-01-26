package cs455.overlay.wireformats;

public class TaskInitiate implements Event {
    @Override
    public int getType() {
        return 6;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
