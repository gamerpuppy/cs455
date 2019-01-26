package cs455.overlay.wireformats;

public class TaskComplete implements Event {
    @Override
    public int getType() {
        return 7;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
