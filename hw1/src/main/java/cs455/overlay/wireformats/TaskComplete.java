package cs455.overlay.wireformats;

public class TaskComplete implements Event {
    @Override
    public Type getType() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
