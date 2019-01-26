package cs455.overlay.wireformats;

public class Message implements Event{

    @Override
    public int getType() {
        return 10;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
