package cs455.overlay.wireformats;

public class RegisterRequest implements Event {
    @Override
    public int getType() {
        return 0;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
