package cs455.overlay.wireformats;

public class RegisterResponse implements Event {
    @Override
    public int getType() {
        return 1;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
