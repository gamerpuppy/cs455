package cs455.overlay.wireformats;

public class LinkWeights implements Event {
    @Override
    public int getType() {
        return 5;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
