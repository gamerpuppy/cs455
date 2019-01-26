package cs455.overlay.wireformats;

public interface Event {

    enum Type {

    }

    public Type getType();

    public byte[] getBytes();

}
