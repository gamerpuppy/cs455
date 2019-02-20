package cs455.overlay.wireformats;

public class InvalidEvent implements Event{
    @Override
    public int getCode() {
        return 12;
    }
}
