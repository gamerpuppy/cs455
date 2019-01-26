package cs455.overlay.wireformats;

public class TaskSummaryRequest implements Event {
    @Override
    public Type getType() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }
}
