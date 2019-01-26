package cs455.overlay.wireformats;

public class MessagingNodesList implements Event {

    @Override
    public int getType() {
        return 4;
    }

    @Override
    public byte[] getBytes() {
        return new byte[0];
    }

    public MessagingNodesList(byte data[]){

    }

}
