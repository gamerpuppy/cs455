package cs455.overlay.wireformats;

import java.nio.ByteBuffer;

public class LinkWeights implements Event {

    public LinkWeights(ByteBuffer buf){

    }

    @Override
    public int getCode() {
        return 5;
    }
}
