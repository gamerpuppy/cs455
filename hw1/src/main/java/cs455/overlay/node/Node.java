package cs455.overlay.node;

import cs455.overlay.wireformats.Event;

import java.nio.channels.SocketChannel;

public interface Node {

    public void onEvent(Event event, SocketChannel socketChannel);

}
