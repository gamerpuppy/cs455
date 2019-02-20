package cs455.overlay.node;

import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.wireformats.Event;

import java.nio.channels.SocketChannel;

public abstract class Node {

    public abstract void  onEvent(Event event, SocketChannel socketChannel);

    public String ipAddress;
    public int port;

}
