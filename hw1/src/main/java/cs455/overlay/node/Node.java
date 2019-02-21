package cs455.overlay.node;

import cs455.overlay.transport.SocketContainer;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.wireformats.Event;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;

public abstract class Node {

    public abstract void  onEvent(Event event, SocketContainer socket);

    public String myIpAddress;
    int myPort;

    public static Node theInstance;

}
