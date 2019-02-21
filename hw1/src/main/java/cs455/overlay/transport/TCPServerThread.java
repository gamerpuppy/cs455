package cs455.overlay.transport;

import cs455.overlay.node.Node;
import cs455.overlay.util.Logger;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TCPServerThread implements Runnable {

    public static final int defaultPort = 10001;

    public ServerSocket server;
    public List<SocketContainer> socketContainers = new ArrayList<>();


    public TCPServerThread(int portToBind) throws IOException {
        server = new ServerSocket(portToBind);
        Logger.log("server bound to "+server.getLocalPort());
    }

    public TCPServerThread() throws IOException {
        server = new ServerSocket();
        bindServerSocket(server, defaultPort);

        Logger.log("server bound to "+server.getLocalPort());
    }

    @Override
    public void run() {
        try {
            while(true){
                Socket socket = server.accept();
                SocketContainer container = new SocketContainer(socket);
                socketContainers.add(container);
            }

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private void bindServerSocket(ServerSocket s, int port) throws IOException{
        while(!s.isBound() && port < 0xFFFF){
            try {
                s.bind(new InetSocketAddress("127.0.0.1", port));
            } catch(Exception e){
                Logger.log("exception trying to bind to myPort "+ port);
                port++;
            }
        }
        if(!s.isBound()){
            throw new IOException();
        }

        Logger.log("bound to "+s.getLocalPort());

    }

}
