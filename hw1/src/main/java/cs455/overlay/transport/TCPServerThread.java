package cs455.overlay.transport;

import cs455.overlay.node.MessagingNode;
import cs455.overlay.util.Logger;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class TCPServerThread implements Runnable {

    public ServerSocket server;
    public List<SocketContainer> socketContainers = new ArrayList<>();


    public TCPServerThread(int portToBind) throws IOException {
        server = new ServerSocket(portToBind);
        Logger.log("server bound to "+server.getLocalPort());
    }

    public TCPServerThread(){
        server = getServerSocket(MessagingNode.defaultPort);
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

    private ServerSocket getServerSocket(int port) {
          try {
              return new ServerSocket(port);
          } catch(Exception e){
              return getServerSocket(port+1);
          }
    }

}
