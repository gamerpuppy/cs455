package cs455.overlay.transport;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TCPServerThread implements Runnable {

    @Override
    public void run() {
        ServerSocket server = aquireServerSocket();

        if(server == null){
            System.err.println("TCPServerThread: server was null exiting.");
            return;
        }

        if(!server.isBound()){
            System.err.println("TCPServerThread: server was null exiting.");
            return;
        }

        while(true){
            try {
                Socket socket = server.accept();
                TCPReceiverThread receiver = new TCPReceiverThread(socket);
                Thread thread = new Thread(receiver);
                thread.start();

            } catch(IOException ioe){
                System.err.println("TCPServerThread: error when accepting socket.");
                System.err.print(ioe.getMessage());
            }
        }


    }

    public ServerSocket aquireServerSocket(){
        int port = 9999;
        while(port < 65356){
            try {
                ServerSocket server = new ServerSocket(port);
                if(server != null)
                    return server;

            } catch(IOException ioe){
                System.err.println("TCPServerThread: error when creating ServerSocket on port "+port+".");
                System.err.print(ioe.getMessage());
            }

        }
        return null;
    }
}
