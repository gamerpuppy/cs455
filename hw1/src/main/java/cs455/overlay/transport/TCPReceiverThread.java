package cs455.overlay.transport;

import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class TCPReceiverThread implements Runnable {

    private Socket socket;
    private DataInputStream din;

    public TCPReceiverThread(Socket socket) throws IOException {
        this.socket = socket;
        din = new DataInputStream(socket.getInputStream());
    }

    @Override
    public void run() {

        while(socket != null){
            try {

                int dataLength = din.readInt();
                byte[] data = new byte[dataLength];
                din.readFully(data, 0, dataLength);

            } catch (SocketException se) {
                se.printStackTrace();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }

    }
}
