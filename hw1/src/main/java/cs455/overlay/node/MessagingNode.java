package cs455.overlay.node;

import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.BufUtils;
import cs455.overlay.util.Logger;
import cs455.overlay.wireformats.DeregisterResponse;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.EventFactory;
import cs455.overlay.wireformats.RegisterResponse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class MessagingNode implements Node {

    @Override
    public synchronized void onEvent(Event event, SocketChannel socketChannel) {
        switch (event.getCode()){
            case 1: this.onRegisterResponse((RegisterResponse) event); break;
            case 3: this.onDeregisterResponse((DeregisterResponse) event); break;
            default: System.out.println("Event received: type not recognized"+event.getCode());
        }
    }

    private void onRegisterResponse(RegisterResponse resp){
        Logger.log("received register response with status:"+resp.status+" info:"+resp.info);
    }

    private void onDeregisterResponse(DeregisterResponse resp){
        Logger.log("received deregister response with status:"+resp.status+" info:"+resp.info);

    }

    private synchronized void sendDeregisterRequest(){

        try {
            SocketChannel socketChannel = TCPServerThread.getTheInstance().registry;

            String ipAddress = socketChannel.socket().getLocalAddress().getHostName();
            int port = socketChannel.socket().getLocalPort();

            Logger.log("sending deregister request with ip:"+ipAddress+" port:"+port);

            ByteBuffer buf = ByteBuffer.allocate(256);
            buf.putInt(EventFactory.DEREGISTER_REQUEST);
            BufUtils.putString(buf, ipAddress);
            buf.putInt(port);
            buf.flip();

            socketChannel.write(buf);

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private synchronized void sendRegistrationRequest(){
        try {
            SocketChannel socketChannel = TCPServerThread.getTheInstance().registry;

            String ipAddress = socketChannel.socket().getLocalAddress().getHostName();
            int port = socketChannel.socket().getLocalPort();

            Logger.log("sending registration request with ip:"+ipAddress+" port:"+port);

            ByteBuffer buf = ByteBuffer.allocate(256);
            buf.putInt(EventFactory.REGISTER_REQUEST);
            BufUtils.putString(buf, ipAddress);
            buf.putInt(port);
            buf.flip();

            socketChannel.write(buf);

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        try {

            if (args.length < 2) {
                System.out.println("Please provide host and port number");
                return;
            }

            String host = args[0];
            int port = Integer.parseInt(args[1]);

            MessagingNode msgNode = new MessagingNode();
            TCPServerThread serverThread = new TCPServerThread(msgNode);
            serverThread.setupMessaging(new InetSocketAddress(host, port), 10001);
            TCPServerThread.setTheInstance(serverThread);

            Thread thread = new Thread(serverThread);
            thread.start();


            msgNode.sendRegistrationRequest();

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                String instruction = br.readLine();

                if(instruction.equals("exit-overlay")){
                    msgNode.sendDeregisterRequest();
                }

            }

        } catch(Exception e){
            e.printStackTrace();
        }
    }

}
