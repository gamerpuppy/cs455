package cs455.overlay.node;

import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.BufUtils;
import cs455.overlay.util.Logger;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.EventFactory;
import cs455.overlay.wireformats.RegisterRequest;
import cs455.overlay.wireformats.DeregisterRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class Registry implements Node {

    List<SocketChannel> channels;
    Map<SocketChannel, NodeInfo> nodeInfoMap;
    List<Link> linkList;

    private Registry(){
        this.channels = new ArrayList<>();
        this.nodeInfoMap = new HashMap<>();
        this.linkList = new ArrayList<>();
    }

    @Override
    public synchronized void onEvent(Event event, SocketChannel socketChannel) {
        switch (event.getCode()) {
            case 0: this.onRegisterRequest((RegisterRequest) event, socketChannel); break;
            case 2: this.onDeregisterRequest((DeregisterRequest) event, socketChannel); break;
            default:
                System.out.println("Event received: type not recognized");
        }
    }

    private void onRegisterRequest(RegisterRequest req, SocketChannel socketChannel) {
        Logger.log("received register request from: ip:"+req.ipAddress+" port:"+req.port);

        int nodeId = channels.size();

        String socketAddr = socketChannel.socket().getInetAddress().getHostName();
        int socketPort = socketChannel.socket().getPort();

        boolean doesMatch = socketAddr.equals(req.ipAddress)
                && socketPort == req.port;

        if(doesMatch){
            this.channels.add(socketChannel);
            this.nodeInfoMap.put(socketChannel, new NodeInfo(nodeId, socketAddr, socketPort));
        }

        Logger.log("sending register response with status:"+doesMatch+" id:"+nodeId);

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.REGISTER_RESPONSE);
        buf.put((byte)(doesMatch ? 1 : 0));
        BufUtils.putString(buf, Integer.toString(nodeId));
        buf.flip();

        try {
            socketChannel.write(buf);
        } catch(IOException e){
            Logger.log("failure to write register response, removing node id:"+nodeId);
            this.channels.remove(socketChannel);
            this.nodeInfoMap.remove(socketChannel);
        }
    }

    private void onDeregisterRequest(DeregisterRequest req, SocketChannel socketChannel) {
        Logger.log("received deregister request from: ip:"+req.ipAddress+" port:"+req.port);

        String socketAddr = socketChannel.socket().getInetAddress().getHostName();
        int socketPort = socketChannel.socket().getPort();

        boolean doesMatch = socketAddr.equals(req.ipAddress)
                && socketPort == req.port;

        boolean isRegistered = this.nodeInfoMap.containsKey(socketChannel);

        Logger.log("deregister request ip "+ (doesMatch ? "does match" : "doesn't match"));
        Logger.log("deregister request was sent from a "+(isRegistered ? "": "un")+"registered node");

        boolean success = doesMatch && isRegistered;

        if(success){
            this.channels.remove(socketChannel);
            this.nodeInfoMap.remove(socketChannel);
        }

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.DEREGISTER_RESPONSE);
        buf.put((byte)(success ? 1 : 0));
        BufUtils.putString(buf, (success ? "successfully unregistered" : "failed to unregister"));
        buf.flip();

        try {
            socketChannel.write(buf);
        } catch(IOException e){
            Logger.log("failure to write deregister response");
        }
    }

    private synchronized void listMessagingNodes(){
        for(SocketChannel channel : this.channels){
            System.out.println(this.nodeInfoMap.get(channel).toString());
        }
    }

    private synchronized void listWeights(){
        for(Link link : this.linkList){
            System.out.println(link.toString());
        }
    }

    public static void main(String[] args){
        try {

            if (args.length < 1) {
                System.out.println("Please provide port number");
                return;
            }

            int port = Integer.parseInt(args[0]);
            Registry reg = new Registry();
            TCPServerThread serverThread = new TCPServerThread(reg);
            serverThread.setupRegistry(port);
            TCPServerThread.setTheInstance(serverThread);

            Thread thread = new Thread(serverThread);
            thread.start();


            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                String instruction = br.readLine();

                if(instruction.equals("list-messaging-nodes")){
                    reg.listMessagingNodes();
                } else if(instruction.equals("list-weights")){
                    reg.listWeights();
                } else if(instruction.matches("setup-overlay \\d+")){

                } else if(instruction.equals("send-overlay-link-weights")){

                }

            }

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    class NodeInfo {

        final int nodeId;
        final String ipAddr;
        final int port;

        NodeInfo(int nodeId, String ipAddr, int port){
            this.nodeId = nodeId;
            this.ipAddr = ipAddr;
            this.port = port;
        }

        @Override
        public String toString(){
            return ipAddr+":"+port;
        }

    }

    class Link {

        final NodeInfo a;
        final NodeInfo b;
        final int weight;

        Link(NodeInfo a, NodeInfo b){
            this.a = a;
            this.b = b;
            weight = new Random().nextInt();
        }

        @Override
        public String toString() {
            return a.toString() + " " + b.toString() + " " + weight;
        }
    }
}
