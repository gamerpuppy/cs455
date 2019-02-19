package cs455.overlay.node;

import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.BufUtils;
import cs455.overlay.util.Logger;
import cs455.overlay.wireformats.Event;
import cs455.overlay.wireformats.EventFactory;
import cs455.overlay.wireformats.RegisterRequest;
import cs455.overlay.wireformats.DeregisterRequest;
import javafx.collections.transformation.SortedList;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class Registry implements Node {

    List<SocketChannel> channels;
    Map<SocketChannel, NodeInfo> nodeInfoMap;

    private Registry(){
        this.channels = new ArrayList<>();
        this.nodeInfoMap = new HashMap<>();
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
        for(SocketChannel channel : this.channels){
            NodeInfo node = this.nodeInfoMap.get(channel);
            for(NodeInfo dest : node.links.keySet()){
                if(node.nodeId < dest.nodeId)
                    System.out.println(node.toString() + " " + dest.toString() + " " + node.links.get(dest));
            }
        }
    }

    private synchronized void setupOverlay(int connections){
        Logger.log("setting up overlay with "+connections+" connections");

        Random rand = new Random();

        boolean found = false;
        int iterations = 0;
        while(!found && iterations < 500) {

            ArrayList<NodeInfo> availableNodes = new ArrayList<>();

            for (int i = 0; i < channels.size(); i++) {
                NodeInfo node = this.nodeInfoMap.get(channels.get(i));
                int idx = (i + 1) % channels.size();
                NodeInfo node2 = this.nodeInfoMap.get(channels.get(idx));
                int weight = rand.nextInt(10) + 1;

                if(!node.links.containsKey(node2))
                    node.links.put(node2, weight);

                if(!node2.links.containsKey(node))
                    node2.links.put(node, weight);

                availableNodes.add(node);
            }

            while(availableNodes.size() > 1){
                int idx1 = rand.nextInt(availableNodes.size());
                NodeInfo node1 = availableNodes.get(idx1);
                int idx2 = 0;

                int i = 0;
                for(; i < 100; i++) {
                    idx2 = rand.nextInt(availableNodes.size());
                    NodeInfo node2 = availableNodes.get(idx2);

                    if(node1 == node2)
                        continue;

                    if (!node1.links.containsKey(node2))
                        break;
                }
                if(i == 100) {
                    break;
                }

                NodeInfo node2 = availableNodes.get(idx2);

                if(node1.links.size() >=  connections) {
                    availableNodes.remove(node1);
                    continue;
                }

                if(node2.links.size() >=  connections) {
                    availableNodes.remove(node2);
                    continue;
                }

                int weight = rand.nextInt(10) + 1;
                node1.links.put(node2, weight);
                node2.links.put(node1, weight);



                if(node1.links.size() == connections)
                    availableNodes.remove(node1);

                if(node2.links.size() == connections)
                    availableNodes.remove(node2);
            }

            if(availableNodes.size() == 0)
                found = true;
            else
                clearOverlay();

            iterations++;
        }

        if(iterations < 500)
            Logger.log("found a good overlay in " + iterations + "iterations");
        else
            Logger.log("could not find a good overlay in " + iterations + "iterations");

    }

    private synchronized void clearOverlay(){
        for(SocketChannel channel : this.channels){
            NodeInfo node = this.nodeInfoMap.get(channel);
            node.links = new HashMap<>();
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
                    int connections = Integer.parseInt(instruction.substring(14));
                    reg.setupOverlay(connections);

                } else if(instruction.equals("send-overlay-link-weights")){

                } else if(instruction.equals("clear-overlay")){
                    reg.clearOverlay();
                } else if(instruction.equals("port")){
                    System.out.println(Node.getMyIp() + ":" + Node.getMyPort());
                }

            }

        } catch(Exception e){
            e.printStackTrace();
        }
    }

    class NodeInfo{

        final int nodeId;
        final String ipAddr;
        final int port;
        Map<NodeInfo, Integer> links;

        NodeInfo(int nodeId, String ipAddr, int port){
            this.nodeId = nodeId;
            this.ipAddr = ipAddr;
            this.port = port;
            links = new HashMap<>();
        }

        @Override
        public String toString(){
            return ipAddr+":"+port;
        }

    }

}
