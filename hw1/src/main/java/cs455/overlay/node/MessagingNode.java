package cs455.overlay.node;

import cs455.overlay.dijkstra.RoutingCache;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.BufUtils;
import cs455.overlay.util.Logger;
import cs455.overlay.wireformats.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessagingNode extends Node {

    List<NodeInfo> connections = new ArrayList<>();
    Map<String, NodeInfo> allNodes = new HashMap<>();

    RoutingCache routingCache = null;

    @Override
    public synchronized void onEvent(Event event, SocketChannel socketChannel) {
        switch (event.getCode()){
            case 1: this.onRegisterResponse((RegisterResponse) event); break;
            case 3: this.onDeregisterResponse((DeregisterResponse) event); break;
            case 4: this.onMessagingNodeList((MessagingNodesList) event); break;
            case 5: this.onLinkWeights((LinkWeights) event); break;

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

    private void onMessagingNodeList(MessagingNodesList list) {
        try {
            Logger.log("received register messaging list with " + list.count + " connections");
            for (NodeInfo connection : list.connections) {
                Logger.log("connecting to: " + connection.ipAddr + ":" + connection.port);

                SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(connection.ipAddr, connection.port));
                socketChannel.configureBlocking(false);
                socketChannel.register(TCPServerThread.getTheInstance().selector, SelectionKey.OP_READ);

                connection.channel = socketChannel;
            }
            connections = list.connections;
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    private void onLinkWeights(LinkWeights weights){
        Logger.log("received register link weights with " + weights.count + " links");

        for(NodeInfo node : connections){
            allNodes.put(node.getId(), node);
        }

        for(LinkWeights.Link link : weights.links){
            String id1 = link.ip1+link.port1;
            if(!allNodes.containsKey(id1)){
                allNodes.put(id1, new NodeInfo(link.ip1, link.port1));
            }
            String id2= link.ip2+link.port2;
            if(!allNodes.containsKey(id2)){
                allNodes.put(id2, new NodeInfo(link.ip2, link.port2));
            }

            NodeInfo node1 = allNodes.get(id1);
            NodeInfo node2 = allNodes.get(id2);

            node1.links.put(node2, link.weight);
            node2.links.put(node1, link.weight);
        }

        routingCache = new RoutingCache(allNodes, ipAddress+port);


    }

    private synchronized void listAllNodes(){

        for(String id : allNodes.keySet()){
            NodeInfo node = allNodes.get(id);

            System.out.println(node.toString());
        }
    }

    private synchronized void printShortestPath(){
        if(routingCache == null)
            return;

        for(String key : allNodes.keySet()){
            String res = routingCache.getRouteString(key);
            if(res.length() > 0){
                System.out.println(res);
            }
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
                } else if(instruction.equals("list-all-nodes")){
                    msgNode.listAllNodes();
                } else if(instruction.equals("print-shortest-path")){
                    msgNode.printShortestPath();
                }

            }

        } catch(Exception e){
            e.printStackTrace();
        }
    }

}
