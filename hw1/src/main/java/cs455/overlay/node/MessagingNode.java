package cs455.overlay.node;

import cs455.overlay.dijkstra.RoutingCache;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.BufUtils;
import cs455.overlay.util.Logger;
import cs455.overlay.util.StatisticsCollectorAndDisplay;
import cs455.overlay.wireformats.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.*;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.*;

public class MessagingNode extends Node {

    Map<String, NodeInfo> allNodes = new HashMap<>();
    RoutingCache routingCache = null;
    String selfKey = null;
    volatile StatisticsCollectorAndDisplay stats = new StatisticsCollectorAndDisplay();

    @Override
    public synchronized void onEvent(Event event, SocketChannel socketChannel) {
        switch (event.getCode()){
            case EventFactory.REGISTER_RESPONSE:
                this.onRegisterResponse((RegisterResponse) event);
                break;
            case EventFactory.DEREGISTER_RESPONSE:
                this.onDeregisterResponse((DeregisterResponse) event);
                break;
            case EventFactory.MESSAGING_NODES_LIST:
                this.onMessagingNodeList((MessagingNodesList) event);
                break;
            case EventFactory.LINK_WEIGHTS:
                this.onLinkWeights((LinkWeights) event);
                break;
            case EventFactory.TASK_INITIATE:
                this.onTaskInitiate((TaskInitiate) event);
                break;
            case EventFactory.MESSAGE:
                this.onMessage((Message) event);
                break;
            case EventFactory.CONNECT:
                this.onConnect((Connect) event, socketChannel);
                break;
            case EventFactory.TRAFFIC_SUMMARY_REQUEST:
                this.onTrafficSummaryRequest((TrafficSummaryRequest) event);
                break;

            default: System.out.println("Event received: type not recognized"+event.getCode());
        }
    }

    private void sendTrafficSummaryResponse(){
        Logger.log("sending traffic summary response");

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.TRAFFIC_SUMMARY_RESPONSE);
        BufUtils.putString(buf, ipAddress);
        buf.putInt(port);
        buf.putInt(stats.getSendTracker());
        buf.putLong(stats.getSentSum());
        buf.putInt(stats.getReceiveTracker());
        buf.putLong(stats.getReceivedSum());
        buf.putInt(stats.getRelayTracker());
        buf.flip();

        SocketChannel socketChannel = TCPServerThread.getTheInstance().registry;
        try {
            socketChannel.write(buf);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private void onTrafficSummaryRequest(TrafficSummaryRequest req) {
        Logger.log("received traffic summary request");
        sendTrafficSummaryResponse();
        Logger.log("reseting traffic stats");
        stats.reset();

    }

    private void onRegisterResponse(RegisterResponse resp){
        Logger.log("received register response with status:"+resp.status+" info:"+resp.info);
    }

    private void onDeregisterResponse(DeregisterResponse resp){
        Logger.log("received deregister response with status:"+resp.status+" info:"+resp.info);
        System.exit(0);
    }

    private void onConnect(Connect connect, SocketChannel channel){
        Logger.log("received connect from "+connect.ip+":"+connect.port);

        String key = connect.ip+connect.port;

        if(allNodes.containsKey(key)){
            allNodes.get(key).channel = channel;
        } else {
            NodeInfo node = new NodeInfo(connect.ip, connect.port);
            node.channel = channel;
            allNodes.put(key, node);
        }

    }

    private void onMessage(Message message){
//        Logger.log("received message with hops "+message.hopsIncluded+" and payload "+message.payload);

        for(int i = 0; i < message.routingPlan.size(); i++){
            NodeInfo cur = message.routingPlan.get(i);

            if(cur.getId().equals(selfKey)){
                if(i == message.routingPlan.size()-1){
                    stats.incReceivedSum(message.payload);
                    stats.incReceiveTracker();

                    Logger.log("was destination for message with payload "+message.payload);

                } else {
                    stats.incMessagesRelayed();
                    NodeInfo next = message.routingPlan.get(i+1);
                    SocketChannel channel = allNodes.get(next.getId()).channel;

                    Logger.log("relaying message with payload "+message.payload+" to next hop "+next.getId());
                    sendMessage(message.routingPlan, message.payload, channel);
                }
                break;
            }
        }
    }

    private void sendMessage(List<NodeInfo> route, int payload, SocketChannel channel){

        ByteBuffer buf = ByteBuffer.allocate(32768);
        buf.putInt(EventFactory.MESSAGE);
        buf.putInt(payload);
        buf.putInt(route.size());

        for(int i = 0; i < route.size(); i++){
            NodeInfo node = route.get(i);
            BufUtils.putString(buf, node.ipAddr);
            buf.putInt(node.port);
        }
        buf.flip();

        byte[] data = new byte[buf.limit()];
        buf.get(data, 0, buf.limit());

        try {
            ByteBuffer buf2 = ByteBuffer.wrap(data).asReadOnlyBuffer();
            channel.write(buf2);
        } catch(IOException e){
            e.printStackTrace();
        }
    }

    private synchronized void sendTaskComplete(){
        Logger.log("sending task complete");

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.TASK_COMPLETE);
        BufUtils.putString(buf, ipAddress);
        buf.putInt(port);
        buf.putInt(EventFactory.TASK_COMPLETE);
        buf.flip();

        SocketChannel socketChannel = TCPServerThread.getTheInstance().registry;
        try {
            socketChannel.write(buf);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private void onTaskInitiate(TaskInitiate task){

        Logger.log("received task initiate with "+task.rounds+" rounds");

        if(allNodes.keySet().size() < 2)
            return;

        RoundsSender sender = new RoundsSender(task.rounds);
        Thread senderThread = new Thread(sender);
        senderThread.start();
    }

    private synchronized void sendDeregisterRequest(){
        SocketChannel socketChannel = TCPServerThread.getTheInstance().registry;

        Logger.log("sending deregister request with ip:"+ipAddress+" port:"+port);

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.DEREGISTER_REQUEST);
        BufUtils.putString(buf, ipAddress);
        buf.putInt(port);
        buf.flip();

        try {
            socketChannel.write(buf);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private synchronized void sendRegistrationRequest(){
        SocketChannel socketChannel = TCPServerThread.getTheInstance().registry;

        Logger.log("sending registration request with ip:"+ipAddress+" port:"+port);

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.REGISTER_REQUEST);
        BufUtils.putString(buf, ipAddress);
        buf.putInt(port);
        buf.flip();

        try {
            socketChannel.write(buf);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    private void onMessagingNodeList(MessagingNodesList list) {
        try {
            Logger.log("received register messaging list with " + list.count + " connections");

            ByteBuffer buf = ByteBuffer.allocate(256);
            buf.putInt(EventFactory.CONNECT);
            BufUtils.putString(buf, ipAddress);
            buf.putInt(port);
            buf.flip();

            byte[] data = new byte[buf.limit()];
            buf.get(data, 0, buf.limit());

            for (NodeInfo connection : list.connections) {

                SocketChannel socketChannel;
                if(ipAddress.equals(connection.ipAddr)){
                    Logger.log("connecting to: localhost:" + connection.port);
                    socketChannel = SocketChannel.open(new InetSocketAddress("localhost", connection.port));

                } else {
                    Logger.log("connecting to: "+connection.ipAddr + ":" + connection.port);
                    socketChannel = SocketChannel.open(new InetSocketAddress(connection.ipAddr, connection.port));

                }

                socketChannel.configureBlocking(false);
                socketChannel.register(TCPServerThread.getTheInstance().selector, SelectionKey.OP_READ);

                connection.channel = socketChannel;

                ByteBuffer buf2 = ByteBuffer.wrap(data).asReadOnlyBuffer();
                socketChannel.write(buf2);

                allNodes.put(connection.getId(), connection);
            }



        } catch(IOException e){
            e.printStackTrace();
        }
    }

    private void onLinkWeights(LinkWeights weights){
        Logger.log("received register link weights with " + weights.count + " links");

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

            msgNode.selfKey = msgNode.ipAddress+msgNode.port;

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

    class RoundsSender implements Runnable {

        final int rounds;

        public RoundsSender(int rounds ){
            this.rounds = rounds;
        }

        @Override
        public void run() {
            Random rand = new Random();
            List<String> keys = new ArrayList<>();
            for(String key : allNodes.keySet())
                if(!key.equals(selfKey))
                    keys.add(key);

            for(int i = 0; i < rounds; i++){
                String destKey = keys.get(rand.nextInt(keys.size()));
                int payload = rand.nextInt();
                List<NodeInfo> route = routingCache.getRouteTo(destKey);
                Logger.log("sending message to "+destKey+" with payload "+payload);

                stats.incSendTracker();
                stats.incSentSum(payload);

                sendMessage(route, payload, route.get(1).channel);
            }

            sendTaskComplete();
        }
    }

}
