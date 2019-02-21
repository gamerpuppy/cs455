package cs455.overlay.node;

import cs455.overlay.dijkstra.RoutingCache;
import cs455.overlay.transport.SocketContainer;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.BufUtils;
import cs455.overlay.util.Logger;
import cs455.overlay.util.StatisticsCollectorAndDisplay;
import cs455.overlay.wireformats.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.*;
import java.util.*;

public class MessagingNode extends Node {

    public static final int defaultPort = 11000;
    Map<NodeInfo, SocketContainer> socketContainerMap = new HashMap<>();
    Map<String, NodeInfo> allNodes = new HashMap<>();

    RoutingCache routingCache = null;
    SocketContainer registry;
    String selfKey;

    StatisticsCollectorAndDisplay stats = new StatisticsCollectorAndDisplay();


    private MessagingNode(String host, int port) throws IOException {
        Node.theInstance = this;
        this.myIpAddress = InetAddress.getLocalHost().getHostName();
//        this.myIpAddress = InetAddress.getLocalHost().getHostAddress();

        TCPServerThread serverThread = new TCPServerThread();
        this.myPort = serverThread.server.getLocalPort();
        this.selfKey = myIpAddress+myPort;

        Thread thread = new Thread(serverThread);
        thread.start();

        Socket socket = new Socket(host, port);
        this.registry = new SocketContainer(socket);
    }

    public synchronized void onEvent(Event event, SocketContainer socket) {
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
                this.onConnect((Connect) event, socket);
                break;
            case EventFactory.TRAFFIC_SUMMARY_REQUEST:
                this.onTrafficSummaryRequest((TrafficSummaryRequest) event);
                break;

            default: System.out.println("Event received: type not recognized ("+event.getCode()+")");
        }
    }

    private void sendRegistrationRequest(){
        Logger.log("sending registration request with ip:"+ myIpAddress +" myPort:"+ myPort);

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.REGISTER_REQUEST);
        BufUtils.putString(buf, myIpAddress);
        buf.putInt(myPort);

        registry.sendData(buf);
    }

    private void onRegisterResponse(RegisterResponse resp){
        Logger.log("received register response with status:"+resp.status+" info:"+resp.info);
    }

    private void sendDeregisterRequest(){
        Logger.log("sending deregister request with ip:"+ myIpAddress +" myPort:"+ myPort);

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.DEREGISTER_REQUEST);
        BufUtils.putString(buf, myIpAddress);
        buf.putInt(myPort);

        registry.sendData(buf);
    }

    private void onDeregisterResponse(DeregisterResponse resp){
        Logger.log("received deregister response with status:"+resp.status+" info:"+resp.info);
        System.exit(0);
    }

    private void sendTrafficSummaryResponse(){
        Logger.log("sending traffic summary response");

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.TRAFFIC_SUMMARY_RESPONSE);
        BufUtils.putString(buf, myIpAddress);
        buf.putInt(myPort);
        buf.putInt(stats.getSendTracker());
        buf.putLong(stats.getSentSum());
        buf.putInt(stats.getReceiveTracker());
        buf.putLong(stats.getReceivedSum());
        buf.putInt(stats.getRelayTracker());

        registry.sendData(buf);
    }

    private void onTrafficSummaryRequest(TrafficSummaryRequest req) {
        Logger.log("received traffic summary request");
        sendTrafficSummaryResponse();
        Logger.log("reseting traffic stats");
        stats.reset();
    }

    private synchronized void onConnect(Connect connect, SocketContainer socket){
        Logger.log("received connect from "+connect.ip+":"+connect.port);

        String key = connect.ip+connect.port;
        if(!allNodes.containsKey(key)){
            NodeInfo node = new NodeInfo(connect.ip, connect.port);
            allNodes.put(key, node);
        }
        socketContainerMap.put(allNodes.get(key), socket);
    }

    private void onMessage(Message message){
        if(message.routingPlan.size() == 0){
            stats.incReceivedSum(message.payload);
            stats.incReceiveTracker();
            Logger.log("was destination for message with payload "+message.payload);
        } else {
            stats.incMessagesRelayed();
            NodeInfo next = allNodes.get(message.routingPlan.get(0).getId());
            message.routingPlan.remove(0);

            SocketContainer socket = socketContainerMap.get(next);
            Logger.log("relaying message with payload "+message.payload+" to next hop "+next.getId());
            sendMessage(message.routingPlan, message.payload, socket);
        }
    }

//    private void onMessage(Message message){
////        Logger.log("received message with hops "+message.hopsIncluded+" and payload "+message.payload);
//
//        for(int i = 0; i < message.routingPlan.size(); i++){
//            NodeInfo cur = message.routingPlan.get(i);
//
//            if(cur.getId().equals(selfKey)){
//                if(i == message.routingPlan.size()-1){
//                    stats.incReceivedSum(message.payload);
//                    stats.incReceiveTracker();
//
//                    Logger.log("was destination for message with payload "+message.payload);
//
//                } else {
//                    stats.incMessagesRelayed();
//                    NodeInfo next = message.routingPlan.get(i+1);
//                    SocketContainer socket = socketContainerMap.get(next);
//
//
//                    Logger.log("relaying message with payload "+message.payload+" to next hop "+next.getId());
//                    sendMessage(message.routingPlan, message.payload, socket);
//                }
//                break;
//            }
//        }
//    }

    private void sendMessage(List<NodeInfo> route, int payload, SocketContainer socket){
        ByteBuffer buf = ByteBuffer.allocate(2048);
        buf.putInt(EventFactory.MESSAGE);
        buf.putInt(payload);
        buf.putInt(route.size());

        for(int i = 0; i < route.size(); i++){
            NodeInfo node = route.get(i);
            BufUtils.putString(buf, node.ipAddr);
            buf.putInt(node.port);
        }

        socket.sendData(buf);
    }

    private void sendTaskComplete(){
        Logger.log("sending task complete");

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.TASK_COMPLETE);
        BufUtils.putString(buf, myIpAddress);
        buf.putInt(myPort);
        buf.putInt(EventFactory.TASK_COMPLETE);

        registry.sendData(buf);
    }

    private void onTaskInitiate(TaskInitiate task){

        Logger.log("received task initiate with "+task.rounds+" rounds");

        if(allNodes.keySet().size() < 2)
            return;

        RoundsSender sender = new RoundsSender(task.rounds);
        Thread senderThread = new Thread(sender);
        senderThread.start();
    }


    private void onMessagingNodeList(MessagingNodesList list) {
        Logger.log("received register messaging list with " + list.count + " connections");

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.CONNECT);
        BufUtils.putString(buf, myIpAddress);
        buf.putInt(myPort);
        byte[] data = BufUtils.getBytesFromWritingBuf(buf);

        synchronized (this) {
            for (NodeInfo connection : list.connections) {
                try {
                    Socket socket;
                    if (myIpAddress.equals(connection.ipAddr)) {
                        Logger.log("connecting to: localhost:" + connection.port);
                        socket = new Socket("localhost", connection.port);

                    } else {
                        Logger.log("connecting to: " + connection.ipAddr + ":" + connection.port);
                        socket = new Socket(connection.ipAddr, connection.port);
                    }
                    SocketContainer container = new SocketContainer(socket);

                    socketContainerMap.put(connection, container);
                    container.sendData(data);
                    allNodes.put(connection.getId(), connection);
                } catch(IOException e){
                    e.printStackTrace();
                }
            }
        }
    }

    private synchronized void onLinkWeights(LinkWeights weights){
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

        routingCache = new RoutingCache(allNodes, myIpAddress + myPort);
    }

    private synchronized void printShortestPath(){
        Logger.log("printing shortest path");

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
                System.out.println("Please provide host and port number of registry");
                return;
            }

            Logger.logLevel = 1;

            String ip = args[0];
            int port = Integer.parseInt(args[1]);
            MessagingNode msgNode = new MessagingNode(ip, port);

            msgNode.sendRegistrationRequest();
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                String instruction = br.readLine();

                if(instruction.equals("exit-overlay")) {
                    msgNode.sendDeregisterRequest();

                } else if(instruction.equals("print-shortest-path")){
                    msgNode.printShortestPath();
                }

            }

        } catch(ConnectException e){
            System.out.println("Error connecting to registry");
            System.exit(1);

        } catch(Exception e) {
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

                NodeInfo toSend = route.get(1);
                SocketContainer socket = socketContainerMap.get(toSend);

                sendMessage(route.subList(2, route.size()), payload,  socket);
            }

            sendTaskComplete();
        }
    }

}
