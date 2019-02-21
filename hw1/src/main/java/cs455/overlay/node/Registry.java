package cs455.overlay.node;

import cs455.overlay.transport.SocketContainer;
import cs455.overlay.transport.TCPServerThread;
import cs455.overlay.util.BufUtils;
import cs455.overlay.util.Logger;
import cs455.overlay.wireformats.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;

public class Registry extends Node {

    List<SocketContainer> nodes = new ArrayList<>();

    Map<SocketContainer, NodeInfo> nodeInfoMap = new HashMap<>();
    Map<SocketContainer, Boolean> taskCompleteMap = new HashMap<>();
    Map<SocketContainer, TrafficSummaryResponse> trafficSummaryMap = new HashMap<>();

    private Registry(int portToBind) throws IOException{
        Node.theInstance = this;
        this.myIpAddress = InetAddress.getLocalHost().getCanonicalHostName();
        TCPServerThread serverThread = new TCPServerThread(portToBind);
        this.myPort = serverThread.server.getLocalPort();

        Thread thread = new Thread(serverThread);
        thread.start();
    }

    public void onEvent(Event event, SocketContainer socket) {
        switch (event.getCode()) {
            case EventFactory.REGISTER_REQUEST:
                this.onRegisterRequest((RegisterRequest) event, socket);
                break;
//            case EventFactory.DEREGISTER_REQUEST:
//                this.onDeregisterRequest((DeregisterRequest) event, socket);
//                break;
//            case EventFactory.TASK_COMPLETE:
//                this.onTaskComplete((TaskComplete) event, socket);
//                break;
//            case EventFactory.TRAFFIC_SUMMARY_RESPONSE:
//                this.onTrafficSummaryResponse((TrafficSummaryResponse) event, socket);
//                break;
            default:
                System.out.println("Event received: type not recognized");
        }
    }

//    private boolean areTrafficSummariesComplete(){
//        for(SocketChannel channel : nodes){
//            if(!trafficSummaryMap.containsKey(channel))
//                return false;
//        }
//        return true;
//    }
//
//    private void printTrafficSummaries(){
//        Logger.log("printing traffic summaries");
//
//        System.out.println("Node\t\t\t\t\tSentCount\t\t\tReceivedCount\t\t\tSentSum\t\t\tReceiveSum\t\t\tRelayCount");
//        for(SocketChannel socketChannel : nodes){
//            TrafficSummaryResponse resp = trafficSummaryMap.get(socketChannel);
//            System.out.println(resp.toString());
//        }
//
//        trafficSummaryMap.clear();
//    }
//
//    private void onTrafficSummaryResponse(TrafficSummaryResponse resp, SocketChannel channel){
//        Logger.log("received traffic summary from  "+resp.ip+":"+resp.port);
//        trafficSummaryMap.put(channel, resp);
//
//        if(areTrafficSummariesComplete()){
//            printTrafficSummaries();
//        }
//    }

    private void onRegisterRequest(RegisterRequest req, SocketContainer socket) {
        Logger.log("received register request from: ip:"+req.ipAddress+" myPort:"+req.port);

        boolean ipMatches = socket.matchesIp(req.ipAddress);
        if(ipMatches){
            synchronized (this){
                this.nodes.add(socket);

                NodeInfo node = new NodeInfo(req.ipAddress, req.port);
                this.nodeInfoMap.put(socket, node);
            }
        }

        Logger.log("sending register response with status:"+ipMatches);

        ByteBuffer buf = ByteBuffer.allocate(256);
        buf.putInt(EventFactory.REGISTER_RESPONSE);
        buf.put((byte)(ipMatches ? 1 : 0));
        BufUtils.putString(buf, "Registration request "+(ipMatches ? "":"un")+"successful, The number of messaging" +
                "nodes is currently ("+ nodes.size()+")");

        socket.sendData(BufUtils.getBytesFromWritingBuf(buf));
    }

//
//    private boolean areTasksComplete(){
//        for(SocketChannel channel : nodes){
//            if(!taskCompleteMap.containsKey(channel)
//            || !taskCompleteMap.get(channel))
//                return false;
//        }
//        return true;
//    }
//
//    private void sendTrafficRequests(){
//
//
//        ByteBuffer buf = ByteBuffer.allocate(4);
//        buf.putInt(EventFactory.TRAFFIC_SUMMARY_REQUEST);
//        buf.flip();
//
//        byte[] data = new byte[4];
//        buf.get(data, 0, 4);
//
//        for(SocketChannel channel : nodes){
//            try {
//                ByteBuffer buf2 = ByteBuffer.wrap(data).asReadOnlyBuffer();
//                channel.write(buf2);
//            } catch(IOException e){
//                e.printStackTrace();
//            }
//        }
//    }
//
//    private void onTaskComplete(TaskComplete task, SocketChannel channel){
//        Logger.log("received task complete from "+task.ip+":"+task.port);
//
//        taskCompleteMap.put(channel, true);
//
//        if(areTasksComplete()){
//            int sleepTime = 1000;
//            Logger.log("all tasks complete, sleeping for "+sleepTime);
//            taskCompleteMap.clear();
//            try {
//                Thread.sleep(sleepTime);
//            } catch(InterruptedException e){
//                e.printStackTrace();
//            }
//            sendTrafficRequests();
//        }
//    }
//
//
//

//
//    private void onDeregisterRequest(DeregisterRequest req, SocketChannel socketChannel) {
//        Logger.log("received deregister request from: ip:"+req.ipAddress+" myPort:"+req.port);
//
//        boolean doesMatch = ipMatches(socketChannel, req.ipAddress);
//        boolean isRegistered = this.nodeInfoMap.containsKey(socketChannel);
//
//        Logger.log("deregister request ip "+ (doesMatch ? "does match" : "doesn't match"));
//        Logger.log("deregister request was sent from a "+(isRegistered ? "": "un")+"registered node");
//
//        boolean success = doesMatch && isRegistered;
//
//        if(success){
//            this.nodes.remove(socketChannel);
//            this.nodeInfoMap.remove(socketChannel);
//        }
//
//        ByteBuffer buf = ByteBuffer.allocate(256);
//        buf.putInt(EventFactory.DEREGISTER_RESPONSE);
//        buf.put((byte)(success ? 1 : 0));
//        BufUtils.putString(buf, (success ? "successfully unregistered" : "failed to unregister"));
//        buf.flip();
//
//        try {
//            socketChannel.write(buf);
//        } catch(IOException e){
//            Logger.log("failure to write deregister response");
//        }
//    }
//
//    private synchronized void listMessagingNodes(){
//        for(SocketChannel channel : this.nodes){
//            System.out.println(this.nodeInfoMap.get(channel).toString());
//        }
//    }
//
//    private synchronized void listWeights(){
//        for(SocketChannel channel : this.nodes){
//            NodeInfo node = this.nodeInfoMap.get(channel);
//            for(NodeInfo dest : node.links.keySet()){
//                if(node.getId().compareTo(dest.getId()) < 1)
//                    System.out.println(node.toString() + " " + dest.toString() + " " + node.links.get(dest));
//            }
//        }
//    }
//
//    private synchronized void sendLinkWeights() throws IOException {
//        ByteBuffer buf = ByteBuffer.allocate(32768);
//
//        buf.putInt(EventFactory.LINK_WEIGHTS);
//
//        int degree = 0;
//        if(nodes.size() > 0){
//            NodeInfo node = this.nodeInfoMap.get(this.nodes.get(0));
//            degree = node.links.size();
//        }
//
//        int connectionCount = this.nodes.size() * degree / 2;
//        buf.putInt(connectionCount);
//
//        int writtenCount = 0;
//        for(SocketChannel channel : this.nodes){
//            NodeInfo node = this.nodeInfoMap.get(channel);
//            for(NodeInfo dest : node.links.keySet()){
//                if(node.getId().compareTo(dest.getId()) < 1) {
//                    BufUtils.putString(buf, node.ipAddr);
//                    buf.putInt(node.port);
//                    BufUtils.putString(buf, dest.ipAddr);
//                    buf.putInt(dest.port);
//                    buf.putInt(node.links.get(dest));
//                    writtenCount++;
//                }
//            }
//        }
//        Logger.log("sending "+connectionCount+" connectionCount, wrote "+writtenCount);
//
//        buf.flip();
//
//        byte[] data = new byte[buf.limit()];
//        buf.get(data, 0, buf.limit());
//
//        for(SocketChannel channel : this.nodes){
//            ByteBuffer buf2 = ByteBuffer.wrap(data).asReadOnlyBuffer();
//            channel.write(buf2);
//        }
//    }
//
//    private synchronized void sendMessagingNodeList() throws IOException{
//        for(SocketChannel channel : this.nodes){
//            NodeInfo node = this.nodeInfoMap.get(channel);
//
//            ArrayList<NodeInfo> connections = new ArrayList<>();
//            for(NodeInfo dest : node.links.keySet()){
//                if(node.getId().compareTo(dest.getId()) < 1) {
//                    connections.add(dest);
//                }
//            }
//
//            ByteBuffer buf = ByteBuffer.allocate(32768);
//            buf.putInt(EventFactory.MESSAGING_NODES_LIST);
//            buf.putInt(connections.size());
//            for(NodeInfo connection: connections){
//                BufUtils.putString(buf, connection.ipAddr);
//                buf.putInt(connection.port);
//            }
//            buf.flip();
//
//            channel.write(buf);
//        }
//    }
//
//    private synchronized void setupOverlay(int connections){
//        Logger.log("setting up overlay with "+connections+" connections");
//
//        Random rand = new Random();
//
//        boolean found = false;
//        int iterations = 0;
//        while(!found && iterations < 500) {
//
//            ArrayList<NodeInfo> availableNodes = new ArrayList<>();
//
//            for (int i = 0; i < nodes.size(); i++) {
//                NodeInfo node = this.nodeInfoMap.get(nodes.get(i));
//                int idx = (i + 1) % nodes.size();
//                NodeInfo node2 = this.nodeInfoMap.get(nodes.get(idx));
//                int weight = rand.nextInt(10) + 1;
//
//                if(!node.links.containsKey(node2))
//                    node.links.put(node2, weight);
//
//                if(!node2.links.containsKey(node))
//                    node2.links.put(node, weight);
//
//                availableNodes.add(node);
//            }
//
//            while(availableNodes.size() > 1){
//                int idx1 = rand.nextInt(availableNodes.size());
//                NodeInfo node1 = availableNodes.get(idx1);
//                int idx2 = 0;
//
//                int i = 0;
//                for(; i < 100; i++) {
//                    idx2 = rand.nextInt(availableNodes.size());
//                    NodeInfo node2 = availableNodes.get(idx2);
//
//                    if(node1 == node2)
//                        continue;
//
//                    if (!node1.links.containsKey(node2))
//                        break;
//                }
//                if(i == 100) {
//                    break;
//                }
//
//                NodeInfo node2 = availableNodes.get(idx2);
//
//                if(node1.links.size() >=  connections) {
//                    availableNodes.remove(node1);
//                    continue;
//                }
//
//                if(node2.links.size() >=  connections) {
//                    availableNodes.remove(node2);
//                    continue;
//                }
//
//                int weight = rand.nextInt(10) + 1;
//                node1.links.put(node2, weight);
//                node2.links.put(node1, weight);
//
//
//
//                if(node1.links.size() == connections)
//                    availableNodes.remove(node1);
//
//                if(node2.links.size() == connections)
//                    availableNodes.remove(node2);
//            }
//
//            if(availableNodes.size() == 0)
//                found = true;
//            else {
//                if(availableNodes.get(0).links.size() == connections)
//                    found = true;
//                else
//                    clearOverlay();
//            }
//            iterations++;
//        }
//
//        if(iterations < 500)
//            Logger.log("found a good overlay in " + iterations + " iterations");
//        else
//            Logger.log("could not find a good overlay in " + iterations + " iterations");
//
//    }
//
//    private synchronized void clearOverlay(){
//        for(SocketChannel channel : this.nodes){
//            NodeInfo node = this.nodeInfoMap.get(channel);
//            node.links = new HashMap<>();
//        }
//    }
//
//    private void startRounds(int rounds) throws IOException {
//        ByteBuffer buf = ByteBuffer.allocate(8);
//        buf.putInt(EventFactory.TASK_INITIATE);
//        buf.putInt(rounds);
//
//        byte[] data = buf.array();
//
//        for(SocketChannel channel : this.nodes){
//            ByteBuffer buf2 = ByteBuffer.wrap(data).asReadOnlyBuffer();
//            channel.write(buf2);
//        }
//    }


    public static void main(String[] args){
        try {

            if (args.length < 1) {
                System.out.println("Please provide myPort number");
                return;
            }

            int port = Integer.parseInt(args[0]);
            Registry reg = new Registry(port);

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            while (true) {
                String instruction = br.readLine();

//                if(instruction.equals("list-messaging-nodes")){
//                    reg.listMessagingNodes();
//
//                } else if(instruction.equals("list-weights")){
//                    reg.listWeights();
//
//                } else if(instruction.matches("setup-overlay \\d+")){
//                    int connections = Integer.parseInt(instruction.substring(14));
//                    reg.setupOverlay(connections);
//                    reg.sendMessagingNodeList();
//                    reg.sendLinkWeights();
//
//                } else if(instruction.matches("start \\d+")){
//                    int rounds = Integer.parseInt(instruction.substring(6));
//                    reg.startRounds(rounds);
//
//                } else if(instruction.equals("clear-overlay")){
//                    reg.clearOverlay();
//                }
            }

        } catch(Exception e){
            e.printStackTrace();
        }
    }

}
