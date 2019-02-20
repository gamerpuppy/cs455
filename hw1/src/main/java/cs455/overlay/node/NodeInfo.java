package cs455.overlay.node;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

public class NodeInfo implements Comparable<NodeInfo> {

    public String ipAddr;
    int port;
    public Map<NodeInfo, Integer> links;
    SocketChannel channel = null;

    public NodeInfo(String ipAddr, int port){
        this.ipAddr = ipAddr;
        this.port = port;
        links = new HashMap<>();
    }

    @Override
    public String toString(){
        return ipAddr+":"+port;
    }

    public String getId(){
        return ipAddr+port;
    }

    @Override
    public int compareTo(NodeInfo o) {
        return this.getId().compareTo(o.getId());
    }

}