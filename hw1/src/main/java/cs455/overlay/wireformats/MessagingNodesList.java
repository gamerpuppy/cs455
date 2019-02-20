package cs455.overlay.wireformats;

import cs455.overlay.node.NodeInfo;
import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class MessagingNodesList implements Event {


    public final int count;
    public final List<NodeInfo> connections;

    public MessagingNodesList(ByteBuffer buf){
        connections = new ArrayList<>();
        count = buf.getInt();

        for(int i = 0; i < count; i++){
            String hostname = BufUtils.getString(buf);
            int port = buf.getInt();

            NodeInfo node = new NodeInfo(hostname, port);
            connections.add(node);
        }
    }

    @Override
    public int getCode() {
        return 4;
    }
}
