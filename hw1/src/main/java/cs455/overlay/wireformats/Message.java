package cs455.overlay.wireformats;

import cs455.overlay.node.NodeInfo;
import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Message implements Event{

    public final int payload;
    public final int hopsIncluded;
    public List<NodeInfo> routingPlan;

    public Message(ByteBuffer buf){
        routingPlan = new ArrayList<>();
        payload = buf.getInt();
        hopsIncluded = buf.getInt();

        for(int i = 0; i < hopsIncluded; i++){
            String ip = BufUtils.getString(buf);
            int port = buf.getInt();

            NodeInfo node = new NodeInfo(ip, port);
            routingPlan.add(node);
        }
    }

    @Override
    public int getCode() {
        return 10;
    }
}
