package cs455.overlay.wireformats;

import cs455.overlay.node.NodeInfo;
import cs455.overlay.util.BufUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LinkWeights implements Event {


    public List<Link> links;
    public int count;

    public LinkWeights(ByteBuffer buf){
        count = buf.getInt();
        links = new ArrayList<>();

        for(int i = 0; i < count; i++){

            Link link = new Link();
            link.ip1 = BufUtils.getString(buf);
            link.port1 = buf.getInt();
            link.ip2 = BufUtils.getString(buf);
            link.port2 = buf.getInt();
            link.weight = buf.getInt();
            links.add(link);
        }
    }

    public class Link {
        public String ip1;
        public int port1;
        public String ip2;
        public int port2;
        public int weight;
    }

    @Override
    public int getCode() {
        return 5;
    }
}
