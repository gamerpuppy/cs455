package cs455.overlay.dijkstra;

import cs455.overlay.node.Node;
import cs455.overlay.node.NodeInfo;
import cs455.overlay.util.Logger;

import java.util.*;

public class RoutingCache {

    Map<String, List<NodeInfo>> routes;
    static boolean printPort = true;

    public RoutingCache(Map<String, NodeInfo> nodeInfoMap, String sourceId){
        PriorityQueue<NodeWrapper> queue = new PriorityQueue<>();
        Map<NodeInfo, NodeInfo> prev = new HashMap<>();
        Map<NodeInfo, NodeWrapper> wrapperMap = new HashMap<>();

        for(String key : nodeInfoMap.keySet()){
            NodeInfo node = nodeInfoMap.get(key);
            NodeWrapper wrap;
            if(node.getId().equals(sourceId)){
                wrap = new NodeWrapper(node, 0);
            } else {
                wrap = new NodeWrapper(node, Integer.MAX_VALUE);
            }
            wrapperMap.put(node, wrap);
            queue.add(wrap);
        }

        while(!queue.isEmpty()){
            NodeWrapper wrapper = queue.poll();

            for(NodeInfo node : wrapper.node.links.keySet()){
                int altDist = wrapper.dist + wrapper.node.links.get(node);
                NodeWrapper wrap2 = wrapperMap.get(node);
                if(altDist < wrap2.dist){
                    queue.remove(wrap2);
                    wrap2.dist = altDist;
                    prev.put(node, wrapper.node);
                    queue.add(wrap2);
                }
            }
        }


        routes = new HashMap<>();

        for(String key : nodeInfoMap.keySet()){

            List<NodeInfo> route = new ArrayList<>();
            NodeInfo node = nodeInfoMap.get(key);
            route.add(node);

            while(prev.containsKey(node)){
                node = prev.get(node);
                route.add(0, node);
            }
            routes.put(key, route);
        }

    }

    public List<NodeInfo> getRouteTo(String id){
        return routes.get(id);
    }

    public String getRouteString(String id) {

        List<NodeInfo> route = getRouteTo(id);
        if(route.size() < 2)
            return "";

        StringBuilder sb = new StringBuilder();
        NodeInfo last = route.get(0);
        sb.append(last.ipAddr);
        if(printPort){
            sb.append(":"+last.port);
        }


        for(int i = 1; i < route.size(); i++)
        {
            NodeInfo next = route.get(i);
            int weight = last.links.get(next);

            sb.append("--"+weight+"--"+next.ipAddr);
            if(printPort){
                sb.append(":"+next.port);
            }
            last = next;
        }

        return sb.toString();
    }

    class NodeWrapper implements Comparable<NodeWrapper>{
        NodeInfo node;
        int dist;

        public NodeWrapper(NodeInfo node, int weight){
            this.node = node;
            this.dist = weight;
        }

        @Override
        public int compareTo(NodeWrapper o) {
            int res =  Integer.compare(this.dist, o.dist);

            if(res != 0)
                return res;

            return Integer.compare(this.hashCode(),o.hashCode());
        }
    }

}
