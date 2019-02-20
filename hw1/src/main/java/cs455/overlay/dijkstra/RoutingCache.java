package cs455.overlay.dijkstra;

import cs455.overlay.node.NodeInfo;
import cs455.overlay.util.Logger;

import java.util.*;

public class RoutingCache {

    Map<String, List<NodeInfo>> routes;

    public RoutingCache(Map<String, NodeInfo> nodeInfoMap, String sourceId){
        PriorityQueue<NodeWrapper> queue = new PriorityQueue<>();

        Map<NodeInfo, NodeInfo> prev = new HashMap<>();

        for(String key : nodeInfoMap.keySet()){
            NodeInfo node = nodeInfoMap.get(key);
            if(node.getId() == sourceId){
                queue.add(new NodeWrapper(node, 0));
            } else {
                queue.add(new NodeWrapper(node, Integer.MAX_VALUE));
            }
        }

        while(!queue.isEmpty()){
            NodeWrapper wrapper = queue.poll();

            for(NodeInfo node : wrapper.node.links.keySet()){
                int altDist = wrapper.dist + wrapper.node.links.get(node);

                if(altDist < wrapper.dist){
                    wrapper.dist = altDist;
                    prev.put(node, wrapper.node);
                    queue.add(wrapper);
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

        for(int i = 1; i < route.size(); i++)
        {
            NodeInfo next = route.get(i);
            int weight = last.links.get(next);

            sb.append("--"+weight+"--"+next.ipAddr);
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
            return Integer.compare(this.dist, o.dist);
        }
    }

}
