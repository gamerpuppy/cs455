package cs455.hadoop.util;

import java.util.ArrayList;
import java.util.Comparator;

// do not use topCount of 0
public class TopList<T> {
    public ArrayList<T> topList = new ArrayList<>();
    public final int topCount;
    public final Comparator<T> cmp;

    public TopList(int topCount, Comparator<T> cmp) {
        this.topCount = topCount;
        this.cmp = cmp;
    }

    public void addIfTop(T obj) {
        int i = topList.size();

        for(; i > 0; i--) {
            if(cmp.compare(obj, topList.get(i-1)) < 0) {
                break;
            }
        }

        if(i < topList.size() || i == 0) {
            if (topList.size() >= topCount)
                topList.remove(topList.size()-1);

            topList.add(i, obj);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < topList.size(); i++) {
            sb.append((i+1)+": "+topList.get(i)+"\n");
        }
        return sb.toString();
    }
}
