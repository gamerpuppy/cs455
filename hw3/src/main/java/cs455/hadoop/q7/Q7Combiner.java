package cs455.hadoop.q7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class Q7Combiner extends Reducer<IntWritable, SegmentArrayWritable, IntWritable, SegmentArrayWritable>  {

    ArrayList<Segment> segmentList = new ArrayList<>();

    @Override
    protected void reduce(IntWritable key, Iterable<SegmentArrayWritable> values, Context context) {
        for(SegmentArrayWritable segArray : values) {
            combineSegments(segmentList, segArray.toArray());
        }
    }

    public static void combineSegments(ArrayList<Segment> segmentList, Segment[] segments) {
        int segmentListSize = segmentList.size();
        for(int i = 0; i < segments.length; i++) {
            if(i < segmentListSize) {
                segmentList.get(i).add(segments[i]);
            } else {
                segmentList.add(segments[i]);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(segmentList.isEmpty())
            return;

        IntWritable key = new IntWritable(new Random().nextInt());

        Segment[] segArr = new Segment[segmentList.size()];
        segmentList.toArray(segArr);
        SegmentArrayWritable value = new SegmentArrayWritable(segArr);
        context.write(key, value);
    }

}
