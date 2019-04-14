package cs455.hadoop;

import cs455.hadoop.io.Segment;
import cs455.hadoop.io.SegmentArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class Q7Reducer extends Reducer<IntWritable, SegmentArrayWritable, Text, Text> {

    ArrayList<Segment> segmentList = new ArrayList<>();

    @Override
    protected void reduce(IntWritable key, Iterable<SegmentArrayWritable> values, Context context) {
        for(SegmentArrayWritable segArr : values) {
            Q7Combiner.combineSegments(segmentList, segArr.toArray());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        StringBuilder segmentDataSb = new StringBuilder();
        for (Segment segment: segmentList) {
            segment.average();
            segmentDataSb.append(segment);
            segmentDataSb.append(',');
        }
        context.write(
                new Text( "Q7: Segment Data. segments are seperated by commas and the values start time," +
                        " pitch, timbre, max loudness, max loudness time and start loudness are seperated by spaces."),
                new Text(segmentDataSb.toString()));
    }

}
