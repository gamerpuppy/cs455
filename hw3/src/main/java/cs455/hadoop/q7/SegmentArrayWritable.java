package cs455.hadoop.q7;

import org.apache.hadoop.io.ArrayWritable;

public class SegmentArrayWritable extends ArrayWritable {

    public SegmentArrayWritable() {
        super(Segment.class);
    }

    public SegmentArrayWritable(Segment[] arr) {
        super(Segment.class, arr);
    }

    @Override
    public Segment[] toArray() {
        return (Segment[])super.toArray();
    }

}
