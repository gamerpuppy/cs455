package cs455.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

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
