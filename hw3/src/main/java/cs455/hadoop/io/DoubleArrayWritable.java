package cs455.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }

    public DoubleArrayWritable(DoubleWritable[] arr) {
        super(DoubleWritable.class, arr);
    }

    @Override
    public DoubleWritable[] toArray() {
        return (DoubleWritable[])super.toArray();
    }

}
