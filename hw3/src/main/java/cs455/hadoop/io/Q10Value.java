package cs455.hadoop.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Q10Value implements Writable {

    public int year = 0;
    public double hotttnesss = 0;
    public double duration = 0;
    public double compressability = 0;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeDouble(hotttnesss);
        out.writeDouble(duration);
        out.writeDouble(compressability);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readInt();
        hotttnesss = in.readDouble();
        duration = in.readDouble();
        compressability = in.readDouble();
    }

}
