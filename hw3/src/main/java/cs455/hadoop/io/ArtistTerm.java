package cs455.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArtistTerm implements Writable {

    public Text term = new Text();
    public double freq = 0;
    public double weight = 0;

    @Override
    public void write(DataOutput out) throws IOException {
        term.write(out);
        out.writeDouble(freq);
        out.writeDouble(weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term.readFields(in);
        freq = in.readDouble();
        weight = in.readDouble();
    }
}
