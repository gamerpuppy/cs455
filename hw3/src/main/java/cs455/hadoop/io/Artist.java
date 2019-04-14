package cs455.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Artist implements Writable {

    public Text artistName = new Text();
    public ArtistTermArrayWritable terms = new ArtistTermArrayWritable();

    @Override
    public void write(DataOutput out) throws IOException {
        artistName.write(out);
        terms.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        artistName.readFields(in);
        terms.readFields(in);
    }

}
