package cs455.hadoop.wireformats;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetadataQ1Key implements WritableComparable<MetadataQ1Key> {

    Text artistId = new Text();
    Text artistName = new Text();

    public MetadataQ1Key(){}

    public MetadataQ1Key(String artistId, String artistName) {
        this.artistId.set(artistId);
        this.artistName.set(artistName);
    }

    @Override
    public int compareTo(MetadataQ1Key o) {
        int cmp = artistId.compareTo(o.artistId);
        if(cmp != 0)
            return cmp;

        return artistName.compareTo(o.artistName);
    }

    public int hashCode() {
        return artistId.hashCode();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        artistId.write(out);
        artistName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        artistId.readFields(in);
        artistName.readFields(in);
    }

}
