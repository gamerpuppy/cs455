package cs455.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class ArtistTermArrayWritable extends ArrayWritable {

    public ArtistTermArrayWritable() {
        super(ArtistTerm.class);
    }

    public ArtistTermArrayWritable(ArtistTerm[] arr) {
        super(ArtistTerm.class, arr);
    }

    @Override
    public ArtistTerm[] toArray() {
        return (ArtistTerm[])super.toArray();
    }

}
