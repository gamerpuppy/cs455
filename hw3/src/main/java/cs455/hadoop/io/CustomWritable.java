package cs455.hadoop.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements Writable {

    public static final int INT = 0;
    public static final int TEXT = 1;
    public static final int DOUBLE = 2;
    public static final int METADATA_VALUE_1 = 50;
    public static final int ANALYSIS_VALUE_1 = 123;
    public static final int DOUBLE_ARRAY = 124;
    public static final int ARTIST_TERM_ARRAY = 125;
    public static final int ARTIST = 126;

    private IntWritable id = new IntWritable();
    private Writable inner = null;

    public CustomWritable() { }

    public CustomWritable(int id, Writable inner) {
        this.id.set(id);
        this.inner = inner;
    }

    public int getId() {
        return id.get();
    }

    public Writable getInner() {
        return inner;
    }

    public CustomWritable setId(int id) {
        this.id.set(id);
        return this;
    }

    public CustomWritable setInner(Writable inner) {
        this.inner = inner;
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        inner.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        inner = getInnerInstance(id.get());
        inner.readFields(in);
    }

    @Override
    public String toString() {
        return id.get()+":"+inner.toString();
    }

    private static Writable getInnerInstance(int id) {
        switch(id)
        {
            case TEXT: return new Text();
            case METADATA_VALUE_1: return new MetadataValue1();
            case ANALYSIS_VALUE_1: return new AnalysisValue1();
            case DOUBLE_ARRAY: return new DoubleArrayWritable();
            case ARTIST_TERM_ARRAY: return new ArtistTermArrayWritable();
            case ARTIST: return new Artist();

            default: return new IntWritable(-1);
        }
    }

}
