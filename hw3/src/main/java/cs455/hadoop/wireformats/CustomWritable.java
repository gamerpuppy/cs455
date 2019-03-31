package cs455.hadoop.wireformats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritable implements WritableComparable<CustomWritable> {

    static final int INT = 0;
    static final int TEXT = 1;
    static final int METADATAQ1KEY = 65;
    static final int METADATAQ3 = 66;

    private IntWritable id = new IntWritable();
    private WritableComparable inner = null;

    public int getId() {
        return id.get();
    }

    public CustomWritable() { }

    public CustomWritable(int id, WritableComparable inner) {
        this.id.set(id);
        this.inner = inner;
    }

    @Override
    public int compareTo(CustomWritable o) {
        int cmp = Integer.compare(id.get(), o.id.get());

        if(cmp == 0)
            return inner.compareTo(o.inner);
        else
            return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof CustomWritable))
            return false;

        CustomWritable cw = (CustomWritable) o;
        if(id != cw.id)
            return false;
        if(inner == null && cw.inner == null)
            return true;
        if(inner == null || cw.inner == null)
            return false;
        return inner.equals(cw.inner);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        inner.write(out);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + id.hashCode();
        result = 31 * result + (inner == null ? 0 : inner.hashCode());
        return result;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        switch(id.get()) {
            case METADATAQ1:
                inner = new MetadataQ1Key();
                break;

            case TEXT:
                inner = new Text();
            readFields(in);

        }

        inner = new MetadataQ1Key();

        inner.readFields(in);
    }

    private static WritableComparable getInnerInstance(int id) {
        switch(id) {
            case METADATAQ1: return new MetadataQ1Key();
            case TEXT: return new Text();


            default: return new IntWritable(-1);
        }
    }

}
