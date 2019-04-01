package cs455.hadoop.wireformats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritableComparable implements WritableComparable<CustomWritableComparable> {

    public static final int METADATAQ1KEY = 1;
    public static final int OUTQ1KEY = 66;

    private IntWritable id = new IntWritable();
    private WritableComparable inner = null;

    public CustomWritableComparable() { }

    public CustomWritableComparable(int id, WritableComparable inner) {
        this.id.set(id);
        this.inner = inner;
    }

    public int getId() {
        return id.get();
    }

    public WritableComparable getInner() {
        return inner;
    }

    public void setId(int id) {
        this.id.set(id);
    }

    public void setInner(WritableComparable inner) {
        this.inner = inner;
    }

    @Override
    public int compareTo(CustomWritableComparable o) {
        int cmp = Integer.compare(id.get(), o.id.get());

        if(cmp == 0)
            return inner.compareTo(o.inner);
        else
            return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof CustomWritableComparable))
            return false;

        CustomWritableComparable cw = (CustomWritableComparable) o;
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
        inner = getInnerInstance(id.get());
        inner.readFields(in);
    }

    @Override
    public String toString() {
        return id.get()+":"+inner.toString();
    }


    private static WritableComparable getInnerInstance(int id) {
        switch(id) {
            case METADATAQ1KEY:
            case OUTQ1KEY:
                return new Text();

            default: return new IntWritable(-1);
        }
    }

}
