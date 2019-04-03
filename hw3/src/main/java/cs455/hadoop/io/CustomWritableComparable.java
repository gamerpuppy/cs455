package cs455.hadoop.io;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritableComparable implements WritableComparable<CustomWritableComparable> {

    public static final int SONG_ID_KEY = 513;

    public static final int MOSTSONGS_OUT_KEY = 1;
    public static final int LOUDEST_OUT_KEY = 2;
    public static final int HOTTTNESSS_OUT_KEY = 3;
    public static final int MOSTFADE_OUT_KEY = 4;
    public static final int SHORTEST_OUT_KEY = 51;
    public static final int LONGEST_OUT_KEY = 52;
    public static final int ENERGY_OUT_KEY = 61;
    public static final int DANCY_OUT_KEY = 62;

    public static final int ERROR_LINE_KEY = 66;
    public static final int DEBUG_KEY = -1;

    private IntWritable id = new IntWritable();
    private Text inner = new Text();

    public CustomWritableComparable() { }

    public CustomWritableComparable(int id, Text inner) {
        this.id.set(id);
        this.inner = inner;
    }

    public int getId() {
        return id.get();
    }

    public Text getInner() {
        return inner;
    }

    public CustomWritableComparable setId(int id) {
        this.id.set(id);
        return this;
    }

    public CustomWritableComparable setInner(Text inner) {
        this.inner = inner;
        return this;
    }

    @Override
    public int compareTo(CustomWritableComparable o) {
        int cmp = Integer.compare(id.get(), o.id.get());
        if(cmp == 0) {
            return inner.compareTo(o.inner);
        } else
            return cmp;
    }

    @Override
    public boolean equals(Object o) {
        if(!(o instanceof CustomWritableComparable))
            return false;

        CustomWritableComparable cw = (CustomWritableComparable) o;
        if(id != cw.id)
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
        result = 31 * result + inner.hashCode();
        return result;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        inner.readFields(in);
    }

    @Override
    public String toString() {
        return getMsg(id.get())+":"+inner.toString();
    }

    private String getMsg(int id) {
        switch(id) {
            case MOSTSONGS_OUT_KEY: return "Q1: Artist with most songs";
            case LOUDEST_OUT_KEY: return "Q2: Artist with loudest avg";
            case HOTTTNESSS_OUT_KEY: return "Q3: Hotttessst song";
            case MOSTFADE_OUT_KEY: return "Q4: Artist with most fade time";
            case SHORTEST_OUT_KEY: return "Q5: Shortest song";
            case LONGEST_OUT_KEY: return "Q5: Longest song";
            case ENERGY_OUT_KEY: return "Q6: Top 10 songs with most energy";
            case DANCY_OUT_KEY: return "Q6: Top 10 songs with most danceability";
            default: return "key:" + id;
        }
    }

}
