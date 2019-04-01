package cs455.hadoop.wireformats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetadataQ1Value implements Writable {

    private Text artistName = new Text();
    private IntWritable songCount = new IntWritable();

    public MetadataQ1Value(){}

    public MetadataQ1Value(String artistName, int count) {
        this.artistName.set(artistName);
        this.songCount.set(count);
    }

    public String getArtistName() {
        return artistName.toString();
    }

    public int getSongCount() {
        return songCount.get();
    }

    public void addToSongCount(int amt) {
        this.songCount.set(this.songCount.get() + amt);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        artistName.write(out);
        songCount.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        artistName.readFields(in);
        songCount.readFields(in);
    }

}
