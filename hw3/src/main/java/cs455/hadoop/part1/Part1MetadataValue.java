package cs455.hadoop.part1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Part1MetadataValue implements Writable {

    private Text artistId = new Text();
    private Text artistName = new Text();
    private Text title = new Text();

    public Part1MetadataValue(){}

    @Override
    public void write(DataOutput out) throws IOException {
        artistId.write(out);
        artistName.write(out);
        title.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        artistId.readFields(in);
        artistName.readFields(in);
        title.readFields(in);
    }

    public String getArtistId() {
        return artistId.toString();
    }

    public Part1MetadataValue setArtistId(String artistId) {
        this.artistId.set(artistId);
        return this;
    }

    public String getArtistName() {
        return artistName.toString();
    }

    public Part1MetadataValue setArtistName(String artistName) {
        this.artistName.set(artistName);
        return this;
    }

    public String getTitle() {
        return title.toString();
    }

    public Part1MetadataValue setTitle(String title) {
        this.title.set(title);
        return this;
    }

    @Override
    public String toString() {
        return artistId+" "+artistName+" "+title;
    }

}
