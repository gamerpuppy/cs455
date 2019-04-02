package cs455.hadoop.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetadataValue1 implements Writable {

    private Text artistId = new Text();
    private Text artistName = new Text();
    private Text title = new Text();

    public MetadataValue1(){}

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

    public MetadataValue1 setArtistId(String artistId) {
        this.artistId.set(artistId);
        return this;
    }

    public String getArtistName() {
        return artistName.toString();
    }

    public MetadataValue1 setArtistName(String artistName) {
        this.artistName.set(artistName);
        return this;
    }

    public String getTitle() {
        return title.toString();
    }

    public MetadataValue1 setTitle(String title) {
        this.title.set(title);
        return this;
    }

    @Override
    public String toString() {
        return artistId+" "+artistName+" "+title;
    }

}
