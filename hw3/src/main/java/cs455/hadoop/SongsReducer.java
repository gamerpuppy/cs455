package cs455.hadoop;

import cs455.hadoop.wireformats.CustomWritable;
import cs455.hadoop.wireformats.CustomWritableComparable;
import cs455.hadoop.wireformats.MetadataQ1Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongsReducer extends Reducer<CustomWritableComparable, CustomWritable, CustomWritableComparable, CustomWritable> {

    // Question 1
    private String maxSongsArtist = "";
    private int maxSongs = 0;


    private String maxHotnessTitle = null;
    private Double maxHotness = null;



    @Override
    protected void reduce(CustomWritableComparable key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {

        switch(key.getId()) {
            // Question 1
            case CustomWritableComparable.METADATAQ1KEY:
                reduceQ1(key, values, context);
                break;

        }

    }

    protected void reduceQ1(CustomWritableComparable key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        int totalSongCount = 0;
        String artist = null;
        for(CustomWritable rawValue : values) {
            MetadataQ1Value value = (MetadataQ1Value) rawValue.getInner();
            artist = value.getArtistName();
            totalSongCount += value.getSongCount();
        }

        if(totalSongCount > maxSongs) {
            maxSongs = totalSongCount;
            maxSongsArtist = artist;
        }
    }

    private void cleanupQ1(Context context) throws IOException, InterruptedException {
        CustomWritableComparable key = new CustomWritableComparable(CustomWritableComparable.OUTQ1KEY, new Text(maxSongsArtist));
        CustomWritable value = new CustomWritable(CustomWritable.INT, new IntWritable(maxSongs));
        context.write(key, value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Question 1
        cleanupQ1(context);

        // Question 3
//        cleanupQ3(context);
    }

}
