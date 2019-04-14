package cs455.hadoop;

import cs455.hadoop.io.Artist;
import cs455.hadoop.io.ArtistTerm;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Q8Reducer extends Reducer<Text, Artist, Text, Text> {

    Artist mostGeneric = new Artist();
    double maxSum = Double.MIN_VALUE;

    Artist mostUnique = new Artist();
    double minSum = Double.MAX_VALUE;

    @Override
    protected void reduce(Text key, Iterable<Artist> values, Context context) throws IOException, InterruptedException {

        Iterator<Artist> it = values.iterator();
        // just to be safe
        if(!it.hasNext())
            return;

        Artist artist = it.next();
        ArtistTerm[] terms = artist.terms.toArray();

        double weightedFreqSum = 0;
        for(ArtistTerm term : terms) {
            weightedFreqSum += term.freq * term.weight;
        }

        if(weightedFreqSum > maxSum) {
            mostGeneric = artist;
            maxSum = weightedFreqSum;
        }

        if(weightedFreqSum < minSum) {
            mostUnique = artist;
            minSum = weightedFreqSum;
        }
//
//        context.write(new Text("Q8:"),
//                new Text(artist.artistName.toString()+String.format("%.2f",weightedFreqSum)));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Q8 Most Generic:"), new Text(mostGeneric.artistName.toString()+String.format("%.2f", maxSum)));
        context.write(new Text("Q8 Most Unique:"), new Text(mostUnique.artistName.toString()+String.format("%.2f", minSum)));
    }

}
