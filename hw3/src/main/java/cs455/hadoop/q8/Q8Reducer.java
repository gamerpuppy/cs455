package cs455.hadoop.q8;

import cs455.hadoop.io.Artist;
import cs455.hadoop.io.ArtistTerm;
import cs455.hadoop.util.TopList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

public class Q8Reducer extends Reducer<Text, Artist, Text, Text> {

    TopList<ArtistWeight> maxList = new TopList<>(10, Comparator.comparingDouble(o -> o.weightedSum));
    TopList<ArtistWeight> minList = new TopList<>(10, Comparator.comparingDouble(o -> 0-o.weightedSum));

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
        ArtistWeight artistWeight = new ArtistWeight(artist.artistName.toString(), weightedFreqSum);
        maxList.addIfTop(artistWeight);
        minList.addIfTop(artistWeight);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("Question 8: Most generic"), new Text("\n"+maxList.toString()));
        context.write(new Text("Question 8: Most unique"), new Text("\n"+minList.toString()));
    }

    private static class ArtistWeight {
        String name;
        double weightedSum;

        public ArtistWeight(String name, double weightedSum) {
            this.name = name;
            this.weightedSum = weightedSum;
        }

        @Override
        public String toString() {
            return String.format("%s %.2f", name, weightedSum);
        }
    }


}
