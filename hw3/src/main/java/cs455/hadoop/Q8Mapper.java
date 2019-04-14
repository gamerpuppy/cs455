package cs455.hadoop;

import cs455.hadoop.io.Artist;
import cs455.hadoop.util.CsvTokenizer;
import cs455.hadoop.io.ArtistTerm;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class Q8Mapper extends Mapper<LongWritable, Text, Text, Artist> {

    HashSet<String> writtenArtists = new HashSet<>();

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
//         Should exclude header lines
        if(byteOffset.get() == 0)
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());
        String artistId = csv.getTokAt(3);

        if (writtenArtists.contains(artistId))
            return;
        else
           writtenArtists.add(artistId);

        String[] terms = csv.getTokAt(11).split(" ");
        ArtistTerm[] ArtistTerms = new ArtistTerm[terms.length];
        setArtistTerms(ArtistTerms, terms, new TermSetter());
        setArtistTerms(ArtistTerms, csv.getTokAt(12).split(" "), new FreqSetter());
        setArtistTerms(ArtistTerms, csv.getTokAt(13).split(" "), new WeightSetter());

        Artist out = new Artist();
        out.terms.set(ArtistTerms);
        out.artistName.set(csv.getTokAt(7));

        context.write(new Text(artistId), out);
    }

    private void setArtistTerms(ArtistTerm[] ArtistTerms, String[] values, ArtistTermSetter setter) {

        for(int i = 0; i < ArtistTerms.length; i++)
        {
            if(ArtistTerms[i] == null)
                ArtistTerms[i] = new ArtistTerm();

            if(i < values.length)
                setter.setTerm(ArtistTerms[i], values[i]);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("./testfiles/metadata1.csv"));

        Q8Mapper mapper = new Q8Mapper();
        String line;
        int i = 0;
        while((line = reader.readLine()) != null) {
            mapper.map(new LongWritable(i++), new Text(line), null);
        }

        System.out.println(mapper);
    }

}

interface ArtistTermSetter {
    void setTerm(ArtistTerm artistTerm, String token);
}

class TermSetter implements ArtistTermSetter {
    @Override
    public void setTerm(ArtistTerm artistTerm, String token) {
        artistTerm.term.set(token);
    }
}

class FreqSetter implements ArtistTermSetter {
    @Override
    public void setTerm(ArtistTerm artistTerm, String token) {
        try {
            artistTerm.freq = Double.parseDouble(token);
        } catch (Exception e) {}
    }
}

class WeightSetter implements ArtistTermSetter {
    @Override
    public void setTerm(ArtistTerm artistTerm, String token) {
        try {
            artistTerm.weight = Double.parseDouble(token);
        } catch (Exception e) {}
    }
}