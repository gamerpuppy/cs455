package cs455.hadoop.q8;

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

        Artist out = new Artist();
        out.terms.set(ArtistTerm.getTerms(csv));
        out.artistName.set(csv.getTokAt(7));

        context.write(new Text(artistId), out);
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