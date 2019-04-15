package cs455.hadoop;

import cs455.hadoop.io.Artist;
import cs455.hadoop.io.ArtistTerm;
import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class Q9MetadataMapper extends Mapper<LongWritable, Text, CustomWritableComparable, CustomWritable> {

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

        Artist artist = new Artist();
        artist.songTitle.set(csv.getTokAt(9));
        artist.artistName.set(csv.getTokAt(7));
        artist.terms.set(ArtistTerm.getTerms(csv));

        CustomWritableComparable keyOut = new CustomWritableComparable()
                .setId(CustomWritableComparable.Q9)
                .setInner(new Text(csv.getTokAt(8)));

        CustomWritable valueOut = new CustomWritable()
                .setId(CustomWritable.ARTIST)
                .setInner(artist);

        context.write(keyOut, valueOut);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("./testfiles/metadata1.csv"));

        Q9MetadataMapper mapper = new Q9MetadataMapper();
        String line;
        int i = 0;
        while((line = reader.readLine()) != null) {
            mapper.map(new LongWritable(i++), new Text(line), null);
        }

        System.out.println(mapper);
    }

}