package cs455.hadoop;

import cs455.hadoop.io.MetadataValue1;
import cs455.hadoop.util.CsvTokenizer;
import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Part1MetadataMapper extends Mapper<LongWritable, Text, CustomWritableComparable, CustomWritable> {

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
        //         Should exclude header lines
        if(byteOffset.get() == 0)
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());
        String songId = csv.getTokAt(8);

        CustomWritableComparable outKey = new CustomWritableComparable()
                .setId(CustomWritableComparable.SONG_ID_KEY)
                .setInner(new Text(songId));

        MetadataValue1 metadataValue1 = new MetadataValue1()
                .setArtistId(csv.getTokAt(3))
                .setArtistName(csv.getTokAt(7))
                .setTitle(csv.getTokAt(9));

        CustomWritable outValue = new CustomWritable()
                .setId(CustomWritable.METADATA_VALUE_1)
                .setInner(metadataValue1);

        context.write(outKey, outValue);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("./testfiles/metadata1.csv"));
        Part1MetadataMapper mapper = new Part1MetadataMapper();

        String line;
        while((line = reader.readLine()) != null) {
            mapper.map(new LongWritable(0), new Text(line), null);
        }

    }

}
