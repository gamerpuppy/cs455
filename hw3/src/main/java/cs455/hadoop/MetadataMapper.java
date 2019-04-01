package cs455.hadoop;

import cs455.hadoop.io.MetadataValue1;
import cs455.hadoop.util.CsvTokenizer;
import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MetadataMapper extends Mapper<LongWritable, Text, CustomWritableComparable, CustomWritable> {

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
//         Should exclude header lines
        if(value.charAt(0) != '0')
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

}
