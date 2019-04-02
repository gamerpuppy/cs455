package cs455.hadoop;

import cs455.hadoop.io.AnalysisValue1;
import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnalysisMapper extends Mapper<LongWritable, Text, CustomWritableComparable, CustomWritable> {

    boolean haveWrote = false;

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
        // Should exclude header lines
//        if(value.charAt(0) != '0')
//            return;
        CsvTokenizer csv = new CsvTokenizer(value.toString());
        String songId = csv.getTokAt(1);

        CustomWritableComparable outKey = new CustomWritableComparable()
                .setId(CustomWritableComparable.SONG_ID_KEY)
                .setInner(new Text(songId));

        AnalysisValue1 analysisValue = new AnalysisValue1()
                .setHotttnesss(csv.getTokAsDouble(2))
                .setDanceability(csv.getTokAsDouble(4))
                .setDuration(csv.getTokAsDouble(5))
                .setEndFadeIn(csv.getTokAsDouble(6))
                .setEnergy(csv.getTokAsDouble(7))
                .setLoudness(csv.getTokAsDouble(10));

        CustomWritable outValue = new CustomWritable()
                .setId(CustomWritable.ANALYSIS_VALUE_1)
                .setInner(analysisValue);

        context.write(outKey, outValue);


    }

}
