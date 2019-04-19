package cs455.hadoop.q10;

import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.DoubleArrayWritable;
import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q10MetadataMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

    static final String[] arrayValues = {
            "hotttnesss",
            "latitude",
            "longitude",
            "year"
    };

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
//         Should exclude header lines
        if(byteOffset.get() == 0)
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());

        DoubleWritable[] dwArray = {
                new DoubleWritable(csv.getTokAsDouble(2)),
                new DoubleWritable(csv.getTokAsDouble(4)),
                new DoubleWritable(csv.getTokAsDouble(5)),
                new DoubleWritable(csv.getTokAsDouble(14))
        };

        Text songId = new Text(csv.getTokAt(8));
        CustomWritable valueOut = new CustomWritable()
                .setId(CustomWritable.Q10_METADATA)
                .setInner(new DoubleArrayWritable(dwArray));

        context.write(songId, valueOut);
    }

}
