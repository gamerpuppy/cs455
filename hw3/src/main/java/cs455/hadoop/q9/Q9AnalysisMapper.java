package cs455.hadoop.q9;

import cs455.hadoop.io.*;
import cs455.hadoop.util.CsvTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Q9AnalysisMapper extends Mapper<LongWritable, Text, CustomWritableComparable, CustomWritable> {

    static final String[] arrayValues = {
            "hottttnesss",
            "tempo",
            "time signature",
            "duration",
            "mode",
            "key",
            "loudness",
            "fadein",
            "fadeout"
    };

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException {
//         Should exclude header lines
        if (byteOffset.get() == 0)
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());

        DoubleWritable[] dwArray = new DoubleWritable[] {
                new DoubleWritable(csv.getTokAsDouble(2)),      // 0: hotttnesss
                new DoubleWritable(csv.getTokAsDouble(14)),     // 1: tempo
                new DoubleWritable(csv.getTokAsDouble(15)),     // 2: time signature
                new DoubleWritable(csv.getTokAsDouble(5)),      // 3: duration
                new DoubleWritable(csv.getTokAsDouble(11)),     // 4: mode
                new DoubleWritable(csv.getTokAsDouble(8)),      // 5: key
                new DoubleWritable(csv.getTokAsDouble(10)),     // 6: loudness
                new DoubleWritable(csv.getTokAsDouble(6)),      // 7: fadein
                new DoubleWritable(csv.getTokAsDouble(13))      // 8: fadeout
        };

        // Key is string of artist id
        CustomWritableComparable key = new CustomWritableComparable()
                .setId(CustomWritableComparable.Q9)
                .setInner(new Text(csv.getTokAt(1)));

        CustomWritable valueOut = new CustomWritable()
                .setId(CustomWritable.DOUBLE_ARRAY)
                .setInner(new DoubleArrayWritable(dwArray));

        context.write(key, valueOut);
    }

}
