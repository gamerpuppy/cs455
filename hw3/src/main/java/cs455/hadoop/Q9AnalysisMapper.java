package cs455.hadoop;

import cs455.hadoop.io.*;
import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

/*
        0: hotttnesss
        1: tempo
        2: time signature
        3: duration
        4: mode
        5: key
        6: loudness
        7: fadein
        8: fadeout

        *danceability and energy are not listed as they are always zero in the dataset
*/

public class Q9AnalysisMapper extends Mapper<LongWritable, Text, CustomWritableComparable, CustomWritable> {

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
//         Should exclude header lines
        if(byteOffset.get() == 0)
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());

        DoubleWritable[] dwArray = new DoubleWritable[9];
        dwArray[0] = new DoubleWritable(csv.getTokAsDouble(2));
        dwArray[1] = new DoubleWritable(csv.getTokAsDouble(14));
        dwArray[2] = new DoubleWritable(csv.getTokAsDouble(15));
        dwArray[3] = new DoubleWritable(csv.getTokAsDouble(5));
        dwArray[4] = new DoubleWritable(csv.getTokAsDouble(11));
        dwArray[5] = new DoubleWritable(csv.getTokAsDouble(8));
        dwArray[6] = new DoubleWritable(csv.getTokAsDouble(10));
        dwArray[7] = new DoubleWritable(csv.getTokAsDouble(6));
        dwArray[8] = new DoubleWritable(csv.getTokAsDouble(13));

        CustomWritableComparable key = new CustomWritableComparable()
                .setId(CustomWritableComparable.Q9)
                .setInner(new Text(csv.getTokAt(1)));

        CustomWritable valueOut = new CustomWritable()
                .setId(CustomWritable.DOUBLE_ARRAY)
                .setInner(new DoubleArrayWritable(dwArray));

        context.write(key, valueOut);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("./testfiles/analysis1.csv"));

        Q9AnalysisMapper mapper = new Q9AnalysisMapper();
        String line;
        int i = 0;
        while((line = reader.readLine()) != null) {
            mapper.map(new LongWritable(i++), new Text(line), null);
        }

        System.out.println(mapper);
    }

}
