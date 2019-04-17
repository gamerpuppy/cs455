package cs455.hadoop.q10;

import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.DoubleArrayWritable;
import cs455.hadoop.util.CsvTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Q10AnalysisMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

    static final String[] arrayValues = {
            "compressibility",
            "duration",
            "key",
            "loudness",
            "mode",
            "fade out",
            "tempo",
            "time signature",
            "bytes in line"
    };

    @Override
    protected void map(LongWritable byteOffset, Text value, Context context)
    {
//         Should exclude header lines
        if(byteOffset.get() == 0)
            return;

        String textStr = value.toString();
        byte[] bytes = textStr.getBytes();

        CsvTokenizer csv = new CsvTokenizer(textStr);
        Text songId = new Text(csv.getTokAt(1));

        try {
            DoubleWritable[] dwArray = {
                    new DoubleWritable(getCompressibility(bytes)),
                    new DoubleWritable(csv.getTokAsDouble(5)),
                    new DoubleWritable(csv.getTokAsDouble(8)),
                    new DoubleWritable(csv.getTokAsDouble(10)),
                    new DoubleWritable(csv.getTokAsDouble(11)),
                    new DoubleWritable(csv.getTokAsDouble(13)),
                    new DoubleWritable(csv.getTokAsDouble(14)),
                    new DoubleWritable(csv.getTokAsDouble(15)),
                    new DoubleWritable(bytes.length)
            };

            context.write(songId, new CustomWritable(CustomWritable.Q10_ANALYSIS, new DoubleArrayWritable(dwArray)));

        } catch (Exception e) {}
    }

    public static double getCompressibility(byte[] bytes) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

        GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(byteOut);
        gzip.write(bytes);

        double compressedSize = byteOut.size();
        return bytes.length / compressedSize;
    }

}