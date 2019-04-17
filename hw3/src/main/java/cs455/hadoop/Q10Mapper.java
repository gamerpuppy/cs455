package cs455.hadoop;

import cs455.hadoop.io.Q10Value;
import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.compress.compressors.lzma.LZMACompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;

public class Q10Mapper extends Mapper<LongWritable, Text, IntWritable, Q10Value> {

    Random r = new Random();

    @Override
    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
//         Should exclude header lines
        if(byteOffset.get() == 0)
            return;

        String textStr = value.toString();
        CsvTokenizer csv = new CsvTokenizer(textStr);


        Q10Value outValue = new Q10Value();
        outValue.hotttnesss = csv.getTokAsDouble(2);
        outValue.duration = csv.getTokAsDouble(10);
        try {
            outValue.compressability = getCompressability(textStr.getBytes(Charset.defaultCharset()));
            context.write(new IntWritable(r.nextInt()), outValue);

        } catch (IOException e) {}
        return;
    }

    public double getCompressability(byte[] bytes) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();

//        LZMACompressorOutputStream lzma = new LZMACompressorOutputStream(byteOut);
//        lzma.write(bytes);
        GzipCompressorOutputStream gzip = new GzipCompressorOutputStream(byteOut);
        gzip.write(bytes);

        double compressedSize = byteOut.size();
        return bytes.length / compressedSize;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("./testfiles/analysis1.csv"));

        Q10Mapper mapper = new Q10Mapper();
        String line;
        int i = 0;
        while((line = reader.readLine()) != null) {
            mapper.map(new LongWritable(i++), new Text(line), null);
        }

        System.out.println(mapper);
    }

}