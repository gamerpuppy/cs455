package cs455.hadoop;

import cs455.hadoop.io.SegmentArrayWritable;
import cs455.hadoop.util.CsvTokenizer;
import cs455.hadoop.io.Segment;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class Q7Mapper extends Mapper<LongWritable, Text, IntWritable, SegmentArrayWritable> {

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
//         Should exclude header lines
        if(byteOffset.get() == 0)
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());

        String[] startSegments = csv.getTokAt(18).split(" ");
        Segment[] segments = new Segment[startSegments.length];
        setSegments(segments, startSegments, "start");
        setSegments(segments, csv.getTokAt(20).split(" "), "pitch");
        setSegments(segments, csv.getTokAt(21).split(" "), "timbre");
        setSegments(segments, csv.getTokAt(22).split(" "), "maxLoudness");
        setSegments(segments, csv.getTokAt(23).split(" "), "maxLoudnessTime");
        setSegments(segments, csv.getTokAt(24).split(" "), "startLoudness");

        if(segments.length < 2)
            return;

        IntWritable outKey = new IntWritable(new Random().nextInt());
        SegmentArrayWritable outValue = new SegmentArrayWritable(segments);
        context.write(outKey, outValue);
    }

    private void setSegments(Segment[] segments, String[] segmentValues, String attr) {
        assert(segments.length == segmentValues.length);

        for(int i = 0; i < segments.length; i++)
        {
            if(segments[i] == null) {
                segments[i] = new Segment();
            }

            double value = Double.parseDouble(segmentValues[i]);
            Segment s = segments[i];
            switch (attr) {
                case "start": s.start = value; break;
                case "pitch": s.pitch = value; break;
                case "timbre": s.timbre = value; break;
                case "maxLoudness": s.maxLoudness = value; break;
                case "maxLoudnessTime": s.maxLoudnessTime = value; break;
                case "startLoudness": s.startLoudness = value; break;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new FileReader("./testfiles/analysis1.csv"));

        Q7Mapper mapper = new Q7Mapper();
        String line;
        int i = 0;
        while((line = reader.readLine()) != null) {
            mapper.map(new LongWritable(i++), new Text(line), null);
        }

        System.out.println(mapper);
    }

}
