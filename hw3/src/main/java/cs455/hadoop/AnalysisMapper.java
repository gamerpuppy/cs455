package cs455.hadoop;

import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;

public class AnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Q2
    private HashMap<String, Integer> q2Map = new HashMap<>();

    protected void map(LongWritable byteOffset, Text value, Context context) throws IOException, InterruptedException
    {
        if(value.charAt(0) != '0')
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());

        Double loudness = Double.parseDouble(csv.getTokAt(10));
        String artistId = csv.getTokAt(3);
        String artistName = csv.getTokAt(7);
        String songTitle = csv.getTokAt(9);

        // Question 1
//        mapQ1(artistId, artistName);
    }

    private void mapQ2(String artistId, String artistName) {
        String key = '1'+artistId+','+artistName;
        int count = this.q2Map.getOrDefault(key, 0);
        this.q2Map.put(key, count + 1);
    }

    private void cleanupQ2(Context context) throws IOException, InterruptedException {
        for (String q1Key : this.q2Map.keySet()) {
            int count = this.q2Map.get(q1Key);
            context.write(new Text(q1Key), new IntWritable(count));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Question 1
        cleanupQ2(context);
    }

    public static void main(String[] args) throws Exception {
        Text input = new Text("");

        new AnalysisMapper().map(new LongWritable(100), input, null);
    }
}
