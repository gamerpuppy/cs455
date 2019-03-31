package cs455.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives word, list<count> pairs.
 * Sums up individual counts per given word. Emits <word, total count> pairs.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private String maxWord = "";
    private int maxCount = 0;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for(IntWritable val : values){
            count += val.get();
        }

        if(count > this.maxCount) {
            maxWord = key.toString();
            maxCount = count;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(this.maxWord), new IntWritable(this.maxCount));

    }

}
