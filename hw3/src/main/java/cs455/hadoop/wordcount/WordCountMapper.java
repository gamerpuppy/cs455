package cs455.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * MetadataMapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            String tok = itr.nextToken();
            String res = evalToken(tok);

            context.write(new Text(res), new IntWritable(1));
        }
    }

    private static String evalToken(String tok) {
        String res = "";
        for(char c : tok.toCharArray()) {
            if(Character.isAlphabetic(c)) {
                res += Character.toLowerCase(c);
            }
        }

        if(res.equals(""))
            res = "!nonAlphabeticSequence";

        return res;
    }

}
