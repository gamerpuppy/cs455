package cs455.hadoop;

import cs455.hadoop.io.AnalysisValue1;
import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import cs455.hadoop.io.MetadataValue1;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongsReducer2 extends Reducer<CustomWritableComparable, CustomWritable, CustomWritableComparable, CustomWritable> {

    @Override
    protected void reduce(CustomWritableComparable key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {

        if(key.getId() == CustomWritableComparable.ERROR_LINE_KEY) {
            context.write(key, values.iterator().next());
            return;
        }

        for(CustomWritable value : values) {
            context.write (
                    key,
                    value
            );
        }


    }

}
