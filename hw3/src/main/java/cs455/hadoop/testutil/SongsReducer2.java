package cs455.hadoop.testutil;

import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SongsReducer2 extends Reducer<CustomWritableComparable, CustomWritable, CustomWritableComparable, CustomWritable> {

    @Override
    protected void reduce(CustomWritableComparable key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {

        if(key.getId() == CustomWritableComparable.ERROR_LINE) {
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
