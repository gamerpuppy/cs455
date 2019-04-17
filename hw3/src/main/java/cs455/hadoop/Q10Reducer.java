package cs455.hadoop;


import cs455.hadoop.io.Q10Value;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q10Reducer extends Reducer<IntWritable, Q10Value, Text, Text> {

    SimpleRegression hotRegression = new SimpleRegression();
    SimpleRegression durRegression = new SimpleRegression();

    @Override
    public void reduce(IntWritable key, Iterable<Q10Value> values, Context context) throws IOException, InterruptedException {

        for(Q10Value value : values) {
            if(value.hotttnesss > 0) {
                hotRegression.addData(value.compressability, value.hotttnesss);
            }
            if(value.duration > 0) {
                durRegression.addData(value.duration, value.hotttnesss);
            }
//            context.write(new Text(String.valueOf(value.hotttnesss)), new Text(String.valueOf(value.compressability)));
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text("hotttnesss correlation"), new Text(String.format("%d %.2f %.2f", hotRegression.getN(), hotRegression.getSignificance(), hotRegression.getSlope())));
        context.write(new Text("duration correlation"), new Text(String.format("%d %.2f %.2f", durRegression.getN(), durRegression.getSignificance(), durRegression.getSlope())));
    }

}
