package cs455.hadoop.q10;


import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.DoubleArrayWritable;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Q10Reducer extends Reducer<Text, CustomWritable, Text, Text> {

    SimpleRegression[] analysisRegressions = new SimpleRegression[Q10AnalysisMapper.arrayValues.length-1];
    SimpleRegression[] metadataRegressions = new SimpleRegression[Q10MetadataMapper.arrayValues.length];

    public Q10Reducer() {
        for(int i = 0; i < analysisRegressions.length; i++) {
            analysisRegressions[i] = new SimpleRegression();
        }

        for(int i = 0; i < metadataRegressions.length; i++) {
            metadataRegressions[i] = new SimpleRegression();
        }
    }

    @Override
    public void reduce(Text key, Iterable<CustomWritable> values, Context context) {

        DoubleWritable[] analysisValues = null;
        DoubleWritable[] metadataValues = null;
        for(CustomWritable value : values) {
            if(value.getId() == CustomWritable.Q10_ANALYSIS)
                analysisValues = ((DoubleArrayWritable) value.getInner()).toArray();

            if(value.getId() == CustomWritable.Q10_METADATA)
                metadataValues = ((DoubleArrayWritable) value.getInner()).toArray();

        }

        if(analysisValues == null)
            return;

        double compressabilty = analysisValues[0].get();
        for(int i = 1; i < analysisValues.length; i++) {
            analysisRegressions[i-1].addData(analysisValues[i].get(), compressabilty);
        }

        if(metadataValues == null)
            return;

        for(int i = 0; i < metadataValues.length; i++) {
            metadataRegressions[i].addData(metadataValues[i].get(), compressabilty);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for(int i = 0; i < analysisRegressions.length; i++) {
            SimpleRegression reg = analysisRegressions[i];
            context.write(new Text(Q10AnalysisMapper.arrayValues[i+1]+" correlation"), new Text(String.format("%d %.2f %.2f", reg.getN(), reg.getSignificance(), reg.getSlope())));
        }

        for(int i = 0; i < metadataRegressions.length; i++) {
            SimpleRegression reg = metadataRegressions[i];
            context.write(new Text(Q10MetadataMapper.arrayValues[i] + " correlation"), new Text(String.format("%d %.2f %.2f", reg.getN(), reg.getSignificance(), reg.getSlope())));
        }

    }

}





































