package cs455.hadoop.q10;


import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.DoubleArrayWritable;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;

public class Q10Reducer extends Reducer<Text, CustomWritable, Text, Text> {

    SimpleRegression[] aReg = new SimpleRegression[Q10AnalysisMapper.arrayValues.length-1];
    SummaryStatistics[] aStats = new SummaryStatistics[Q10AnalysisMapper.arrayValues.length];

    SimpleRegression[] mReg = new SimpleRegression[Q10MetadataMapper.arrayValues.length];
    SummaryStatistics[] mStats = new SummaryStatistics[Q10MetadataMapper.arrayValues.length];

    public Q10Reducer() {
        Arrays.setAll(aReg, p -> new SimpleRegression());
        Arrays.setAll(mReg, p -> new SimpleRegression());
        Arrays.setAll(aStats, p -> new SummaryStatistics());
        Arrays.setAll(mStats, p -> new SummaryStatistics());
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

        double compressibility = analysisValues[0].get();
        aStats[0].addValue(compressibility);

        for(int i = 1; i < analysisValues.length; i++) {
            double value = analysisValues[i].get();
            aStats[i].addValue(value);
            if(i == 4 || value != 0 )
                aReg[i-1].addData(value, compressibility);
        }

        if(metadataValues == null)
            return;

        for(int i = 0; i < metadataValues.length; i++) {
            double value = metadataValues[i].get();
            mStats[i].addValue(value);
            if(value != 0 )
                mReg[i].addData(value, compressibility);
        }

    }

    public String getRegressionStr(SimpleRegression reg) {
        return String.format("n %d slope %.2f", reg.getN(), reg.getSlope());

    }

    public String getStatsStr(SummaryStatistics stats) {
       return String.format("min %.2f max %.2f mean %.2f stddev %.2f", stats.getMin(), stats.getMax(), stats.getMean(), stats.getStandardDeviation());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        context.write(new Text("compressibility"), new Text(getStatsStr(aStats[0])));

        for(int i = 0; i < aReg.length; i++) {
            context.write(new Text(Q10AnalysisMapper.arrayValues[i+1]), new Text('\n'+getStatsStr(aStats[i+1])+'\n'+getRegressionStr(aReg[i])));
        }

        for(int i = 0; i < mReg.length; i++) {
            context.write(new Text(Q10MetadataMapper.arrayValues[i]),  new Text('\n'+getStatsStr(mStats[i])+'\n'+getRegressionStr(mReg[i])));
        }

    }

}





































