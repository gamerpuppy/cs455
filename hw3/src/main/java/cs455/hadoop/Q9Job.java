package cs455.hadoop;

import cs455.hadoop.io.Artist;
import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.io.IOException;

public class Q9Job {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        SimpleRegression regression = new SimpleRegression();
//
//        regression.addData(1,3);
//        regression.addData(2,5);
//        regression.addData(3,7);
//        regression.addData(4,9);
//        regression.addData(3,9);
//
//        System.out.println(regression.getSlope());
//        System.out.println(regression.getSignificance());

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "q9 job");
        job.setJarByClass(Q9Job.class);
        job.setReducerClass(Q9Reducer.class);

        job.setMapOutputKeyClass(CustomWritableComparable.class);
        job.setMapOutputValueClass(CustomWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
                job,
                new Path("/data/metadata/"),
                TextInputFormat.class,
                Q9MetadataMapper.class);

        MultipleInputs.addInputPath(
                job,
                new Path("/data/analysis/"),
                TextInputFormat.class,
                Q9AnalysisMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
