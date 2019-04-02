package cs455.hadoop;

import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SongsJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "songs job");
            job.setJarByClass(SongsJob.class);
            job.setReducerClass(SongsReducer2.class);

            job.setMapOutputKeyClass(CustomWritableComparable.class);
            job.setMapOutputValueClass(CustomWritable.class);

            job.setOutputKeyClass(CustomWritableComparable.class);
            job.setOutputValueClass(CustomWritable.class);

            MultipleInputs.addInputPath(job,
                    new Path("/data/metadata/metadata1.csv"),
                    TextInputFormat.class,
                    MetadataMapper.class);

            MultipleInputs.addInputPath(job,
                    new Path("/data/analysis/analysis1.csv"),
                    TextInputFormat.class,
                    AnalysisMapper.class);

            FileOutputFormat.setOutputPath(job, new Path(args[0]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.err.println(e.getMessage());

        }
    }


}
