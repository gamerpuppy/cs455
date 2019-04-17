package cs455.hadoop.part1;

import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Part1Job {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "q1-6");
            job.setJarByClass(Part1Job.class);
            job.setReducerClass(Part1Reducer.class);

            job.setMapOutputKeyClass(CustomWritableComparable.class);
            job.setMapOutputValueClass(CustomWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(1);

            MultipleInputs.addInputPath(job,
                    new Path("/data/metadata/"),
                    TextInputFormat.class,
                    Part1MetadataMapper.class);

            MultipleInputs.addInputPath(job,
                    new Path("/data/analysis/"),
                    TextInputFormat.class,
                    Part1AnalysisMapper.class);

            FileOutputFormat.setOutputPath(job, new Path(args[0]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.err.println(e.getMessage());

        }
    }

}
