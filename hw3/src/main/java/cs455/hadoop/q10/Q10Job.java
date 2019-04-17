package cs455.hadoop.q10;

import cs455.hadoop.io.CustomWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q10Job {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "q10 job");
            job.setJarByClass(Q10Job.class);

            job.setMapperClass(Q10AnalysisMapper.class);
            job.setReducerClass(Q10Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CustomWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path("/data/analysis/"));
            FileOutputFormat.setOutputPath(job, new Path(args[0]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.err.println(e.getMessage());

        }
    }

}
