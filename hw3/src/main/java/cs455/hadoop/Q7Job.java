package cs455.hadoop;

import cs455.hadoop.io.SegmentArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q7Job {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "songs job");

            job.setJarByClass(Q7Job.class);
            job.setMapperClass(Q7Mapper.class);
            job.setCombinerClass(Q7Combiner.class);
            job.setReducerClass(Q7Reducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(SegmentArrayWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path("/data/analysis/"));
            FileOutputFormat.setOutputPath(job, new Path(args[0]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.err.println(e.getMessage());

        }
    }

}
