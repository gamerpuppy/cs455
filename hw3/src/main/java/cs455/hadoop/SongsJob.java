package cs455.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SongsJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "songs job");
            job.setJarByClass(SongsJob.class);
            job.setMapperClass(MetadataMapper.class);
            job.setReducerClass(SongsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);




        } catch (Exception e) {
            System.err.println(e.getMessage());

        }
    }


}
