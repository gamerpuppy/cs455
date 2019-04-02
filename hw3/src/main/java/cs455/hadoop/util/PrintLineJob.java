package cs455.hadoop.util;

import cs455.hadoop.SongsJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PrintLineJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "printlinejob");
            job.setJarByClass(PrintLineJob.class);
            job.setMapperClass(PrintLineMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            System.err.println(e.getMessage());

        }
    }

}


class PrintLineMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable byteOffset, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String seq = "b'ARN80361187FB36732'";

        if(value.toString().contains(seq))
            context.write(new Text(seq), value);

    }

}