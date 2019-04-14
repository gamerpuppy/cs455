package cs455.hadoop.testutil;

import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SongIdArtistJob {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "songIdArtist");
            job.setJarByClass(SongIdArtistJob.class);
            job.setMapperClass(MetadataMapper.class);

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

class MetadataMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable byteOffset, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if(value.charAt(0) != '0')
            return;

        CsvTokenizer csv = new CsvTokenizer(value.toString());

        String artistId = csv.getTokAt(3);
        String songId = csv.getTokAt(8);

        context.write(new Text(songId), new Text(artistId));
    }

}

//class Part1AnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//    protected void map(LongWritable byteOffset, Text value, Mapper.Context context) throws IOException, InterruptedException {
//        if(value.charAt(0) != '0')
//            return;
//
//        CsvTokenizer csv = new CsvTokenizer(value.toString());
//
//        String songId = csv.getTokAt(1);
////        String artistId = csv.getTokAt(3);
//
//        context.write(new Text(songId), new Text(artistId));
//    }
//
//}