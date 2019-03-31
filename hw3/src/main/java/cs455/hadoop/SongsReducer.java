package cs455.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class SongsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private HashMap<String, Integer> artistSongCountMap = new HashMap<>();

    private String maxHotnessTitle = null;
    private Double maxHotness = null;

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        reduceQ1(key, values, context);

        switch (key.charAt(0)){
            // Question 1
            case '1':
                reduceQ1(key, values, context);
                break;
            case '3':
                reduceQ3(key, values, context);
                break;
        }

    }

    private void reduceQ1(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalCount = 0;
        for(IntWritable value : values) {
            totalCount += value.get();
        }

        String keyStr = key.toString();

        int count = this.artistSongCountMap.getOrDefault(keyStr, 0);
        this.artistSongCountMap.put(keyStr, count + totalCount);
    }

    private void reduceQ3(Text key, Iterable<IntWritable> values, Context context) {
        String[] splits = key.toString().split(",");

        String songTitle = splits[0].substring(1);
        Double hotness = Double.parseDouble(splits[1]);

        if(maxHotness == null || hotness > maxHotness) {
            maxHotness = hotness;
            maxHotnessTitle = songTitle;
        }
    }

    private void cleanupQ1(Context context) throws IOException, InterruptedException {
        String maxKey = "this should not appear";
        int maxCount = -1;

        for (String artistId : this.artistSongCountMap.keySet()) {
            int count = this.artistSongCountMap.get(artistId);
            if (count > maxCount) {
                maxKey = artistId;
                maxCount = count;
            }
        }

//        String name = maxKey.split(",")[1];
        context.write(new Text(maxKey), new IntWritable(maxCount));
    }

    private void cleanupQ3(Context context) throws IOException, InterruptedException {
        context.write(new Text('3'+ maxHotnessTitle +','+maxHotness.toString()), new IntWritable(0));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Question 1
        cleanupQ1(context);

        // Question 3
        cleanupQ3(context);
    }

}
