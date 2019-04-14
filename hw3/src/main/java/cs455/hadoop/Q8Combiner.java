package cs455.hadoop;

import cs455.hadoop.io.Artist;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class Q8Combiner extends Reducer<Text, Artist, Text, Artist>  {

    @Override
    protected void reduce(Text artistId, Iterable<Artist> values, Context context) throws IOException, InterruptedException {
        Iterator<Artist> it = values.iterator();
        // just to be safe
        if(!it.hasNext())
            return;

        context.write(artistId, it.next());
    }

}
