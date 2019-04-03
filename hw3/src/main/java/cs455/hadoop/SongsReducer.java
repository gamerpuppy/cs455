package cs455.hadoop;

import cs455.hadoop.io.AnalysisValue1;
import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import cs455.hadoop.io.MetadataValue1;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class SongsReducer extends Reducer<CustomWritableComparable, CustomWritable, CustomWritableComparable, CustomWritable> {

    String hotttnessstTitle = "";
    double hotttnessst = Double.MIN_VALUE;

    String longestTitle = "";
    double longest = Double.MIN_VALUE;

    String shortestTitle = "";
    double shortest = Double.MAX_VALUE;

    HashMap<String, ArtistInfo> artistInfoMap = new HashMap<>();

    int keyCount = 0;
    int completedKeyCount = 0;

    ArrayList<SongInfo> songs = new ArrayList<>();

    @Override
    protected void reduce(CustomWritableComparable key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {

        keyCount++;

        if(key.getId() == CustomWritableComparable.ERROR_LINE_KEY) {
            context.write(key, values.iterator().next());
            return;
        }

        // only one key for now
        if(key.getId() != CustomWritableComparable.SONG_ID_KEY)
            return;

        AnalysisValue1 analysis = null;
        MetadataValue1 metadata = null;

        for(CustomWritable customWritable : values)
        {
            if(customWritable.getId() == CustomWritable.ANALYSIS_VALUE_1)
                analysis = (AnalysisValue1) customWritable.getInner();

            else if(customWritable.getId() == CustomWritable.METADATA_VALUE_1)
                metadata = (MetadataValue1) customWritable.getInner();

        }

        if(analysis == null || metadata == null)
            return;

        if(analysis.getHotttnesss() > hotttnessst) {
            hotttnessst = analysis.getHotttnesss();
            hotttnessstTitle = metadata.getTitle();
        }

        if(analysis.getDuration() > longest) {
            longest = analysis.getDuration();
            longestTitle = metadata.getTitle();
        }

        if(analysis.getDuration() < shortest) {
            shortest = analysis.getDuration();
            shortestTitle = metadata.getTitle();
        }

        SongInfo songInfo = new SongInfo(metadata.getTitle(), analysis.getEnergy(), analysis.getDanceability());
        songs.add(songInfo);

        if(artistInfoMap.containsKey(metadata.getArtistId())) {
            ArtistInfo info = artistInfoMap.get(metadata.getArtistId());
            info.fadeSum += analysis.getEndFadeIn();
            info.loudSum += analysis.getLoudness();
            info.songCount++;
        } else {
            artistInfoMap.put(
                    metadata.getArtistId(),
                    new ArtistInfo(metadata.getArtistName(), analysis.getEndFadeIn(), analysis.getLoudness(), 1)
            );
        }

        completedKeyCount++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        String mostSongsArtist = "";
        int mostSongs = 0;

        String loudestArtist = "";
        double loudestAvg = Double.MIN_VALUE;

        String mostFadeArtist = "";
        double mostFade = 0;

        for(String artistId : artistInfoMap.keySet())
        {
            ArtistInfo info = artistInfoMap.get(artistId);

            if(info.songCount > mostSongs) {
                mostSongs = info.songCount;
                mostSongsArtist = info.artistName;
            }

            if(info.fadeSum > mostFade) {
                mostFade = info.fadeSum;
                mostFadeArtist = info.artistName;
            }

            double avg = info.loudSum / info.songCount;
            if(avg > loudestAvg) {
                loudestAvg = avg;
                loudestArtist = info.artistName;
            }
        }

        // Question 1 most songs
        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.MOSTSONGS_OUT_KEY)
                        .setInner(new Text(mostSongsArtist)),
                new CustomWritable()
                        .setId(CustomWritable.INT)
                        .setInner(new IntWritable(mostSongs))
        );

        // Question 2 loudest
        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.LOUDEST_OUT_KEY)
                        .setInner(new Text(loudestArtist)),
                new CustomWritable()
                        .setId(CustomWritable.DOUBLE)
                        .setInner(new DoubleWritable(loudestAvg))
        );

        // Question 3 hotttessst
        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.HOTTTNESSS_OUT_KEY)
                        .setInner(new Text(hotttnessstTitle)),
                new CustomWritable()
                        .setId(CustomWritable.DOUBLE)
                        .setInner(new DoubleWritable(hotttnessst))
        );

        // Question 4 fade time
        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.MOSTFADE_OUT_KEY)
                        .setInner(new Text(mostFadeArtist)),
                new CustomWritable()
                        .setId(CustomWritable.DOUBLE)
                        .setInner(new DoubleWritable(mostFade))
        );

        // Question 5 longest
        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.LONGEST_OUT_KEY)
                        .setInner(new Text(longestTitle)),
                new CustomWritable()
                        .setId(CustomWritable.DOUBLE)
                        .setInner(new DoubleWritable(longest))
        );

        // Question 5 shortest
        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.SHORTEST_OUT_KEY)
                        .setInner(new Text(shortestTitle)),
                new CustomWritable()
                        .setId(CustomWritable.DOUBLE)
                        .setInner(new DoubleWritable(shortest))
        );

        // Question 6 top 10 energy

        StringBuilder energyTop10 = new StringBuilder();
        songs.sort((o1, o2) -> Double.compare(o1.energy, o2.energy));
        for(int i = 0; i < 10; i++)
            energyTop10.append((i+1)+": "+songs.get(i).songName+" "+songs.get(i).energy);

        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.ENERGY_OUT_KEY),
                new CustomWritable()
                        .setId(CustomWritable.TEXT)
                        .setInner(new Text(energyTop10.toString()))
        );

        // Question 6 top 10 danceability

        StringBuilder dancyTop10 = new StringBuilder();
        songs.sort((o1, o2) -> Double.compare(o1.danceability, o2.danceability));
        for(int i = 0; i < 10; i++)
            dancyTop10.append((i+1)+": "+songs.get(i).songName+" "+songs.get(i).danceability);

        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.DANCY_OUT_KEY),
                new CustomWritable()
                        .setId(CustomWritable.TEXT)
                        .setInner(new Text(dancyTop10.toString()))
        );

        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.ERROR_LINE_KEY)
                        .setInner(new Text("number of keys")),
                new CustomWritable()
                        .setId(CustomWritable.INT)
                        .setInner(new IntWritable(keyCount))
        );

        context.write(
                new CustomWritableComparable()
                        .setId(CustomWritableComparable.ERROR_LINE_KEY)
                        .setInner(new Text("number of keys that had both values")),
                new CustomWritable()
                        .setId(CustomWritable.INT)
                        .setInner(new IntWritable(completedKeyCount))
        );

    }

    private static class ArtistInfo {
        String artistName;
        double fadeSum;
        double loudSum;
        int songCount;

        public ArtistInfo(String artistName, double fadeSum, double loudSum, int songCount) {
            this.artistName = artistName;
            this.fadeSum = fadeSum;
            this.loudSum = loudSum;
            this.songCount = songCount;
        }
    }

    private static class SongInfo {
        String songName;
        double energy;
        double danceability;

        public SongInfo(String songName, double energy, double danceability) {
            this.songName = songName;
            this.energy = energy;
            this.danceability = danceability;
        }

    }




}
