package cs455.hadoop.part1;

import cs455.hadoop.io.CustomWritable;
import cs455.hadoop.io.CustomWritableComparable;
import cs455.hadoop.util.TopList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Part1Reducer extends Reducer<CustomWritableComparable, CustomWritable, Text, Text> {

    String hotttnessstTitle = "";
    double hotttnessst = Double.MIN_VALUE;

    ArrayList<SongLength> songList = new ArrayList<>();
    HashMap<String, ArtistInfo> artistInfoMap = new HashMap<>();

    TopList<SongInfo> energyTop = new TopList<>(10, SongInfo.energyCmp);
    TopList<SongInfo> dancyTop = new TopList<>(10, SongInfo.dancyCmp);

    @Override
    protected void reduce(CustomWritableComparable key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {

        Part1AnalysisValue analysis = null;
        Part1MetadataValue metadata = null;

        for(CustomWritable customWritable : values)
        {
            if(customWritable.getId() == CustomWritable.ANALYSIS_VALUE_1)
                analysis = (Part1AnalysisValue) customWritable.getInner();

            else if(customWritable.getId() == CustomWritable.METADATA_VALUE_1)
                metadata = (Part1MetadataValue) customWritable.getInner();

        }

        if(analysis == null || metadata == null)
            return;

        if(analysis.getHotttnesss() > hotttnessst) {
            hotttnessst = analysis.getHotttnesss();
            hotttnessstTitle = metadata.getTitle();
        }

        songList.add(new SongLength(metadata.getTitle(), analysis.getDuration()));


        SongInfo songInfo = new SongInfo(metadata.getTitle(), analysis.getEnergy(), analysis.getDanceability());
        energyTop.addIfTop(songInfo);
        dancyTop.addIfTop(songInfo);

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

        context.write(new Text("Question 1: Artist with most songs"), new Text(mostSongsArtist+" "+new IntWritable(mostSongs)));

        context.write(new Text("Question 2: Loudest artist"), new Text(String.format("%s %.2f", loudestArtist, loudestAvg)));

        context.write(new Text("Question 3: Hotttnessst song"), new Text(String.format("%s %.2f", hotttnessstTitle, hotttnessst)));

        context.write(new Text("Question 4: Most total fade out artist"), new Text(String.format("%s %.2f", mostFadeArtist, mostFade)));

        if(songList.size() > 0) {
            Collections.sort(songList);

            SongLength longest = songList.get(songList.size() - 1);
            context.write(new Text("Question 5: Longest song"), new Text(String.format("%s %.2f", longest.songname, longest.duration)));

            SongLength shortest = songList.get(0);
            context.write(new Text("Question 5: Shortest song"), new Text(String.format("%s %.2f", shortest.songname, shortest.duration)));

            SongLength median = songList.get(songList.size()/2);
            context.write(new Text("Question 5: Median length song"), new Text(String.format("%s %.2f", median.songname, median.duration)));
        }

        StringBuilder energyTop10 = new StringBuilder();
        int idx = 1;
        energyTop10.append('\n');
        for(SongInfo info : energyTop.topList) {
            energyTop10.append(idx++ +": "+info.songName+" "+String.format("%.2f\n", info.energy));
        }
        context.write(new Text("Question 6: Top10 songs with most energy"), new Text(energyTop10.toString()));

        StringBuilder dancyTop10 = new StringBuilder();
        idx = 1;
        dancyTop10.append('\n');
        for(SongInfo info : dancyTop.topList) {
            dancyTop10.append(idx++ +": "+info.songName+" "+String.format("%.2f\n", info.danceability));
        }
        context.write(new Text("Question 6: Top10 songs with most danceability"), new Text(dancyTop10.toString()));
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

        static final Comparator<SongInfo> energyCmp = Comparator.comparingDouble(o -> o.energy);
        static final Comparator<SongInfo> dancyCmp = Comparator.comparingDouble(o -> o.danceability);

        public SongInfo(String songName, double energy, double danceability) {
            this.songName = songName;
            this.energy = energy;
            this.danceability = danceability;
        }
    }

    private static class SongLength implements Comparable<SongLength> {
        String songname;
        double duration;

        public SongLength(String songname, double duration) {
            this.songname = songname;
            this.duration = duration;
        }

        @Override
        public int compareTo(SongLength o) {
            return Double.compare(duration, o.duration);
        }
    }

}
