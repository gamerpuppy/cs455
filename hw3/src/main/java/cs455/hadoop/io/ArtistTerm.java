package cs455.hadoop.io;

import cs455.hadoop.util.CsvTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArtistTerm implements Writable {

    public Text term = new Text();
    public double freq = 0;
    public double weight = 0;

    @Override
    public void write(DataOutput out) throws IOException {
        term.write(out);
        out.writeDouble(freq);
        out.writeDouble(weight);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        term.readFields(in);
        freq = in.readDouble();
        weight = in.readDouble();
    }

    public static ArtistTerm[] getTerms(CsvTokenizer csv) {
        String[] terms = csv.getTokAt(11).split(" ");
        ArtistTerm[] artistTerms = new ArtistTerm[terms.length];
        setArtistTerms(artistTerms, terms, new TermSetter());
        setArtistTerms(artistTerms, csv.getTokAt(12).split(" "), new FreqSetter());
        setArtistTerms(artistTerms, csv.getTokAt(13).split(" "), new WeightSetter());
        return artistTerms;
    }

    private static void setArtistTerms(ArtistTerm[] ArtistTerms, String[] values, ArtistTermSetter setter) {

        for(int i = 0; i < ArtistTerms.length; i++)
        {
            if(ArtistTerms[i] == null)
                ArtistTerms[i] = new ArtistTerm();

            if(i < values.length)
                setter.setTerm(ArtistTerms[i], values[i]);
        }
    }

}

interface ArtistTermSetter {
    void setTerm(ArtistTerm artistTerm, String token);
}

class TermSetter implements ArtistTermSetter {
    @Override
    public void setTerm(ArtistTerm artistTerm, String token) {
        artistTerm.term.set(token);
    }
}

class FreqSetter implements ArtistTermSetter {
    @Override
    public void setTerm(ArtistTerm artistTerm, String token) {
        try {
            artistTerm.freq = Double.parseDouble(token);
        } catch (Exception e) {}
    }
}

class WeightSetter implements ArtistTermSetter {
    @Override
    public void setTerm(ArtistTerm artistTerm, String token) {
        try {
            artistTerm.weight = Double.parseDouble(token);
        } catch (Exception e) {}
    }
}
