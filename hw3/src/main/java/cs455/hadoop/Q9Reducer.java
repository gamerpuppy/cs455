package cs455.hadoop;

import cs455.hadoop.io.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

public class Q9Reducer extends Reducer<CustomWritableComparable, CustomWritable, Text, Text> {

    SimpleRegression[] regressions = new SimpleRegression[9];
    HashMap<String, SimpleRegression> termRegressions = new HashMap<>();

    public Q9Reducer() {
        for (int i = 1; i < 9; i++) {
            regressions[i] = new SimpleRegression();
        }
    }

    @Override
    protected void reduce(CustomWritableComparable key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {

        Artist artist = null;
        DoubleArrayWritable array = null;

        Iterator<CustomWritable> it = values.iterator();
        while (it.hasNext()) {
            CustomWritable customWritable = it.next();
            if (customWritable.getId() == CustomWritable.ARTIST)
                artist = (Artist) customWritable.getInner();

            else if (customWritable.getId() == CustomWritable.DOUBLE_ARRAY)
                array = (DoubleArrayWritable) customWritable.getInner();

        }

        if (artist == null || array == null)
            return;


        DoubleWritable[] dwArray = array.toArray();
        double hotttnesss = dwArray[0].get();
        for (int i = 1; i < 9; i++) {
            regressions[i].addData(hotttnesss, dwArray[i].get());
        }


        ArtistTerm[] terms = (ArtistTerm[])artist.terms.get();

        for(ArtistTerm term : terms) {
            String termString = term.term.toString();
            SimpleRegression regression = termRegressions.getOrDefault(termString, new SimpleRegression());
//            regression.addData(hotttnesss, );

        }

        /*
            get all terms
            regress on hotttnesss and weight of the term
            weight is zero if does not contain
         */

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for(int i = 1; i < 9; i++)
        {
            SimpleRegression regression = regressions[i];
            double significance = regression.getSignificance();
            double slope = regression.getSlope();
            double intercept = regression.getIntercept();
            double predict = intercept + slope*1.5;

            context.write(new Text(String.valueOf(i)), new Text(String.format("%.2f %.2f %.2f %.2f", significance,  slope, intercept, predict)));
        }

    }

}
