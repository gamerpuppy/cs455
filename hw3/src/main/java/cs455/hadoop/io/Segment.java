package cs455.hadoop.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Segment implements Writable {

    // represents the number of samples the values represent the average of
    public int samples = 1;
    public double start = 0;
    public double pitch = 0;
    public double timbre = 0;
    public double maxLoudness = 0;
    public double maxLoudnessTime = 0;
    public double startLoudness = 0;

    public Segment() {}

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(samples);
        out.writeDouble(start);
        out.writeDouble(pitch);
        out.writeDouble(timbre);
        out.writeDouble(maxLoudness);
        out.writeDouble(maxLoudnessTime);
        out.writeDouble(startLoudness);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        samples = in.readInt();
        start = in.readDouble();
        pitch = in.readDouble();
        timbre = in.readDouble();
        maxLoudness = in.readDouble();
        maxLoudnessTime = in.readDouble();
        startLoudness = in.readDouble();
    }

    public void add(Segment o) {
        samples += o.samples;
        start += o.start;
        pitch += o.pitch;
        timbre += o.timbre;
        maxLoudness += o.maxLoudness;
        maxLoudnessTime += o.maxLoudnessTime;
        startLoudness += o.startLoudness;
    }

    public void average() {
        start /= samples;
        pitch /= samples;
        timbre /= samples;
        maxLoudness /= samples;
        maxLoudnessTime /= samples;
        startLoudness /= samples;
        samples = 1;
    }

    @Override
    public String toString() {
        return String.format("%.2f %.2f %.2f %.2f %.2f %.2f", start, pitch, timbre, maxLoudness, maxLoudnessTime, startLoudness);
    }
}
