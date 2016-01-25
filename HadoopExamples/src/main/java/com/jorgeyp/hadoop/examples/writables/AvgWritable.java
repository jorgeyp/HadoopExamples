package com.jorgeyp.hadoop.examples.writables;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jorge.yague on 25/01/16.
 */
public class AvgWritable implements Writable {
    private LongWritable count;
    private LongWritable sum;

    public AvgWritable() {
        count = new LongWritable();
        sum = new LongWritable();
    }

    public AvgWritable(long count, long sum) {
        this.count = new LongWritable(count);
        this.sum = new LongWritable(sum);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        count.write(out);
        sum.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count.readFields(in);
        sum.readFields(in);
    }

    public LongWritable getCount() {
        return count;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }

    public LongWritable getSum() {
        return sum;
    }

    public void setSum(LongWritable sum) {
        this.sum = sum;
    }

    public double getAverage() {
        if (count.get() > 0)
            return sum.get() / (double) count.get();
        return 0.0;
    }

    /* MRUnit needs hashcode and equals in order to compare outputs */
    @Override
    public int hashCode() {
        return count.hashCode() * 128 + sum.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AvgWritable) {
            AvgWritable avg = (AvgWritable) obj;
            if (this.count.get() == avg.count.get() && this.sum.get() == avg.sum.get())
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.valueOf(this.getAverage());
    }
}
